// File: workers/sql-worker.js (Optimized for your Azure environment)

// Load environment variables from the .env file in the same directory
require('dotenv').config();

const { Worker } = require('bullmq');
const IORedis = require('ioredis');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');

// --- Environment Variable Validation ---
// This ensures your .env file was loaded correctly before starting.
if (!process.env.NEXT_PUBLIC_SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error('Supabase environment variables are not set!');
}
if (!process.env.REDIS_URL) {
  throw new Error('REDIS_URL environment variable is not set!');
}
if (!process.env.NEXT_PUBLIC_VERCEL_URL) {
  console.warn('[Warning] NEXT_PUBLIC_VERCEL_URL is not set. The worker will default to localhost, which may fail in production.');
}
if (!process.env.WORKER_SECRET_KEY) {
  throw new Error('CRITICAL: WORKER_SECRET_KEY is not defined. The worker cannot authenticate with the API.');
}

// --- Redis Connection for Azure ---
// Connects to "redis://127.0.0.1:6379" as per your .env file.
const connection = new IORedis(process.env.REDIS_URL, {
  maxRetriesPerRequest: null, // Required by BullMQ for robust connections
});

connection.on('connect', () => console.log('[Redis] Successfully connected to Redis.'));
connection.on('error', (err) => console.error('[Redis] Connection Error:', err.message));

// --- Client Initializations ---
const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

const s3Client = new S3Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
  },
});

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// --- HELPER FUNCTIONS ---

/**
 * Fetches the database configuration for a given notebook ID.
 */
const getDatabaseConfig = async (notebookId) => {
  try {
    const { data: notebook, error } = await supabase
      .from('autonomis_user_notebook')
      .select(`
        autonomis_user_database (
          connection_string,
          autonomis_data_source ( name )
        )
      `)
      .eq('id', notebookId)
      .single();

    if (error) throw error;
    if (!notebook || !notebook.autonomis_user_database) {
      throw new Error(`No database configuration found for notebook ${notebookId}`);
    }

    const dbConfig = JSON.parse(notebook.autonomis_user_database.connection_string);
    const dbType = notebook.autonomis_user_database.autonomis_data_source.name.toLowerCase();

    console.log(`[Worker] Database config loaded for notebook ${notebookId}: type is "${dbType}"`);
    return { dbConfig, dbType };

  } catch (error) {
    console.error(`[Worker] Critical failure fetching DB config for notebook ${notebookId}:`, error);
    throw error;
  }
};

/**
 * Executes a single SQL query by calling the secure API endpoint.
 */
const executeQuery = async (query, dbType, dbConfig, queryId) => {
  try {
    // Uses your Vercel URL from the .env file.
    const baseUrl = process.env.NEXT_PUBLIC_VERCEL_URL || 'http://localhost:3000';
    
    console.log(`[Worker] Making API call to: ${baseUrl}/api/executequery for query ID: ${queryId}`);
    
    const response = await axios.post(`${baseUrl}/api/executequery`, 
    {
      sqlstr: query,
      dbtype: dbType,
      dbConfig: dbConfig
    }, 
    {
      timeout: 300000, // 5 minute timeout for long queries
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${process.env.WORKER_SECRET_KEY}`
      }
    });

    const rowCount = response.data?.rows?.length || 0;
    console.log(`[Worker] Query ${queryId} executed successfully. Rows affected/returned: ${rowCount}`);
    return { success: true, rowCount };

  } catch (error) {
    const errorMessage = error.response?.data?.error || error.message || 'Unknown error';
    console.error(`[Worker] Query ${queryId} failed:`, errorMessage);
    return { success: false, error: errorMessage };
  }
};

// --- BULLMQ WORKER PROCESSOR ---
const processor = async (job) => {
  const { s3Key, notebookId } = job.data;

  if (!s3Key || !notebookId) {
    throw new Error('Job failed: Data is missing "s3Key" or "notebookId".');
  }

  console.log(`[Worker] Starting job #${job.id} for Notebook ID: ${notebookId}, S3 file: ${s3Key}`);

  try {
    const { dbConfig, dbType } = await getDatabaseConfig(notebookId);

    const command = new GetObjectCommand({
      Bucket: process.env.AWS_S3_BUCKET_NAME,
      Key: s3Key, 
    });
    
    const response = await s3Client.send(command);
    const queriesJsonString = await response.Body.transformToString();
    const queryObjects = JSON.parse(queriesJsonString);
    console.log(`[Worker] Successfully loaded ${queryObjects.length} queries from S3.`);

    let successCount = 0;
    let failureCount = 0;

    for (const [index, item] of queryObjects.entries()) {
      console.log(`[Worker] Executing query ${index + 1}/${queryObjects.length} (ID: ${item.id})`);
      
      const startTime = Date.now();
      const result = await executeQuery(item.query, dbType, dbConfig, item.id);
      const executionTime = Date.now() - startTime;

      if (result.success) {
        successCount++;
        console.log(`[Worker] ✅ Query ${item.id} completed in ${executionTime}ms.`);
      } else {
        failureCount++;
        console.error(`[Worker] ❌ Query ${item.id} failed after ${executionTime}ms. Error: ${result.error}`);
      }

      await sleep(200);
    }

    const resultMessage = `Execution finished. ${successCount}/${queryObjects.length} queries succeeded. ${failureCount} failed.`;
    console.log(`[Worker] Job #${job.id} completed. ${resultMessage}`);
    
    return { 
      status: failureCount === 0 ? 'Completed' : 'Completed with errors', 
      message: resultMessage,
    };

  } catch (error) {
    console.error(`[Worker] Job #${job.id} failed catastrophically:`, error.message);
    throw error;
  }
};

// --- MONITORING AND WORKER INITIALIZATION ---

const updateMonitoringData = async (job, status) => {
  // Your monitoring logic is correct and does not need changes.
  if (!job || !job.data.notebookId) return;

  const { notebookId, notebookName } = job.data;
  console.log(`[Monitoring] Updating status for notebook ${notebookId} to "${status}".`);

  try {
    const { data: currentMonitoring } = await supabase
      .from('autonomis_job_monitoring').select('run_history').eq('notebook_id', notebookId).single();
      
    let history = currentMonitoring?.run_history || [];
    history.unshift({ status: status, timestamp: new Date().toISOString() });
    const newHistory = history.slice(0, 5);
    
    const { error: upsertError } = await supabase
      .from('autonomis_job_monitoring')
      .upsert({
        notebook_id: notebookId,
        notebook_name: notebookName || 'Untitled Notebook',
        last_known_job_id: job.id,
        last_run_time: new Date().toISOString(),
        last_run_status: status,
        run_history: newHistory,
        is_active: true,
      }, { onConflict: 'notebook_id' });
  
    if (upsertError) console.error('[Monitoring] Error upserting data:', upsertError);
  } catch(e) {
    console.error('[Monitoring] Critical error during update:', e);
  }
};

console.log('[Worker] SQL Execution Worker is starting...');
const worker = new Worker('sql-execution-queue', processor, { connection });

worker.on('completed', (job, result) => {
  console.log(`[Worker-Event] Job ${job.id} has completed with status: ${result.status}`);
  updateMonitoringData(job, 'completed');
});

worker.on('failed', (job, err) => {
  console.error(`[Worker-Event] Job ${job.id} has failed with error: ${err.message}`);
  updateMonitoringData(job, 'failed');
});

console.log('[Worker] Worker is now active and waiting for jobs.');