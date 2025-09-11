// workers/sql-worker.js (For Azure VM Deployment)

// Uses the standard dotenv for Node.js environments to load the .env file
require('dotenv').config();

const { Worker } = require('bullmq');
const IORedis = require('ioredis'); // Using ioredis directly for Azure
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');

// --- Environment Variable Validation ---
if (!process.env.NEXT_PUBLIC_SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error('Supabase environment variables (URL and Service Role Key) are not set!');
}
if (!process.env.REDIS_URL) {
    throw new Error('REDIS_URL is not set in the .env file!');
}

// --- Redis Connection for Azure ---
// This connects directly to your local Redis container on the VM using the REDIS_URL.
const connection = new IORedis(process.env.REDIS_URL, {
    maxRetriesPerRequest: null, // Required by BullMQ for robust connections
});

connection.on('connect', () => console.log('[Redis] Successfully connected to Redis.'));
connection.on('error', (err) => console.error('[Redis] Connection Error:', err.message));


// --- Client Initializations (Logic preserved) ---
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


// --- HELPER FUNCTIONS (Logic and syntax preserved exactly as provided) ---

const getDatabaseConfig = async (notebookId) => {
  try {
    const { data: notebook, error } = await supabase
      .from('autonomis_user_notebook')
      .select(`
        autonomis_user_database (
          connection_string,
          autonomis_data_source (
            name
          )
        )
      `)
      .eq('id', notebookId)
      .single();

    if (error) {
      console.error(`[Worker] Error fetching notebook config for ${notebookId}:`, error);
      throw new Error(`Failed to fetch notebook configuration: ${error.message}`);
    }

    if (!notebook || !notebook.autonomis_user_database || !notebook.autonomis_user_database.autonomis_data_source) {
      throw new Error(`No complete database configuration found for notebook ${notebookId}`);
    }

    const dbConfig = JSON.parse(notebook.autonomis_user_database.connection_string);
    const dbType = notebook.autonomis_user_database.autonomis_data_source.name.toLowerCase();

    console.log(`[Worker] Database config loaded for notebook ${notebookId}: type is "${dbType}"`);
    return { dbConfig, dbType };

  } catch (error) {
    console.error(`[Worker] Critical failure in getDatabaseConfig for notebook ${notebookId}:`, error);
    throw error;
  }
};

const executeQuery = async (query, dbType, dbConfig, queryId) => {
  try {
    // IMPORTANT: Ensure your .env file on Azure sets one of these variables to your public Vercel URL.
    const baseUrl = process.env.API_BASE_URL || process.env.NEXT_PUBLIC_VERCEL_URL || 'http://localhost:3000';

    if (!process.env.WORKER_SECRET_KEY) {
      throw new Error('CRITICAL: WORKER_SECRET_KEY is not defined in the worker environment.');
    }
    
    console.log(`[Worker] Calling API endpoint: ${baseUrl}/api/executequery for query ID: ${queryId}`);
    
    const response = await axios.post(`${baseUrl}/api/executequery`, 
    {
      sqlstr: query,
      dbtype: dbType,
      dbConfig: dbConfig
    }, 
    {
      timeout: 300000,
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${process.env.WORKER_SECRET_KEY}`
      }
    });

    const rowCount = response.data?.rows?.length || 0;
    console.log(`[Worker] Query ${queryId} executed successfully. Rows affected/returned: ${rowCount}`);
    return {
      success: true,
      data: response.data.rows,
      rowCount: rowCount
    };

  } catch (error) {
    const errorMessage = error.response?.data?.error || error.message || 'Unknown error';
    console.error(`[Worker] Query ${queryId} failed:`, errorMessage);
    return {
      success: false,
      error: errorMessage,
      status: error.response?.status
    };
  }
};


// --- BULLMQ WORKER PROCESSOR (Logic and syntax preserved exactly as provided) ---
const processor = async (job) => {
  const { s3Key, notebookId } = job.data;
  const file_url = s3Key;

  if (!file_url) {
    throw new Error('Job failed: Missing "s3Key" in job data.');
  }
  if (!notebookId) {
    throw new Error('Job failed: Missing "notebookId" in job data. Cannot determine which database to connect to.');
  }

  console.log(`[Worker] Starting job #${job.id} for Notebook ID: ${notebookId}, S3 file: ${file_url}`);

  try {
    const { dbConfig, dbType } = await getDatabaseConfig(notebookId);

    const command = new GetObjectCommand({
      Bucket: process.env.AWS_S3_BUCKET_NAME,
      Key: file_url,
    });
    
    const response = await s3Client.send(command);
    const queriesJsonString = await response.Body.transformToString();
    const queryObjects = JSON.parse(queriesJsonString);
    console.log(`[Worker] Successfully loaded ${queryObjects.length} queries from S3.`);

    let successCount = 0;
    let failureCount = 0;
    const executionResults = [];

    for (const [index, item] of queryObjects.entries()) {
      const queryToRun = item.query;
      const queryId = item.id || `query_${index + 1}`;
      const variableName = item.variableName || `result_${index + 1}`;

      console.log(`[Worker] Executing query ${index + 1}/${queryObjects.length} (ID: ${queryId}, Name: ${variableName})`);
      
      const startTime = Date.now();
      const result = await executeQuery(queryToRun, dbType, dbConfig, queryId);
      const executionTime = Date.now() - startTime;

      if (result.success) {
        successCount++;
        console.log(`[Worker] ✅ Query ${queryId} completed in ${executionTime}ms. Rows: ${result.rowCount}`);
      } else {
        failureCount++;
        console.error(`[Worker] ❌ Query ${queryId} failed after ${executionTime}ms. Error: ${result.error}`);
      }

      executionResults.push({
        queryId,
        variableName,
        success: result.success,
        executionTime,
        error: result.error,
        timestamp: new Date().toISOString()
      });

      await sleep(200); 
    }

    const resultMessage = `Execution finished. ${successCount}/${queryObjects.length} queries succeeded. ${failureCount} failed.`;
    console.log(`[Worker] Job #${job.id} completed. ${resultMessage}`);
    
    return { 
      status: failureCount === 0 ? 'Completed' : 'Completed with errors', 
      message: resultMessage,
      results: executionResults
    };

  } catch (error) {
    console.error(`[Worker] Job #${job.id} failed catastrophically:`, error.message);
    throw error;
  }
};


// --- WORKER INITIALIZATION AND EVENT LISTENERS (Logic and syntax preserved exactly as provided) ---
const updateMonitoringData = async (job, status) => {
  if (!job || !job.data.notebookId) {
    console.log('[Monitoring] Skipping update: job data is incomplete.');
    return;
  }

  const { notebookId, notebookName } = job.data;
  console.log(`[Monitoring] Updating status for notebook ${notebookId} to "${status}".`);

  try {
    const { data: currentMonitoring, error: selectError } = await supabase
      .from('autonomis_job_monitoring')
      .select('*')
      .eq('notebook_id', notebookId)
      .maybeSingle();

    if (selectError) {
      console.error('[Monitoring] Error fetching current monitoring data:', selectError);
      return;
    }
      
    let history = currentMonitoring?.run_history || [];
    history.unshift({ status: status, timestamp: new Date().toISOString() });
    const newHistory = history.slice(0, 5);
    
    const successful_runs_last_5 = newHistory.filter(run => run.status === 'completed').length;
    const failed_runs_last_5 = newHistory.filter(run => run.status === 'failed').length;
    
    const { error: upsertError } = await supabase
      .from('autonomis_job_monitoring')
      .upsert({
        notebook_id: notebookId,
        notebook_name: notebookName || 'Untitled Notebook',
        last_known_job_id: job.id,
        last_run_time: new Date().toISOString(),
        last_run_status: status,
        run_history: newHistory,
        successful_runs_last_5: successful_runs_last_5,
        failed_runs_last_5: failed_runs_last_5,
        is_active: true,
      }, { onConflict: 'notebook_id' });
  
    if (upsertError) {
      console.error('[Monitoring] Error upserting monitoring data:', upsertError);
    } else {
      console.log(`[Monitoring] Successfully updated notebook ${notebookId}. Success count: ${successful_runs_last_5}, Fail count: ${failed_runs_last_5}`);
    }
  } catch(e) {
    console.error('[Monitoring] A critical error occurred during the update process:', e);
  }
};

console.log('[Worker] SQL Execution Worker is starting...');

// The worker is now instantiated with the ioredis connection object.
const worker = new Worker('sql-execution-queue', processor, { connection });

worker.on('completed', (job, result) => {
  console.log(`[Worker-Event] Job ${job.id} has completed with status: ${result.status}`);
  updateMonitoringData(job, 'completed');
});

worker.on('failed', (job, err) => {
  console.error(`[Worker-Event] Job ${job.id} has failed with error: ${err.message}`);
  updateMonitoringData(job, 'failed');
});