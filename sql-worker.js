require('dotenv').config();

const { Worker } = require('bullmq');
const connection = require('../lib/redis');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');

if (!process.env.NEXT_PUBLIC_SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error('Supabase environment variables (URL and Service Role Key) are not set! Check your .env.local file.');
}
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

// Helper function to get database configuration for a notebook
const getDatabaseConfig = async (notebookId) => {
  try {
    const { data: notebook, error } = await supabase
      .from('autonomis_user_notebook')
      .select(`
        *,
        autonomis_user_database (
          *,
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

    if (!notebook || !notebook.autonomis_user_database) {
      throw new Error(`No database configuration found for notebook ${notebookId}`);
    }

    const dbConfig = JSON.parse(notebook.autonomis_user_database.connection_string);
    const dbType = notebook.autonomis_user_database.autonomis_data_source.name.toLowerCase();

    console.log(`[Worker] Database config loaded for notebook ${notebookId}: ${dbType}`);
    return { dbConfig, dbType };

  } catch (error) {
    console.error(`[Worker] Failed to get database config for notebook ${notebookId}:`, error);
    throw error;
  }
};

// Helper function to execute a single SQL query via the API
const executeQuery = async (query, dbType, dbConfig, queryId) => {
  try {
    // Use server-side API_BASE_URL first, fallback to public URL, then localhost for dev
    const baseUrl = process.env.API_BASE_URL || 
                   process.env.NEXT_PUBLIC_VERCEL_URL || 
                   process.env.NEXT_PUBLIC_BASE_URL || 
                   'http://localhost:3000';

    if (!process.env.WORKER_SECRET_KEY) {
      throw new Error('WORKER_SECRET_KEY is not defined in the environment.');
    }
    
    console.log(`[Worker] Making API call to: ${baseUrl}/api/executequery`);
    const response = await axios.post(`${baseUrl}/api/executequery`, {
      sqlstr: query,
      dbtype: dbType,
      dbConfig: dbConfig
    }, {
      timeout: 300000, // 5 minutes timeout
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${process.env.WORKER_SECRET_KEY}`
      }
    });

    console.log(`[Worker] Query ${queryId} executed successfully. Rows returned: ${response.data?.length || 0}`);
    return {
      success: true,
      data: response.data,
      rowCount: response.data?.length || 0
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

const processor = async (job) => {
  const { s3Key, notebookId } = job.data;

  if (!s3Key) {
    throw new Error('Job failed: The job data is missing the required "s3Key" property.');
  }

  if (!notebookId) {
    throw new Error('Job failed: The job data is missing the required "notebookId" property.');
  }

  console.log(`[Worker] Received job #${job.id}. Starting processing for S3 file: ${s3Key}, Notebook ID: ${notebookId}`);

  try {
    // Step 1: Get database configuration for this notebook
    console.log(`[Worker] Fetching database configuration for notebook ${notebookId}...`);
    const { dbConfig, dbType } = await getDatabaseConfig(notebookId);

    // Step 2: Download SQL queries from S3
    console.log(`[Worker] Downloading SQL queries from S3: ${s3Key}...`);
    const command = new GetObjectCommand({
      Bucket: process.env.AWS_S3_BUCKET_NAME,
      Key: s3Key, 
    });
    
    const response = await s3Client.send(command);
    const queriesJsonString = await response.Body.transformToString();

    const queryObjects = JSON.parse(queriesJsonString);
    console.log(`[Worker] Successfully loaded ${queryObjects.length} queries from ${s3Key}.`);

    // Step 3: Execute each query
    let successCount = 0;
    let failureCount = 0;
    const executionResults = [];

    for (let i = 0; i < queryObjects.length; i++) {
      const item = queryObjects[i];
      const queryToRun = typeof item === 'string' ? item : item.query;
      const queryId = item.id || `query_${i + 1}`;
      const variableName = item.variableName || `result_${i + 1}`;

      console.log(`[Worker] Executing query ${i + 1}/${queryObjects.length} - ID: ${queryId}, Variable: ${variableName}`);
      console.log(`[Worker] Query: "${queryToRun.substring(0, 100)}${queryToRun.length > 100 ? '...' : ''}"`);

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

      // Store execution result
      executionResults.push({
        queryId,
        variableName,
        query: queryToRun.substring(0, 200), // Store first 200 chars for reference
        success: result.success,
        rowCount: result.rowCount || 0,
        executionTime,
        error: result.error,
        timestamp: new Date().toISOString()
      });

      // Add small delay between queries to prevent overwhelming the database
      if (i < queryObjects.length - 1) {
        await sleep(500);
      }
    }

    const resultMessage = `Successfully executed ${successCount}/${queryObjects.length} queries from file ${s3Key}. ${failureCount} queries failed.`;
    console.log(`[Worker] Job #${job.id} completed. ${resultMessage}`);
    
    return { 
      status: failureCount === 0 ? 'Completed' : 'Completed with errors', 
      message: resultMessage,
      successCount,
      failureCount,
      totalQueries: queryObjects.length,
      executionResults
    };

  } catch (error) {
    console.error(`[Worker] Job #${job.id} failed with an error:`, error.message);
    throw error;
  }
};

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
      .select('run_history')
      .eq('notebook_id', notebookId)
      .single();
      
    let history = currentMonitoring ? currentMonitoring.run_history || [] : [];
  
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
      console.log(`[Monitoring] Successfully updated notebook ${notebookId}.`);
    }
  } catch(e) {
    console.error('[Monitoring] A critical error occurred during the update process:', e);
  }
};

console.log('[Worker] Worker process is starting up and waiting for jobs...');
const worker = new Worker('sql-execution-queue', processor, { connection });
worker.on('completed', (job, result) => {
  console.log(`[Worker-Event] Job ${job.id} completed.`);
  updateMonitoringData(job, 'completed');
});
worker.on('failed', (job, err) => {
  console.error(`[Worker-Event] Job ${job.id} failed.`);
  updateMonitoringData(job, 'failed');
});