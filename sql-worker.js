// sql-worker.js

// Load environment variables from the .env file in the same directory
require('dotenv').config();

const { Worker } = require('bullmq');
const IORedis = require('ioredis');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const { createClient } = require('@supabase/supabase-js');

// --- Environment Variable Validation ---
if (!process.env.REDIS_URL || !process.env.NEXT_PUBLIC_SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error('Crucial environment variables (REDIS_URL, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY) are not set! Please check your .env file.');
}

// --- Redis Connection ---
// The connection is created directly here using the environment variable.
const connection = new IORedis(process.env.REDIS_URL, {
  maxRetriesPerRequest: null, // Required by BullMQ
});

connection.on('connect', () => console.log('[Redis] Successfully connected to Redis.'));
connection.on('error', (err) => console.error('[Redis] Connection Error:', err.message));

// --- Supabase Client ---
const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// --- AWS S3 Client ---
const s3Client = new S3Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
  },
});

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// --- BullMQ Job Processor (Your existing logic) ---
const processor = async (job) => {
  const { s3Key } = job.data;
  if (!s3Key) throw new Error('Job failed: Missing "s3Key" in job data.');

  console.log(`[Worker] Received job #${job.id}. Processing S3 file: ${s3Key}`);
  try {
    const command = new GetObjectCommand({ Bucket: process.env.AWS_S3_BUCKET_NAME, Key: s3Key });
    const response = await s3Client.send(command);
    const queriesJsonString = await response.Body.transformToString();
    const queryObjects = JSON.parse(queriesJsonString);
    console.log(`[Worker] Loaded ${queryObjects.length} queries from ${s3Key}.`);
    for (const item of queryObjects) {
      const queryToRun = typeof item === 'string' ? item : item.query;
      console.log(`[Worker] Executing query: "${queryToRun.substring(0, 50)}..."`);
      await sleep(750);
    }
    console.log(`[Worker] Job #${job.id} completed successfully.`);
    return { status: 'Completed' };
  } catch (error) {
    console.error(`[Worker] Job #${job.id} failed:`, error.message);
    throw error;
  }
};

// --- Database Update Logic (Your existing logic) ---
const updateMonitoringData = async (job, status) => {
  if (!job || !job.data.notebookId) return;
  const { notebookId, notebookName } = job.data;
  console.log(`[Monitoring] Updating status for notebook ${notebookId} to "${status}".`);
  try {
    const { data: current } = await supabase.from('autonomis_job_monitoring').select('run_history').eq('notebook_id', notebookId).single();
    let history = current ? current.run_history || [] : [];
    history.unshift({ status, timestamp: new Date().toISOString() });
    const newHistory = history.slice(0, 5);
    const successful_runs_last_5 = newHistory.filter(run => run.status === 'completed').length;
    const failed_runs_last_5 = newHistory.filter(run => run.status === 'failed').length;
    
    await supabase.from('autonomis_job_monitoring').upsert({
      notebook_id: notebookId, notebook_name: notebookName || 'Untitled Notebook',
      last_known_job_id: job.id, last_run_time: new Date().toISOString(),
      last_run_status: status, run_history: newHistory,
      successful_runs_last_5, failed_runs_last_5, is_active: true,
    }, { onConflict: 'notebook_id' });
    console.log(`[Monitoring] Successfully updated notebook ${notebookId}.`);
  } catch (e) {
    console.error('[Monitoring] A critical error occurred during the update process:', e);
  }
};

// --- Worker Initialization ---
console.log('[Worker] Worker process starting up...');
const worker = new Worker('sql-execution-queue', processor, { connection });

worker.on('completed', (job) => updateMonitoringData(job, 'completed'));
worker.on('failed', (job) => updateMonitoringData(job, 'failed'));

console.log('[Worker] Waiting for jobs...');