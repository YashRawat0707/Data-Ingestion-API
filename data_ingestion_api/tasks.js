const { dequeueBatch, queueLength } = require('./queueManager');
const { store, updateBatchStatus, updateIngestionStatus } = require('./store');

const PROCESSING_INTERVAL_MS = 5000; // 1 batch per 5 seconds

async function processBatch(batch) {
  updateBatchStatus(batch.batchId, 'triggered');
  updateIngestionStatus(batch.ingestionId);

  
  await new Promise(resolve => setTimeout(resolve, 2000));

  
  updateBatchStatus(batch.batchId, 'completed');
  updateIngestionStatus(batch.ingestionId);

  console.log(`Processed batch ${batch.batchId} of ingestion ${batch.ingestionId}`);
}

async function batchWorker() {
  while (true) {
    const batch = dequeueBatch();

    if (batch) {
      await processBatch(batch);
     
      await new Promise(resolve => setTimeout(resolve, PROCESSING_INTERVAL_MS));
    } else {
      
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }
}

function startWorker() {
  batchWorker();
}

module.exports = { startWorker };
