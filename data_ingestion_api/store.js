const { v4: uuidv4 } = require('uuid');

const store = {
  ingestions: {},
  batches: {},
};

function createIngestion(ids, priority) {
  const ingestionId = uuidv4();
  const createdTime = Date.now();

  
  const batches = [];
  for (let i = 0; i < ids.length; i += 3) {
    const batchIds = ids.slice(i, i + 3);
    const batchId = uuidv4();
    batches.push({ batchId, ids: batchIds, status: 'yet_to_start' });
  }

  store.ingestions[ingestionId] = {
    ingestionId,
    priority,
    createdTime,
    status: 'yet_to_start',
    batches: batches.map(b => b.batchId),
  };

  batches.forEach(batch => {
    store.batches[batch.batchId] = batch;
  });

  return ingestionId;
}

function updateBatchStatus(batchId, status) {
  if (store.batches[batchId]) {
    store.batches[batchId].status = status;
  }
}

function updateIngestionStatus(ingestionId) {
  const ingestion = store.ingestions[ingestionId];
  if (!ingestion) return;

  const batchStatuses = ingestion.batches.map(bid => store.batches[bid]?.status);

  if (batchStatuses.every(status => status === 'yet_to_start')) {
    ingestion.status = 'yet_to_start';
  } else if (batchStatuses.every(status => status === 'completed')) {
    ingestion.status = 'completed';
  } else if (batchStatuses.some(status => status === 'triggered')) {
    ingestion.status = 'triggered';
  } else {
    ingestion.status = 'yet_to_start';
  }
}

module.exports = {
  store,
  createIngestion,
  updateBatchStatus,
  updateIngestionStatus,
};
