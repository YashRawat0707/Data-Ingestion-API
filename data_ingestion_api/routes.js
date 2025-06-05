const express = require('express');
const router = express.Router();
const { createIngestion, store } = require('./store');
const { enqueueBatches } = require('./queueManager');

router.post('/ingest', (req, res) => {
  const { ids, priority } = req.body;

  if (!ids || !Array.isArray(ids) || ids.some(id => typeof id !== 'number')) {
    return res.status(400).json({ error: 'Invalid ids array' });
  }

  if (!['HIGH', 'MEDIUM', 'LOW'].includes(priority)) {
    return res.status(400).json({ error: 'Invalid priority' });
  }

  const ingestionId = createIngestion(ids, priority);
  const ingestion = store.ingestions[ingestionId];

  enqueueBatches(ingestionId, priority, ingestion.createdTime, ingestion.batches);

  res.json({ ingestion_id: ingestionId });
});

router.get('/status/:ingestionId', (req, res) => {
  const ingestion = store.ingestions[req.params.ingestionId];
  if (!ingestion) {
    return res.status(404).json({ error: 'Ingestion ID not found' });
  }

  const batches = ingestion.batches.map(batchId => store.batches[batchId]);

  res.json({
    ingestion_id: ingestion.ingestionId,
    status: ingestion.status,
    batches,
  });
});

module.exports = router;
