const express = require('express');
const { v4: uuidv4 } = require('uuid');

const app = express();
const port = 3000;


const ingestions = {};


app.post('/ingest', express.json(), (req, res) => {
  const { ids, priority } = req.body;

  if (!ids || !Array.isArray(ids) || !priority) {
    return res.status(400).json({ error: 'Invalid payload' });
  }

  const ingestionId = uuidv4();
  
  const batches = [];
  for (let i = 0; i < ids.length; i += 3) {
    batches.push({
      batch_id: uuidv4(),
      ids: ids.slice(i, i + 3),
      status: 'yet_to_start',
    });
  }

  ingestions[ingestionId] = {
    ingestion_id: ingestionId,
    priority,
    status: 'yet_to_start',
    batches,
    createdAt: Date.now(),
  };

  
  res.json({ ingestion_id: ingestionId });

  
});


app.get('/status/:ingestion_id', (req, res) => {
  const ingestionId = req.params.ingestion_id;
  const ingestion = ingestions[ingestionId];

  if (!ingestion) {
    return res.status(404).json({ error: 'Ingestion ID not found' });
  }


  const batchStatuses = ingestion.batches.map((b) => b.status);
  let overallStatus = 'yet_to_start';
  if (batchStatuses.every((s) => s === 'completed')) {
    overallStatus = 'completed';
  } else if (batchStatuses.some((s) => s === 'triggered')) {
    overallStatus = 'triggered';
  }

  res.json({
    ingestion_id: ingestion.ingestion_id,
    status: overallStatus,
    batches: ingestion.batches,
  });
});

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
