const priorityMap = { HIGH: 1, MEDIUM: 2, LOW: 3 };
const queue = [];

function enqueueBatches(ingestionId, priority, createdTime, batchIds) {
  batchIds.forEach(batchId => {
    queue.push({
      priority: priorityMap[priority],
      createdTime,
      ingestionId,
      batchId,
    });
  });
  
  queue.sort((a, b) => a.priority - b.priority || a.createdTime - b.createdTime);
}

function dequeueBatch() {
  if (queue.length === 0) return null;
  return queue.shift();
}

function queueLength() {
  return queue.length;
}

module.exports = { enqueueBatches, dequeueBatch, queueLength };
