class QueueService {
  constructor(intervalMs = 5000) {
    this.queue = [];
    this.isProcessing = false;
    this.intervalMs = intervalMs;
  }

  enqueue(taskFn) {
    return new Promise((resolve, reject) => {
      this.queue.push({ taskFn, resolve, reject });
      this.processQueue();
    });
  }

  async processQueue() {
    if (this.isProcessing) return;
    if (this.queue.length === 0) return;

    this.isProcessing = true;

    const { taskFn, resolve, reject } = this.queue.shift();

    try {
      const result = await taskFn();
      resolve(result);
    } catch (err) {
      reject(err);
    }

    // enforce 5-sec gap before next request
    setTimeout(() => {
      this.isProcessing = false;
      this.processQueue();
    }, this.intervalMs);
  }
}

// singleton (VERY IMPORTANT)
module.exports = new QueueService(5000);
