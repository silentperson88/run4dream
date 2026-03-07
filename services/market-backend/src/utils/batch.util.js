function createBatches(items, batchSize) {
  const nseItems = [];
  const bseItems = [];

  // 1️⃣ Split items by exchange (symbol#EXCHANGE)
  for (const item of items) {
    const parts = item.split("#");
    const exchange = parts[1]?.toUpperCase();

    if (exchange === "NSE") {
      nseItems.push(parts[0]);
    } else if (exchange === "BSE") {
      bseItems.push(parts[0]);
    }
  }

  // 2️⃣ Helper to create batches
  const createExchangeBatches = (arr) => {
    const batches = [];
    for (let i = 0; i < arr.length; i += batchSize) {
      batches.push(arr.slice(i, i + batchSize));
    }
    return batches;
  };

  // 3️⃣ Create batches per exchange
  const nseBatches = createExchangeBatches(nseItems);
  const bseBatches = createExchangeBatches(bseItems);

  // 4️⃣ Combine and return
  // const totalBatches = [...bseBatches, ...nseBatches];
  const totalBatches = [...nseBatches, ...bseBatches];

  return totalBatches;
}

module.exports = createBatches;
