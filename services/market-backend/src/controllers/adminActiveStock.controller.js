const rawstockService = require("../services/rawstock.service");
const ActiveStockService = require("../services/activestock.service");

async function activateStock(req, res) {
  try {
    const { rawStockId, master_id } = req.body;

    if (!rawStockId || !master_id) {
      return res.status(400).json({
        success: false,
        message: "rawStockId and master_id are required",
      });
    }

    const rawStock = await rawstockService.getRawStockById(rawStockId);
    if (!rawStock) {
      return res.status(404).json({ success: false, message: "Stock not found" });
    }

    const activeStock = await ActiveStockService.addStock({
      token: rawStock.token,
      symbol: rawStock.symbol,
      name: rawStock.name,
      exchange: rawStock.exch_seg || rawStock.exchange,
      instrumenttype: rawStock.instrumenttype,
      master_id,
    });

    return res.json({ success: true, data: activeStock });
  } catch (err) {
    if (err.code === "23505") {
      return res.status(409).json({
        success: false,
        message: "Stock already added",
      });
    }
    return res.status(500).json({ success: false, message: err.message });
  }
}

module.exports = { activateStock };
