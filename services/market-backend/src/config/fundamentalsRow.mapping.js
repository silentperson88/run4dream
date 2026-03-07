const cleanLabel = (label = "") =>
  String(label)
    .replace(/\s+/g, " ")
    .trim()
    .replace(/[+_-]+$/g, "")
    .trim();

const normalizeLabel = (label = "") =>
  cleanLabel(label)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/%/g, " percent ")
    .replace(/[().,/]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const row = (key, label, children = [], aliases = []) => ({
  key,
  label: cleanLabel(label),
  children: children.map(cleanLabel),
  aliases: aliases.map(cleanLabel),
});

module.exports = {
  cleanLabel,
  normalizeLabel,
  fundamentalsMapping: {
    company_info: {
      fields: [
        { key: "company_name", label: "Company Name" },
        { key: "about", label: "About" },
        { key: "key_points", label: "Key Points" },
      ],
      links: {
        list_key: "links",
        item_fields: [
          { key: "title", label: "Title" },
          { key: "url", label: "URL" },
        ],
      },
      top_ratios: [
        { key: "market_cap", label: "Market Cap" },
        { key: "current_price", label: "Current Price" },
        { key: "high_low", label: "High / Low", aliases: ["High/Low"] },
        { key: "stock_pe", label: "Stock P/E", aliases: ["P/E"] },
        { key: "book_value", label: "Book Value" },
        { key: "dividend_yield", label: "Dividend Yield" },
        { key: "roce", label: "ROCE" },
        { key: "roe", label: "ROE" },
        { key: "face_value", label: "Face Value" },
      ],
    },

    analysis: {
      pros_cons: {
        pros_key: "pros",
        cons_key: "cons",
      },
    },

    quarters: {
      rows: [
        row("sales", "Sales", ["YOY Sales Growth %"]),
        row("revenue", "Revenue", ["YOY Sales Growth %"]),
        row("financing_profit", "Financing Profit", []),
        row("financing_margin_percent", "Financing Margin %", []),
        row("raw_pdf", "Raw PDF"),
        row("expenses", "Expenses", ["Material Cost %", "Employee Cost %"]),
        row("operating_profit", "Operating Profit"),
        row("opm_percent", "OPM %"),
        row("other_income", "Other Income", ["Other income normal"]),
        row("interest", "Interest"),
        row("depreciation", "Depreciation"),
        row("profit_before_tax", "Profit before tax"),
        row("tax_percent", "Tax %"),
        row("net_profit", "Net Profit", [
          "YOY Profit Growth %",
          "Exceptional items",
          "Profit excl Excep",
          "Exceptional items AT",
          "Profit for EPS",
          "Minority share",
          "Profit for PE",
        ]),
        row("eps", "EPS in Rs"),
        row("gross_npa_percent", "Gross NPA %"),
        row("net_npa_percent", "Net NPA %"),
      ],
    },

    profit_loss: {
      rows: [
        row("sales", "Sales", ["Sales Growth %"]),
        row("revenue", "Revenue", ["Sales Growth %"]),
        row("financing_profit", "Financing Profit", []),
        row("financing_margin_percent", "Financing Margin %", []),
        row("expenses", "Expenses", [
          "Manufacturing Cost %",
          "Material Cost %",
          "Employee Cost %",
          "Other Cost %",
        ]),
        row("operating_profit", "Operating Profit"),
        row("opm_percent", "OPM %"),
        row("other_income", "Other Income", ["Other income normal"]),
        row("interest", "Interest"),
        row("depreciation", "Depreciation"),
        row("profit_before_tax", "Profit before tax"),
        row("tax_percent", "Tax %"),
        row("net_profit", "Net Profit", [
          "Profit Growth %",
          "Exceptional items",
          "Profit excl Excep",
          "Exceptional items AT",
          "Profit for EPS",
          "Minority share",
          "Profit for PE",
        ]),
        row("eps", "EPS in Rs"),
        row("dividend_payout_percent", "Dividend Payout %"),
      ],
      other_details: {
        compounded_sales_growth: {
          label: "Compounded Sales Growth",
          periods: {
            "10 Years:": "csg_10y",
            "5 Years:": "csg_5y",
            "3 Years:": "csg_3y",
            "TTM:": "csg_ttm",
          },
        },
        compounded_profit_growth: {
          label: "Compounded Profit Growth",
          periods: {
            "10 Years:": "cpg_10y",
            "5 Years:": "cpg_5y",
            "3 Years:": "cpg_3y",
            "TTM:": "cpg_ttm",
          },
        },
        stock_price_cagr: {
          label: "Stock Price CAGR",
          periods: {
            "10 Years:": "spc_10y",
            "5 Years:": "spc_5y",
            "3 Years:": "spc_3y",
            "1 Year:": "spc_1y",
          },
        },
        return_on_equity: {
          label: "Return on Equity",
          periods: {
            "10 Years:": "roe_10y",
            "5 Years:": "roe_5y",
            "3 Years:": "roe_3y",
            "Last Year:": "roe_last_year",
          },
        },
      },
    },

    balance_sheet: {
      rows: [
        row("equity_capital", "Equity Capital"),
        row("reserves", "Reserves"),
        row("borrowing", "Borrowing"),
        row("deposits", "Deposits"),
        row("borrowings", "Borrowings", [
          "Long term Borrowings",
          "Short term Borrowings",
          "Other Borrowings",
        ]),
        row("other_liabilities", "Other Liabilities", [
          "Advance from Customers",
          "Lease Liabilities",
          "Trade Payables",
          "Other liability items",
          "Non controlling int",
        ]),
        row("total_liabilities", "Total Liabilities"),
        row("fixed_assets", "Fixed Assets", [
          "Gross Block",
          "Accumulated Depreciation",
          "Building",
          "Land",
          "Plant Machinery",
          "Railway sidings",
          "Vehicles",
          "Computers",
          "Furniture n fittings",
          "Equipments",
          "Other fixed assets",
          "Intangible Assets",
        ]),
        row("cwip", "CWIP"),
        row("investments", "Investments"),
        row("other_assets", "Other Assets", [
          "Inventories",
          "Trade receivables",
          "Cash Equivalents",
          "Loans n Advances",
          "Other asset items",
        ]),
        row("total_assets", "Total Assets"),
      ],
    },

    cash_flow: {
      rows: [
        row("cash_from_operating_activity", "Cash from Operating Activity", [
          "Profit from operations",
          "Working capital changes",
          "Receivables",
          "Inventory",
          "Payables",
          "Other WC items",
          "Direct taxes",
          "Interest received",
          "Dividends received",
          "Exceptional CF items",
        ]),
        row("cash_from_investing_activity", "Cash from Investing Activity", [
          "Investments purchased",
          "Investments sold",
          "Fixed assets purchased",
          "Fixed assets sold",
          "Acquisition of companies",
          "Invest in subsidiaries",
          "Loans Advances",
          "Other investing items",
        ]),
        row("cash_from_financing_activity", "Cash from Financing Activity", [
          "Proceeds from borrowings",
          "Repayment of borrowings",
          "Interest paid fin",
          "Dividends paid",
          "Financial liabilities",
          "Share application money",
          "Other financing items",
        ]),
        row("net_cash_flow", "Net Cash Flow"),
      ],
    },

    ratios: {
      rows: [
        row("debtor_days", "Debtor Days"),
        row("inventory_days", "Inventory Days"),
        row("days_payable", "Days Payable"),
        row("cash_conversion_cycle", "Cash Conversion Cycle"),
        row("working_capital_days", "Working Capital Days"),
        row("roce_percent", "ROCE %"),
        row("roe_percent", "ROE %"),
      ],
    },

    shareholdings: {
      rows: [
        row("promoters", "Promoters", ["<dynamic_holder_rows>"]),
        row("fiis", "FIIs", ["<dynamic_holder_rows>"]),
        row("diis", "DIIs", ["<dynamic_holder_rows>"]),
        row("public", "Public", [
          "<dynamic_holder_rows>",
          "No. of Shareholders",
        ]),
      ],
      notes: [
        "Shareholding child rows are dynamic per stock and quarter.",
        "Match known rows first; persist unknown rows under a dynamic children list.",
      ],
    },
  },
};
