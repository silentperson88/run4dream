const { pool } = require("../src/config/db");

const portfolioTypes = [
  {
    code: "MANUAL",
    display_name: "Manual Portfolio",
    description: "Trade freely with no restrictions. Best for experienced users.",
    fund: null,
    risk_level: "NONE",
    rules_json: {},
    important_notes: [
      "This portfolio has no restrictions.",
      "You are fully responsible for your trades and risk.",
      "Ideal for learning by doing.",
    ],
  },
  {
    code: "IPO_SIM",
    display_name: "IPO Learning Portfolio",
    description:
      "Simulate IPO bidding with dummy money to understand allotment, listing gains, and risks.",
    fund: 15000,
    risk_level: "HIGH",
    rules_json: {},
    important_notes: [
      "This is a learning-only IPO simulation portfolio.",
      "All allotments are simulated and not guaranteed.",
      "You cannot sell before listing.",
    ],
  },
  {
    code: "GUIDED_LOW",
    display_name: "Low Risk Portfolio",
    description:
      "Designed for conservative investors focusing on stable and low-volatility stocks.",
    fund: 200000,
    risk_level: "LOW",
    rules_json: {},
    important_notes: [
      "This portfolio focuses on stability over high returns.",
      "Ideal for beginners and long-term investors.",
      "Low volatility does not mean zero risk.",
    ],
  },
  {
    code: "GUIDED_MEDIUM",
    display_name: "Medium Risk Portfolio",
    description:
      "Balanced risk-reward portfolio suitable for swing and mid-term investing.",
    fund: 150000,
    risk_level: "MEDIUM",
    rules_json: {},
    important_notes: [
      "This portfolio balances growth and risk.",
      "Suitable for users with some market experience.",
      "Losses are possible during market volatility.",
    ],
  },
  {
    code: "GUIDED_HIGH",
    display_name: "High Risk Portfolio",
    description:
      "Aggressive portfolio for high volatility and high return potential.",
    fund: 100000,
    risk_level: "HIGH",
    rules_json: {},
    important_notes: [
      "High risk portfolio with significant volatility.",
      "Losses can be rapid and substantial.",
      "Recommended only for experienced users.",
    ],
  },
  {
    code: "RETIREMENT",
    display_name: "Retirement Plan",
    description:
      "Long-term investment portfolio focused on wealth creation and capital preservation.",
    fund: 500000,
    risk_level: "LOW",
    rules_json: {},
    important_notes: [
      "Designed for long-term wealth creation.",
      "Frequent trading is discouraged.",
      "Compounding works best with patience.",
    ],
  },
  {
    code: "STRATEGY_AUTO",
    display_name: "Auto Trade Portfolio",
    description:
      "Automated trading portfolio driven by AI or predefined strategies.",
    fund: 200000,
    risk_level: "MEDIUM",
    rules_json: {},
    important_notes: [
      "Trades are executed automatically.",
      "Performance depends on strategy logic.",
      "Always monitor risk and performance.",
    ],
  },
];

async function seedPortfolioTypes(client = pool) {
  for (const type of portfolioTypes) {
    await client.query(
      `
        INSERT INTO portfolio_type (
          code, display_name, description, fund, risk_level, rules_json, important_notes, is_active
        )
        VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, TRUE)
        ON CONFLICT (code)
        DO UPDATE SET
          display_name = EXCLUDED.display_name,
          description = EXCLUDED.description,
          fund = EXCLUDED.fund,
          risk_level = EXCLUDED.risk_level,
          rules_json = EXCLUDED.rules_json,
          important_notes = EXCLUDED.important_notes,
          is_active = EXCLUDED.is_active,
          updated_at = NOW()
      `,
      [
        type.code,
        type.display_name,
        type.description,
        type.fund,
        type.risk_level,
        JSON.stringify(type.rules_json || {}),
        JSON.stringify(type.important_notes || []),
      ],
    );
  }

  console.log("Portfolio types seeded successfully");
}

module.exports = seedPortfolioTypes;
