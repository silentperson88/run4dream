const { withTransaction } = require("../repositories/tx");
const userRepo = require("../repositories/users.repository");
const portfolioRepo = require("../repositories/userPortfolios.repository");
const { toNumber } = require("../repositories/common");

const appendLedgerEntry = (ledger, entry) => {
  const now = new Date().toISOString();
  const current = Array.isArray(ledger) ? ledger : [];
  current.push({ ...entry, created_at: now });
  return current;
};

async function loadWalletFund({ user_id, amount, source = "WALLET_LOAD" }) {
  return withTransaction(async (client) => {
    const user = await userRepo.getById(user_id, client, { forUpdate: true });
    if (!user || !user.isActive) {
      throw new Error("User not found or inactive");
    }

    const amt = toNumber(amount);
    const wallet_fund = user.wallet_fund + amt;
    const total_fund_added = user.total_fund_added + amt;
    const total_fund_withdrawn = user.total_fund_withdrawn;
    const wallet_ledger = appendLedgerEntry(user.wallet_ledger, {
      type: "CREDIT",
      amount: amt,
      source,
      portfolio_id: null,
      balance_after: wallet_fund,
    });

    const updated = await userRepo.updateWalletState(
      user.id,
      { wallet_fund, total_fund_added, total_fund_withdrawn, wallet_ledger },
      client,
    );

    return {
      wallet_fund: updated.wallet_fund,
      total_fund_added: updated.total_fund_added,
      total_fund_withdrawn: updated.total_fund_withdrawn,
    };
  });
}

async function transferWalletToPortfolio({ user_id, portfolio_id, amount }) {
  return withTransaction(async (client) => {
    const user = await userRepo.getById(user_id, client, { forUpdate: true });
    if (!user || !user.isActive) {
      throw new Error("User not found or inactive");
    }

    const portfolio = await portfolioRepo.getActiveById(
      portfolio_id,
      user_id,
      client,
      { forUpdate: true },
    );

    if (!portfolio) throw new Error("Portfolio not found");

    const amt = toNumber(amount);
    if (user.wallet_fund < amt) {
      throw new Error("Insufficient wallet fund");
    }

    const wallet_fund = user.wallet_fund - amt;
    const wallet_ledger = appendLedgerEntry(user.wallet_ledger, {
      type: "DEBIT",
      amount: amt,
      source: "WALLET_TO_PORTFOLIO",
      portfolio_id: Number(portfolio_id),
      balance_after: wallet_fund,
    });

    const updatedUser = await userRepo.updateWalletState(
      user.id,
      {
        wallet_fund,
        total_fund_added: user.total_fund_added,
        total_fund_withdrawn: user.total_fund_withdrawn,
        wallet_ledger,
      },
      client,
    );

    const updatedPortfolio = await portfolioRepo.updateFinancialState(
      portfolio.id,
      {
        available_fund: portfolio.available_fund + amt,
        initial_fund: portfolio.initial_fund === 0 ? amt : portfolio.initial_fund,
      },
      client,
    );

    return {
      wallet_fund: updatedUser.wallet_fund,
      total_fund_added: updatedUser.total_fund_added,
      total_fund_withdrawn: updatedUser.total_fund_withdrawn,
      portfolio_available_fund: updatedPortfolio.available_fund,
    };
  });
}

async function transferPortfolioToWallet({ user_id, portfolio_id, amount }) {
  return withTransaction(async (client) => {
    const user = await userRepo.getById(user_id, client, { forUpdate: true });
    if (!user || !user.isActive) {
      throw new Error("User not found or inactive");
    }

    const portfolio = await portfolioRepo.getActiveById(
      portfolio_id,
      user_id,
      client,
      { forUpdate: true },
    );

    if (!portfolio) throw new Error("Portfolio not found");

    const amt = toNumber(amount);
    if (portfolio.available_fund < amt) {
      throw new Error("Insufficient portfolio fund");
    }

    const wallet_fund = user.wallet_fund + amt;
    const wallet_ledger = appendLedgerEntry(user.wallet_ledger, {
      type: "CREDIT",
      amount: amt,
      source: "PORTFOLIO_TO_WALLET",
      portfolio_id: Number(portfolio_id),
      balance_after: wallet_fund,
    });

    const updatedUser = await userRepo.updateWalletState(
      user.id,
      {
        wallet_fund,
        total_fund_added: user.total_fund_added,
        total_fund_withdrawn: user.total_fund_withdrawn,
        wallet_ledger,
      },
      client,
    );

    const updatedPortfolio = await portfolioRepo.updateFinancialState(
      portfolio.id,
      { available_fund: portfolio.available_fund - amt },
      client,
    );

    return {
      wallet_fund: updatedUser.wallet_fund,
      total_fund_added: updatedUser.total_fund_added,
      total_fund_withdrawn: updatedUser.total_fund_withdrawn,
      portfolio_available_fund: updatedPortfolio.available_fund,
    };
  });
}

async function withdrawWalletFund({ user_id, amount, source = "WALLET_WITHDRAW" }) {
  return withTransaction(async (client) => {
    const user = await userRepo.getById(user_id, client, { forUpdate: true });
    if (!user || !user.isActive) {
      throw new Error("User not found or inactive");
    }

    const amt = toNumber(amount);
    if (user.wallet_fund < amt) {
      throw new Error("Insufficient wallet fund");
    }

    const wallet_fund = user.wallet_fund - amt;
    const total_fund_withdrawn = user.total_fund_withdrawn + amt;
    const wallet_ledger = appendLedgerEntry(user.wallet_ledger, {
      type: "DEBIT",
      amount: amt,
      source,
      portfolio_id: null,
      balance_after: wallet_fund,
    });

    const updatedUser = await userRepo.updateWalletState(
      user.id,
      {
        wallet_fund,
        total_fund_added: user.total_fund_added,
        total_fund_withdrawn,
        wallet_ledger,
      },
      client,
    );

    return {
      wallet_fund: updatedUser.wallet_fund,
      total_fund_added: updatedUser.total_fund_added,
      total_fund_withdrawn: updatedUser.total_fund_withdrawn,
    };
  });
}

async function getWalletLedger({ user_id, limit = 50, skip = 0 }) {
  const user = await userRepo.getById(user_id);
  if (!user) {
    throw new Error("User not found");
  }

  const ledger = Array.isArray(user.wallet_ledger) ? user.wallet_ledger : [];
  const items = ledger
    .slice()
    .sort((a, b) => {
      const aTime = a?.created_at ? new Date(a.created_at).getTime() : 0;
      const bTime = b?.created_at ? new Date(b.created_at).getTime() : 0;
      return bTime - aTime;
    })
    .slice(Number(skip), Number(skip) + Number(limit));

  return {
    wallet_fund: user.wallet_fund,
    total_fund_added: user.total_fund_added,
    total_fund_withdrawn: user.total_fund_withdrawn,
    total: ledger.length,
    items,
  };
}

module.exports = {
  loadWalletFund,
  transferWalletToPortfolio,
  transferPortfolioToWallet,
  withdrawWalletFund,
  getWalletLedger,
};
