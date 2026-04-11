-- Drop the old structured fundamentals layer so it can be rebuilt table by table.

DROP TABLE IF EXISTS stock_fundamental_shareholding_periods CASCADE;
DROP TABLE IF EXISTS stock_fundamental_ratios_periods CASCADE;
DROP TABLE IF EXISTS stock_fundamental_cash_flow_periods CASCADE;
DROP TABLE IF EXISTS stock_fundamental_balance_sheet_periods CASCADE;
DROP TABLE IF EXISTS stock_fundamental_profit_loss_periods CASCADE;
DROP TABLE IF EXISTS stock_fundamental_quarterly_results CASCADE;
DROP TABLE IF EXISTS stock_fundamental_peers_snapshot CASCADE;
DROP TABLE IF EXISTS stock_fundamental_overview CASCADE;
