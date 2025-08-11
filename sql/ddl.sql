--database
create database IF NOT EXISTS eft_db;

-- STAGING: Raw ingested transactions
CREATE TABLE IF NOT EXISTS stg_transactions (
  transaction_id VARCHAR(50) NOT NULL,
  bank_id INT NOT NULL,
  customer_id INT,
  transaction_amount DECIMAL(15,2) NOT NULL,
  transaction_date DATE NOT NULL,
  transaction_ts DATETIME DEFAULT CURRENT_TIMESTAMP,
  source_file VARCHAR(255),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (transaction_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE INDEX idx_stg_transactions_date ON stg_transactions (transaction_date);
CREATE INDEX idx_stg_transactions_bank ON stg_transactions (bank_id);
CREATE INDEX idx_stg_transactions_bank_date ON stg_transactions (bank_id, transaction_date);

-- ANALYTICS: Final tables for dashboards/reports
-- Aggregated daily bank totals
CREATE TABLE IF NOT EXISTS ana_bank_daily_summary (
  bank_id INT NOT NULL,
  agg_date DATE NOT NULL,
  total_amount DECIMAL(18,2) NOT NULL,
  num_transactions INT NOT NULL DEFAULT 0,
  notes VARCHAR(500),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (bank_id, agg_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE INDEX idx_ana_bank_daily_summary_date ON ana_bank_daily_summary (agg_date);


-- Aggregated daily customer totals
CREATE TABLE IF NOT EXISTS ana_customer_daily_summary (
  customer_id INT NOT NULL,
  agg_date DATE NOT NULL,
  total_amount DECIMAL(18,2) NOT NULL,
  num_transactions INT NOT NULL DEFAULT 0,
  notes VARCHAR(500),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (customer_id, agg_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE INDEX idx_ana_customer_daily_summary_date ON ana_customer_daily_summary (agg_date);
