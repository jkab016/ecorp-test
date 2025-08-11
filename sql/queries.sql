-- 1. Top 5 banks by transaction volume in the last 7 days --
SELECT 
    bank_id,
    SUM(total_amount) AS total_volume
FROM ana_bank_daily_summary
WHERE agg_date >= CURDATE() - INTERVAL 7 DAY
GROUP BY bank_id
ORDER BY total_volume DESC
LIMIT 5;

-- 2. Average transaction value per customer for a given month --
-- vary the target_year and target_month as needed before running the query --
SET @target_year = 2025;
SET @target_month = 8;

SELECT 
    customer_id,
    AVG(total_amount / num_transactions) AS avg_transaction_value
FROM ana_customer_daily_summary
WHERE YEAR(agg_date) = @target_year
  AND MONTH(agg_date) = @target_month
GROUP BY customer_id
ORDER BY avg_transaction_value DESC
;