import pandas as pd
import time

def load_to_mysql(engine, df, table_name, logger, entity_name="DATA"):
    """
    Loads a DataFrame into MySQL table with idempotency checks.
    entity_name: label for logging ("BANK" or "CUSTOMER")
    """
    start_time = time.time()
    logger.info(f"[LOAD-{entity_name}] Starting load stage")

    if df.empty:
        logger.warning(f"[LOAD-{entity_name}] No data to load")
        logger.info(f"[LOAD-{entity_name}] Completed load stage in {time.time() - start_time:.2f} seconds")
        return

    # Idempotency: delete existing rows for same agg_dates before insert
    df["agg_date"] = pd.to_datetime(df["agg_date"])
    unique_dates = "', '".join(df["agg_date"].dt.strftime("%Y-%m-%d").unique())
    with engine.begin() as conn:
        conn.execute(f"DELETE FROM {table_name} WHERE agg_date IN ('{unique_dates}')")
    logger.info(f"[LOAD-{entity_name}] Deleted existing rows in {table_name} for dates: {unique_dates}")

    # Insert
    df.to_sql(table_name, engine, if_exists="append", index=False)
    logger.info(f"[LOAD-{entity_name}] Inserted {len(df)} rows into {table_name}")

    elapsed_time = time.time() - start_time
    logger.info(f"[LOAD-{entity_name}] Completed load stage in {elapsed_time:.2f} seconds")
