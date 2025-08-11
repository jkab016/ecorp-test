import pandas as pd
import time

def ingest_csv_to_staging(engine, csv_path, staging_table, logger):
    start_time = time.time()
    logger.info("[INGEST] Starting ingestion stage")

    df = pd.read_csv(csv_path)
    logger.info(f"[INGEST] Read {len(df)} rows from {csv_path}")

    # Truncate staging table before load
    with engine.begin() as conn:
        conn.execute(f"TRUNCATE TABLE {staging_table}")
    logger.info(f"[INGEST] Truncated table {staging_table}")

    # Persist to staging
    df.to_sql(staging_table, engine, if_exists="append", index=False)
    logger.info(f"[INGEST] Inserted {len(df)} rows into {staging_table}")

    elapsed_time = time.time() - start_time
    logger.info(f"[INGEST] Completed ingestion stage in {elapsed_time:.2f} seconds")
    return df
