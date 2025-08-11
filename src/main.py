import time
import pandas as pd
from modules.utils import setup_logging, get_engine, parse_args
from modules.ingest import ingest_csv_to_staging
from modules.transform import transform_bank_txns, transform_customer_txns
from modules.load import load_to_mysql

def main():
    etl_start_time = time.time()
    args = parse_args()
    logger = setup_logging()

    engine = get_engine(args.db_user, args.db_pass, args.db_host, args.db_port, args.db_name)

    # Stage Ingest: Ingest into staging (truncate + full load)
    ingest_csv_to_staging(engine, args.data_path, "stg_transactions", logger)

    # Stage Transform: Transform BANK/CUST transactions read from staging
    df_staging = pd.read_sql("SELECT * FROM stg_transactions", engine)
    bank_df = transform_bank_txns(df_staging, logger)
    customer_df = transform_customer_txns(df_staging, logger)

    # Stage Load: Load transformed results to analytics tables
    load_to_mysql(engine, bank_df, "ana_bank_daily_summary", logger, entity_name="BANK")
    load_to_mysql(engine, customer_df, "ana_customer_daily_summary", logger, entity_name="CUSTOMER")

    elapsed_time = time.time() - etl_start_time
    logger.info(f"ETL pipeline completed in {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    main()
