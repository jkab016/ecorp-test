import logging
import argparse
from sqlalchemy import create_engine
import os

def setup_logging(log_dir="logs", log_file="pipeline.log"):
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, log_file)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_path, mode="a")
        ]
    )
    return logging.getLogger(__name__)

def get_engine(user, password, host, port, db):
    uri = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}"
    return create_engine(uri)

def parse_args():
    parser = argparse.ArgumentParser(description="ETL Pipeline Parameters")
    parser.add_argument("--data-path", required=False, help="Path to txns CSV file", default="data/mock_transactions.csv")
    parser.add_argument("--db-user", default="", help="MySQL username")
    parser.add_argument("--db-pass", default="", help="MySQL password")
    parser.add_argument("--db-host", default="localhost", help="MySQL host")
    parser.add_argument("--db-port", default=3306, type=int, help="MySQL port")
    parser.add_argument("--db-name", default="eft_db", help="Database name")
    parser.add_argument("--log-dir", default="../../logs", help="Directory for log files")
    parser.add_argument("--log-file", default="app.log", help="Log file name")
    return parser.parse_args()