import pandas as pd
import time
from typing import Tuple

def transform_bank_txns(df: pd.DataFrame, logger) -> pd.DataFrame:
    """
    Transform raw transactions DataFrame into bank daily aggregates.
    Returns a DataFrame with (bank_id, agg_date, total_amount, num_transactions).
    """
    start_time = time.time()
    logger.info("[TRANSFORM-BANK] Starting bank transformation stage")

    # Validate required columns
    required_cols = ["bank_id", "transaction_date", "transaction_amount", "transaction_id"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        logger.error(f"[TRANSFORM-BANK] Missing columns: {missing}")
        raise ValueError(f"Missing columns: {missing}")

    # Convert types
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")
    df["transaction_amount"] = pd.to_numeric(df["transaction_amount"], errors="coerce")
    df["bank_id"] = pd.to_numeric(df["bank_id"], errors="coerce", downcast="integer")

    # Drop nulls in key fields
    before_rows = len(df)
    df = df.dropna(subset=["transaction_date", "transaction_amount", "bank_id"])
    logger.info(f"[TRANSFORM-BANK] Dropped {before_rows - len(df)} rows due to nulls in key fields")

    # Remove negative amounts
    before_rows = len(df)
    df = df[df["transaction_amount"] >= 0]
    logger.info(f"[TRANSFORM-BANK] Removed {before_rows - len(df)} rows with negative amounts")

    # Aggregates
    bank_agg_df = (
        df.groupby(["bank_id", "transaction_date"], as_index=False)
          .agg(
              total_amount=("transaction_amount", "sum"),
              num_transactions=("transaction_id", "count")
          )
    )
    bank_agg_df = bank_agg_df.rename(columns={"transaction_date": "agg_date"})

    elapsed_time = time.time() - start_time
    logger.info(f"[TRANSFORM-BANK] Completed bank transformation in {elapsed_time:.2f} seconds")
    return bank_agg_df


def transform_customer_txns(df: pd.DataFrame, logger) -> pd.DataFrame:
    """
    Transform raw transactions DataFrame into customer daily aggregates.
    Returns a DataFrame with (customer_id, agg_date, total_amount, num_transactions).
    If customer_id is not in df, returns empty DataFrame.
    """
    start_time = time.time()
    logger.info("[TRANSFORM-CUSTOMER] Starting customer transformation stage")

    if "customer_id" not in df.columns:
        logger.warning("[TRANSFORM-CUSTOMER] 'customer_id' column not present — skipping customer aggregation")
        return pd.DataFrame(columns=["customer_id", "agg_date", "total_amount", "num_transactions"])

    # Convert type
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce", downcast="integer")
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")
    df["transaction_amount"] = pd.to_numeric(df["transaction_amount"], errors="coerce")

    # Drop nulls in key fields
    before_rows = len(df)
    df = df.dropna(subset=["transaction_date", "transaction_amount", "customer_id"])
    logger.info(f"[TRANSFORM-CUSTOMER] Dropped {before_rows - len(df)} rows due to nulls in key fields")

    # Remove negative amounts
    before_rows = len(df)
    df = df[df["transaction_amount"] >= 0]
    logger.info(f"[TRANSFORM-CUSTOMER] Removed {before_rows - len(df)} rows with negative amounts")

    # Aggregate
    customer_agg_df = (
        df.groupby(["customer_id", "transaction_date"], as_index=False)
          .agg(
              total_amount=("transaction_amount", "sum"),
              num_transactions=("transaction_id", "count")
          )
    )
    customer_agg_df = customer_agg_df.rename(columns={"transaction_date": "agg_date"})

    elapsed_time = time.time() - start_time
    logger.info(f"[TRANSFORM-CUSTOMER] Completed customer transformation in {elapsed_time:.2f} seconds")
    return customer_agg_df


def transform_transactions(df: pd.DataFrame, logger) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Orchestrates both bank and customer transformations.
    """
    bank_df = transform_bank_txns(df, logger)
    customer_df = transform_customer_txns(df, logger)
    return bank_df, customer_df
