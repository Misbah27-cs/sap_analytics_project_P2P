"""
SAP P2P Analytics - ETL Pipeline
====================================
Author  : S M Misbahul Haque
Roll No : 23052832
Branch  : CSE

Extract → Transform → Load pipeline for SAP MM Procure-to-Pay data.
"""

import pandas as pd
import sqlite3
import os
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH  = os.path.join(DATA_DIR, "p2p_analytics.db")


def extract(filepath: str) -> pd.DataFrame:
    """Read raw SAP MM P2P export CSV."""
    log.info(f"[EXTRACT] Reading: {filepath}")
    df = pd.read_csv(filepath)
    log.info(f"[EXTRACT] Loaded {len(df)} records, {len(df.columns)} columns")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and enrich P2P data."""
    log.info("[TRANSFORM] Starting transformation...")

    # Parse dates
    for col in ["PO_Date","GR_Date","Invoice_Date","Payment_Date"]:
        df[col] = pd.to_datetime(df[col])

    # Derived columns
    df["Month"]              = df["PO_Date"].dt.to_period("M").astype(str)
    df["Quarter"]            = df["PO_Date"].dt.to_period("Q").astype(str)
    df["Year"]               = df["PO_Date"].dt.year
    df["PO_to_GR_Days"]      = (df["GR_Date"]      - df["PO_Date"]).dt.days
    df["GR_to_Invoice_Days"] = (df["Invoice_Date"] - df["GR_Date"]).dt.days
    df["Invoice_to_Pay_Days"]= (df["Payment_Date"] - df["Invoice_Date"]).dt.days
    df["Total_Cycle_Days"]   = (df["Payment_Date"] - df["PO_Date"]).dt.days

    # Spend tier
    def spend_tier(val):
        if val >= 1_000_000: return "High Spend"
        elif val >= 300_000: return "Mid Spend"
        else:                return "Low Spend"

    df["Spend_Tier"] = df["PO_Value"].apply(spend_tier)

    # Tax rate
    df["Tax_Rate_Pct"] = round((df["Tax_Amount"] / df["PO_Value"]) * 100, 1)

    # Null check
    null_count = df.isnull().sum().sum()
    if null_count > 0:
        log.warning(f"[TRANSFORM] {null_count} nulls found — filling defaults")
        df.fillna({"PO_Status": "Unknown"}, inplace=True)

    df.columns = [c.strip().replace(" ","_") for c in df.columns]
    log.info(f"[TRANSFORM] Complete — {len(df)} records ready")
    return df


def load(df: pd.DataFrame, db_path: str) -> None:
    """Load into SQLite data warehouse."""
    log.info(f"[LOAD] Writing to: {db_path}")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        df.to_sql("fact_purchases", conn, if_exists="replace", index=False)
        log.info(f"[LOAD] fact_purchases → {len(df)} rows")

        dim_vendors = df[["Vendor_ID","Vendor_Name","Region"]].drop_duplicates()
        dim_vendors.to_sql("dim_vendors", conn, if_exists="replace", index=False)
        log.info(f"[LOAD] dim_vendors → {len(dim_vendors)} rows")

        dim_materials = df[["Material_ID","Material_Name","Category"]].drop_duplicates()
        dim_materials.to_sql("dim_materials", conn, if_exists="replace", index=False)
        log.info(f"[LOAD] dim_materials → {len(dim_materials)} rows")

        dim_buyers = df[["Buyer","Region"]].drop_duplicates()
        dim_buyers.to_sql("dim_buyers", conn, if_exists="replace", index=False)
        log.info(f"[LOAD] dim_buyers → {len(dim_buyers)} rows")

        monthly = (
            df.groupby("Month")
            .agg(Total_POs=("PO_Number","count"),
                 Total_PO_Value=("PO_Value","sum"),
                 Total_Invoice=("Invoice_Amount","sum"),
                 Total_Tax=("Tax_Amount","sum"),
                 Avg_Cycle_Days=("Total_Cycle_Days","mean"))
            .reset_index()
        )
        monthly.to_sql("agg_monthly_purchases", conn, if_exists="replace", index=False)
        log.info(f"[LOAD] agg_monthly_purchases → {len(monthly)} rows")

    log.info("[LOAD] All tables written successfully ✔")


def run_pipeline(source_file: str = None) -> pd.DataFrame:
    if source_file is None:
        source_file = os.path.join(DATA_DIR, "p2p_purchase_data.csv")

    log.info("=" * 60)
    log.info("   SAP P2P Analytics — ETL Pipeline Starting")
    log.info(f"   Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    raw   = extract(source_file)
    clean = transform(raw)
    load(clean, DB_PATH)

    log.info("=" * 60)
    log.info("   ETL Pipeline Completed Successfully ✔")
    log.info("=" * 60)
    return clean


if __name__ == "__main__":
    run_pipeline()
