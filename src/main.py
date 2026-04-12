import logging
import os
import json
from pathlib import Path
from typing import Any, Dict, Union

import pandas as pd

from database import upsert_data
from loader import read_file
from reconciler import detect_fraud


# -----------------------------
# PROJECT PATH CONFIG
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = "config.json"


def _resolve_project_path(path: Union[str, Path]) -> Path:
    """Resolve relative paths against project root."""
    p = Path(path)
    return p if p.is_absolute() else (PROJECT_ROOT / p).resolve()


# -----------------------------
# DATA CLEANING
# -----------------------------
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Basic data cleaning and normalization."""
    df = df.dropna(subset=["transaction_id", "amount"])
    df = df.sort_values("transaction_date")
    df = df.drop_duplicates(subset=["transaction_id"], keep="last")

    df["category"] = df["category"].astype(str).str.strip().str.lower()
    df["currency"] = df["currency"].astype(str).str.upper()
    df["is_reversal"] = df["amount"] < 0

    return df


def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Light-weight preprocessing for transaction DataFrame.

    Purpose:
      - Keep preprocessing minimal and deterministic so downstream
        detection and reporting see original data as much as possible.
      - Operations:
        * remove duplicate transaction_id rows (keep last)
        * sort by transaction_date
        * normalize category (strip, lower) and currency (upper)
        * create is_reversal only if missing (amount < 0)

    Returns:
      Preprocessed DataFrame (a shallow copy of required transformations).
    """
    # Drop exact duplicate transaction_id keeping the last occurrence
    if "transaction_id" in df.columns:
        df = df.drop_duplicates(subset=["transaction_id"], keep="last")
    # Ensure ordering by transaction_date if column exists
    if "transaction_date" in df.columns:
        try:
            df = df.sort_values("transaction_date")
        except Exception:
            # If sorting fails, keep original order but log
            logging.warning("Sorting by transaction_date failed; preserving original order.")
    # Standardize category and currency columns when present
    if "category" in df.columns:
        df["category"] = df["category"].astype(str).str.strip().str.lower()
    if "currency" in df.columns:
        df["currency"] = df["currency"].astype(str).str.upper()

    # Create is_reversal only if missing
    if "is_reversal" not in df.columns and "amount" in df.columns:
        df["is_reversal"] = df["amount"] < 0

    return df


# -----------------------------
# CONFIG LOADER
# -----------------------------
def _load_config(path: Path) -> Dict[str, Any]:
    """Load JSON configuration file."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# -----------------------------
# SCHEMA NORMALIZATION
# -----------------------------
def normalize_schema(df: pd.DataFrame, *, defaults: Dict[str, Any]) -> pd.DataFrame:
    """Ensure consistent schema across datasets."""

    if "bank_txn_id" in df.columns and "transaction_id" not in df.columns:
        df = df.rename(columns={"bank_txn_id": "transaction_id"})

    if "account_id" not in df.columns:
        if "reference_id" in df.columns:
            df["account_id"] = df["reference_id"].fillna(
                defaults.get("unknown_account_id", "unknown")
            ).astype(str)
        else:
            df["account_id"] = defaults.get("unknown_account_id", "unknown")

    if "category" not in df.columns:
        if "transaction_type" in df.columns:
            df["category"] = df["transaction_type"]
        elif "description" in df.columns:
            df["category"] = df["description"]
        else:
            df["category"] = defaults.get("unknown_category", "unknown")

    if "currency" not in df.columns:
        df["currency"] = defaults.get("currency", "INR")

    required = [
        "transaction_id",
        "amount",
        "account_id",
        "transaction_date",
        "category",
        "currency",
    ]

    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns after normalization: {missing}")

    return df[required].copy()


# -----------------------------
# INSIGHTS GENERATION
# -----------------------------
def generate_insights(df, fraud_count, fraud_rate, threshold):
    """
    Produce concise business-focused insights from a transactions DataFrame.

    Parameters:
      - df: transactions DataFrame (expected columns: amount, transaction_date, account_id optional)
      - fraud_count: number of flagged fraud events
      - fraud_rate: fraud percentage (0-100)
      - threshold: high-value threshold used for highlighting large transactions

    Returns:
      List of 3 concise, analyst-style insight strings.
    """
    insights = []

    # High-value transaction insight
    high_value_txns = df[df["amount"] > threshold] if "amount" in df.columns else pd.DataFrame()
    if not high_value_txns.empty:
        proportion = len(high_value_txns) / len(df) * 100 if len(df) > 0 else 0.0
        total_high_value = float(high_value_txns["amount"].abs().sum())
        insights.append(
            f"High-value concentration: {len(high_value_txns)} transactions (≈{proportion:.1f}% of volume) exceed the {threshold} threshold and represent "
            f"a material portion of transacted value (≈{total_high_value:,.2f}); prioritize controls on large-ticket flows."
        )

    # Fraud summary insight
    if fraud_count > 0:
        insights.append(
            f"Fraud summary: {fraud_count} flagged events ({fraud_rate:.2f}% of transactions). "
            "Consider rapid triage and account-level prioritization for repeat offenders."
        )
    else:
        insights.append(
            "No flagged fraud events in the dataset; maintain monitoring thresholds and ensure flags are consistently applied."
        )

    # Business-focused behavioral insight (revised)
    insights.append(
        "Transaction behavior indicates that high-value and high-frequency activity are key contributors to fraud risk."
    )

    # Ensure 3 concise insights
    if len(insights) > 3:
        insights = insights[:3]
    return insights


# -----------------------------
# RECOMMENDATIONS
# -----------------------------
def generate_recommendations(fraud_rate, high_value_exists):
    recommendations = []

    if fraud_rate > 1:
        recommendations.append("Strengthen monitoring for high-risk transactions.")

    if high_value_exists:
        recommendations.append(
            "Introduce manual review for high-value transactions."
        )

    recommendations.append(
        "Improve detection accuracy using behavioral and temporal signals."
    )

    return recommendations


# -----------------------------
# MAIN PIPELINE
# -----------------------------
def main() -> None:
    config_env = os.environ.get("PIPELINE_CONFIG", DEFAULT_CONFIG_PATH)
    config_path = _resolve_project_path(config_env)
    config = _load_config(config_path)

    outputs = config.get("outputs", {})
    fraud_rules = config.get("fraud_rules", {})
    defaults = config.get("defaults", {})

    # Logging setup
    log_file = _resolve_project_path(outputs.get("log_file", "logs/pipeline.log"))
    log_file.parent.mkdir(parents=True, exist_ok=True)

    _resolve_project_path("reports").mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        filename=str(log_file),
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    logging.info("Pipeline started")

    # Output paths
    db_path = _resolve_project_path(outputs.get("transactions_db", "data/transactions.db"))
    fraud_report_csv = _resolve_project_path(outputs.get("fraud_report_csv", "reports/fraud_report.csv"))
    summary_json = _resolve_project_path(outputs.get("summary_json", "reports/fraud_summary.json"))

    # -----------------------------
    # PROCESS FILES
    # -----------------------------
    for file in config.get("input_files", []):
        input_path = _resolve_project_path(file)
        logging.info(f"Processing file: {input_path}")

        df = read_file(str(input_path))

        if df.empty:
            logging.error(f"No data loaded from {input_path}. Skipping.")
            continue

        try:
            df = normalize_schema(df, defaults=defaults)
        except Exception as e:
            logging.error(f"Schema normalization failed: {e}")
            continue

        df = clean_data(df)
        logging.info(f"After cleaning: {len(df)} rows")

        # Fraud detection
        df = detect_fraud(
            df,
            high_amount_threshold=fraud_rules.get("high_amount_threshold", 10000),
            high_amount_weight=fraud_rules.get("high_amount_weight", 0.6),
            velocity_threshold=fraud_rules.get("velocity_threshold", 5),
            velocity_weight=fraud_rules.get("velocity_weight", 0.4),
            fraud_threshold=fraud_rules.get("fraud_threshold", 0.7),
        )

        fraud_count = int(df["is_fraud"].sum())
        total_transactions = len(df)
        fraud_rate = (fraud_count / total_transactions) * 100 if total_transactions else 0

        # Warnings
        if fraud_rate > 10:
            print("⚠️ High fraud rate detected — review thresholds.")
        elif fraud_rate < 0.1:
            print("⚠️ Very low fraud detection — possible under-detection.")

        # Store results
        upsert_data(df, db_path=str(db_path))
        df.to_csv(fraud_report_csv, index=False)

        summary = {
            "input_file": str(input_path),
            "rows_processed": total_transactions,
            "fraud_detected": fraud_count,
            "fraud_rate_pct": fraud_rate,
        }

        with open(summary_json, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

        # Insights
        insights = generate_insights(
            df,
            fraud_count,
            fraud_rate,
            fraud_rules.get("high_amount_threshold", 10000),
        )

        recommendations = generate_recommendations(
            fraud_rate,
            not df[df["amount"] > fraud_rules.get("high_amount_threshold", 10000)].empty,
        )

        # Final Output
        print("\n===== FINANCIAL SUMMARY =====\n")
        print(f"Total Transactions: {total_transactions}")
        print(f"Fraudulent Transactions: {fraud_count} ({fraud_rate:.2f}%)")

        print("\n===== KEY INSIGHTS =====")
        for i in insights:
            print(f"- {i}")

        print("\n===== RECOMMENDATIONS =====")
        for r in recommendations:
            print(f"- {r}")

        logging.info(f"Finished processing: {input_path}")

    logging.info("Pipeline completed successfully")


if __name__ == "__main__":
    main()