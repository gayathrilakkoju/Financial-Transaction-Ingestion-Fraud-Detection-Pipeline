import pandas as pd

def reconcile(bank_df, ledger_df):

    merged = bank_df.merge(
        ledger_df,
        on="transaction_id",
        how="outer",
        suffixes=("_bank", "_ledger"),
        indicator=True
    )

    # Amount Mismatch
    amount_mismatch = merged[
        (merged["_merge"] == "both") &
        (merged["amount_bank"] != merged["amount_ledger"])
    ].copy()

    amount_mismatch["difference"] = (
        amount_mismatch["amount_bank"] - amount_mismatch["amount_ledger"]
    ).abs()

    amount_mismatch["risk_level"] = amount_mismatch["difference"].apply(
        lambda x: "HIGH" if x > 50000 else "MEDIUM"
    )

    amount_mismatch["exception_type"] = "Amount Mismatch"

    # Missing in Ledger
    missing_in_ledger = merged[merged["_merge"] == "left_only"].copy()
    missing_in_ledger["exception_type"] = "Missing in Ledger"
    missing_in_ledger["risk_level"] = "HIGH"

    # Missing in Bank
    missing_in_bank = merged[merged["_merge"] == "right_only"].copy()
    missing_in_bank["exception_type"] = "Missing in Bank"
    missing_in_bank["risk_level"] = "HIGH"

    # Combine all exceptions
    exceptions = pd.concat([
        amount_mismatch,
        missing_in_ledger,
        missing_in_bank
    ])

    return merged, exceptions


def detect_fraud(
    df: pd.DataFrame,
    *,
    high_amount_threshold: float = 10000,
    high_amount_weight: float = 0.6,
    velocity_threshold: int = 5,
    velocity_weight: float = 0.4,
    fraud_threshold: float = 0.7,
) -> pd.DataFrame:
    df["fraud_score"] = 0.0
    df["fraud_reason"] = ""

    # Rule 1: High amount
    high_amount = df["amount"] > high_amount_threshold
    df.loc[high_amount, "fraud_score"] += high_amount_weight
    df.loc[high_amount, "fraud_reason"] += "high_amount,"

    # Rule 2: High frequency (velocity) per account
    velocity = df.groupby("account_id")["transaction_id"].transform("count")
    high_velocity = velocity > velocity_threshold
    df.loc[high_velocity, "fraud_score"] += velocity_weight
    df.loc[high_velocity, "fraud_reason"] += "high_velocity,"

    # Final flag
    df["is_fraud"] = (df["fraud_score"] >= fraud_threshold).astype(int)
    return df