from typing import List
import pandas as pd
import numpy as np

def calculate_kpis(total_transactions, matched_transactions, unmatched_transactions, fraud_transactions):
    """
    Calculate basic reconciliation KPIs.

    Returns a dict with:
      - total_transactions
      - matched_transactions
      - unmatched_transactions
      - reconciliation_rate (as percentage string with 2 decimals, e.g. "95.00%")
      - fraud_count
      - fraud_rate (as percentage string with 2 decimals)
    """
    # Coerce inputs to numeric where reasonable
    try:
        total = float(total_transactions)
    except (TypeError, ValueError):
        total = 0.0

    try:
        matched = float(matched_transactions)
    except (TypeError, ValueError):
        matched = 0.0

    try:
        unmatched = float(unmatched_transactions)
    except (TypeError, ValueError):
        unmatched = 0.0

    try:
        fraud = float(fraud_transactions)
    except (TypeError, ValueError):
        fraud = 0.0

    if total > 0:
        reconciliation_rate = (matched / total) * 100.0
        fraud_rate = (fraud / total) * 100.0
    else:
        reconciliation_rate = 0.0
        fraud_rate = 0.0

    return {
        "total_transactions": int(total),
        "matched_transactions": int(matched),
        "unmatched_transactions": int(unmatched),
        "reconciliation_rate": f"{reconciliation_rate:.2f}%",
        "fraud_count": int(fraud),
        "fraud_rate": f"{fraud_rate:.2f}%"
    }

def generate_insights(df: pd.DataFrame) -> List[str]:
    """
    Generate 3–5 concise business insights from a transactions DataFrame.

    Expected (best-effort) columns: amount, timestamp / transaction_date, fraud_flag / is_fraud.
    Returns: list[str] of professional, analyst-style insight sentences.
    """
    insights: List[str] = []

    if df is None or len(df) == 0:
        return ["No transactions available to generate insights."]

    total_tx = len(df)
    # Normalize amount column presence
    if "amount" not in df.columns:
        return ["Data does not contain an 'amount' column; cannot compute monetary insights."]

    # Use absolute amounts for concentration analysis
    amounts = df["amount"].abs().dropna()
    total_value = float(amounts.sum()) if not amounts.empty else 0.0

    # 1) High-value concentration (top 5% by default)
    try:
        p95 = float(amounts.quantile(0.95))
        top_mask = amounts > p95
        top_count = int(top_mask.sum())
        top_value = float(amounts[top_mask].sum()) if top_count > 0 else 0.0
        top_value_pct = (top_value / total_value * 100.0) if total_value > 0 else 0.0

        insights.append(
            f"Value concentration: the top 5% of transactions (amounts > {p95:,.2f}) account for "
            f"{top_value_pct:.1f}% of total transacted value, representing {top_count} of {total_tx} transactions. "
            "This indicates a material concentration of value in a small subset of activity and merits targeted review of large-ticket flows."
        )
    except Exception:
        insights.append("Value concentration could not be computed due to insufficient or malformed amount data.")

    # 2) Fraud concentration (if fraud flag exists)
    fraud_cols = ["fraud_flag", "is_fraud", "fraud", "isFraud"]
    fraud_col = next((c for c in fraud_cols if c in df.columns), None)
    if fraud_col is None:
        insights.append("No explicit fraud flag column detected; consider adding a binary fraud indicator for operational monitoring.")
    else:
        try:
            flags = df[fraud_col].fillna(0)
            flags_bool = flags.astype(bool)
            fraud_count = int(flags_bool.sum())
            fraud_rate_pct = (fraud_count / total_tx * 100.0) if total_tx > 0 else 0.0

            if fraud_count == 0:
                insights.append("No flagged fraud events were detected in the dataset.")
            else:
                # Check concentration by account if possible
                account_col = next((c for c in ["account_id", "reference_id", "account"] if c in df.columns), None)
                if account_col:
                    top_account = (
                        df.loc[flags_bool]
                        .groupby(account_col)
                        .size()
                        .nlargest(1)
                    )
                    if not top_account.empty:
                        acc_id = str(top_account.index[0])
                        acc_count = int(top_account.iloc[0])
                        acc_pct = (acc_count / fraud_count * 100.0) if fraud_count > 0 else 0.0
                        insights.append(
                            f"Fraud concentration: {fraud_count} flagged events ({fraud_rate_pct:.2f}% of transactions). "
                            f"The largest share is concentrated in account '{acc_id}' with {acc_count} flagged transactions "
                            f"({acc_pct:.1f}% of all flagged events) — prioritize account-level investigation."
                        )
                    else:
                        insights.append(
                            f"Fraud summary: {fraud_count} flagged events ({fraud_rate_pct:.2f}% of transactions). "
                            "No single account dominates flagged activity."
                        )
                else:
                    insights.append(
                        f"Fraud summary: {fraud_count} flagged events ({fraud_rate_pct:.2f}% of transactions). "
                        "Consider enriching records with account identifiers to localize remediation."
                    )
        except Exception:
            insights.append("Fraud concentration analysis failed due to unexpected flag column formatting.")

    # 3) Frequency / temporal anomalies
    time_cols = ["timestamp", "transaction_date", "posting_date", "date"]
    time_col = next((c for c in time_cols if c in df.columns), None)
    if time_col is None:
        insights.append("No timestamp-like column found; temporal frequency analysis could not be performed.")
    else:
        try:
            times = pd.to_datetime(df[time_col], errors="coerce")
            valid_times = times.dropna()
            if len(valid_times) < 2:
                insights.append("Insufficient timestamp data to assess transaction frequency patterns.")
            else:
                span_days = (valid_times.max() - valid_times.min()).days + 1
                # Choose aggregation granularity
                if span_days >= 7:
                    counts = valid_times.dt.date.value_counts().sort_values(ascending=False)
                    bucket_label = "day"
                else:
                    counts = valid_times.dt.hour.value_counts().sort_values(ascending=False)
                    bucket_label = "hour"

                top_bucket = counts.index[0]
                top_count = int(counts.iloc[0])
                mean_cnt = float(counts.mean())
                std_cnt = float(counts.std(ddof=0)) if len(counts) > 1 else 0.0
                z = (top_count - mean_cnt) / std_cnt if std_cnt > 0 else float("inf")

                if np.isfinite(z) and z >= 3:
                    insights.append(
                        f"Temporal anomaly: transactions peak in {bucket_label} '{top_bucket}' with {top_count} events, "
                        f"{z:.1f}σ above the mean activity. This spike is anomalous and should be validated for operational or fraudulent causes."
                    )
                else:
                    insights.append(
                        f"Frequency profile: busiest {bucket_label} is '{top_bucket}' with {top_count} transactions, "
                        f"which is within normal variation (mean {mean_cnt:.1f}, std {std_cnt:.1f})."
                    )
        except Exception:
            insights.append("Temporal frequency analysis failed due to timestamp parsing errors.")

    # 4) Optional: velocity indicator (rapid repeated activity by account)
    if "account_id" in df.columns and fraud_col is not None:
        try:
            velocity = df.groupby("account_id")["amount"].count()
            top_vel = velocity.nlargest(3)
            if not top_vel.empty:
                acc_examples = ", ".join(f"{idx} ({cnt})" for idx, cnt in top_vel.items())
                insights.append(
                    f"Velocity check: the top accounts by transaction count are {acc_examples}. "
                    "High-frequency accounts may represent batching, operational load, or targeted misuse; review flows for the top performers."
                )
        except Exception:
            # Non-fatal; ignore
            pass

    # Trim to 3–5 insights: prefer earlier (value, fraud, temporal) then velocity
    if len(insights) > 5:
        insights = insights[:5]
    elif len(insights) < 3:
        # pad with neutral statements if too few
        insights.extend(["No additional insights could be generated from the available data."] * (3 - len(insights)))

    return insights

def generate_recommendations(kpis: dict, insights: List[str] = None, df: pd.DataFrame = None) -> List[str]:
    """
    Produce 3-4 actionable recommendations based on KPI values and fraud/temporal patterns.

    Parameters
    - kpis: dictionary returned by calculate_kpis
    - insights: optional list of insight strings (from generate_insights)
    - df: optional transactions DataFrame for deriving thresholds (e.g. p95)

    Returns
    - List[str]: 3-4 professional, actionable recommendations.
    """
    recs: List[str] = []
    insights = insights or []

    # Safely parse KPI fields
    try:
        fraud_count = int(kpis.get("fraud_count", 0))
    except Exception:
        fraud_count = 0
    try:
        unmatched = int(kpis.get("unmatched_transactions", 0))
    except Exception:
        unmatched = 0
    try:
        total = int(kpis.get("total_transactions", 0))
    except Exception:
        total = 0
    # reconciliation rate may be formatted like "95.00%"
    rec_rate_raw = kpis.get("reconciliation_rate", "0%")
    try:
        reconciliation_rate = float(str(rec_rate_raw).strip().rstrip("%"))
    except Exception:
        reconciliation_rate = 0.0

    # 1) High-value transactions -> manual review / controls
    high_value_threshold = None
    if df is not None and "amount" in df.columns:
        try:
            high_value_threshold = float(df["amount"].abs().dropna().quantile(0.95))
        except Exception:
            high_value_threshold = None

    if any("Value concentration" in s or "top 5%" in s for s in insights) or (high_value_threshold is not None):
        if high_value_threshold is not None:
            recs.append(
                f"Manual review of high-value activity: institute a targeted review workflow for transactions above {high_value_threshold:,.2f} "
                "and apply enhanced approval/segregation controls for large-ticket flows."
            )
        else:
            recs.append(
                "Manual review of high-value activity: instantiate sampling and approval gates for large transactions and require documented rationale for exceptions."
            )

    # 2) Fraud-related recommendations
    if fraud_count > 0:
        recs.append(
            "Fraud remediation and monitoring: prioritize investigation of accounts with flagged events, deploy near‑real‑time alerts for rising fraud patterns, "
            "and implement blocking or throttling rules for accounts exhibiting repeated flagged behaviour."
        )
    else:
        # still recommend maintaining monitoring if no fraud found
        recs.append(
            "Maintain active fraud monitoring: implement automated alerts on anomaly thresholds and ensure fraud flags are consistently captured and surfaced in operational dashboards."
        )

    # 3) Frequency / velocity monitoring
    if any("Temporal anomaly" in s or "Velocity check" in s or "busiest" in s for s in insights):
        recs.append(
            "Behavioral monitoring: introduce rate‑based and session‑based rules to detect unusual frequency spikes (e.g. hourly/day thresholds), "
            "and instrument dashboards for ongoing surveillance of velocity anomalies."
        )

    # 4) Reconciliation process improvement (based on reconciliation KPIs)
    # Trigger when reconciliation rate is below practical thresholds or unmatched count is material.
    low_rec_threshold = 98.0
    unmatched_pct = (unmatched / total * 100.0) if total > 0 else 0.0
    if reconciliation_rate < low_rec_threshold or unmatched_pct > 1.0:
        recs.append(
            "Reconciliation process improvements: streamline matching logic, enrich records with reference/account identifiers, increase automation of exception handling, "
            "and define SLAs for clearing unmatched items to reduce operational backlog."
        )

def print_financial_summary(kpis: dict, insights: List[str], recommendations: List[str], width: int = 80) -> None:
    """
    Print a clean, business-style financial summary to stdout.

    Sections:
      - Financial Summary (KPI table)
      - Key Insights (numbered)
      - Recommendations (numbered)
    """
    sep = "=" * width
    minor_sep = "-" * width

    # Header
    print("\n" + sep)
    print("Financial Summary".center(width))
    print(sep + "\n")

    # KPI summary
    print("KPI Summary")
    print(minor_sep)
    # ordered display with friendly labels
    kpi_display = [
        ("total_transactions", "Total transactions"),
        ("matched_transactions", "Matched transactions"),
        ("unmatched_transactions", "Unmatched transactions"),
        ("reconciliation_rate", "Reconciliation rate"),
        ("fraud_count", "Fraud count"),
        ("fraud_rate", "Fraud rate"),
    ]
    # determine label padding
    label_width = 32
    for key, label in kpi_display:
        val = kpis.get(key, "")
        print(f"{label:<{label_width}} : {str(val)}")
    print()

    # Key insights
    print("Key Insights")
    print(minor_sep)
    if not insights:
        print("No insights available.")
    else:
        for i, ins in enumerate(insights, start=1):
            # wrap long lines minimally for readability
            print(f"{i}. {ins}")
    print()

    # Recommendations
    print("Recommendations")
    print(minor_sep)
    if not recommendations:
        print("No recommendations available.")
    else:
        for i, rec in enumerate(recommendations, start=1):
            print(f"{i}. {rec}")
    print("\n" + sep + "\n")


def create_and_print_report(kpis: dict = None, df: pd.DataFrame = None,
                            insights: List[str] = None, recommendations: List[str] = None) -> None:
    """
    Orchestrator: ensure insights/recommendations are present (derive them if missing)
    and print the final business report.

    Usage:
      - Provide kpis (dict) and optionally df to derive insights/recommendations.
      - If insights is None and df provided, generate_insights(df) will be used.
      - If recommendations is None, generate_recommendations(...) will be invoked.
    """
    if kpis is None:
        raise ValueError("kpis dict is required to produce the financial summary.")

    # Derive insights if missing and df provided
    if insights is None:
        if df is not None:
            try:
                insights = generate_insights(df)
            except Exception:
                insights = ["Failed to generate insights from provided data."]
        else:
            insights = []

    # Derive recommendations if missing
    if recommendations is None:
        try:
            recommendations = generate_recommendations(kpis, insights=insights, df=df)
        except Exception:
            recommendations = ["Failed to generate recommendations based on current KPIs and insights."]

    # Print the final report
    print_financial_summary(kpis, insights, recommendations)