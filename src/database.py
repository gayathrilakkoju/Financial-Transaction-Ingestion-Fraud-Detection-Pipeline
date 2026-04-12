import sqlite3
import hashlib
import os
from datetime import datetime

DB_NAME = "audit.db"
TRANSACTIONS_DB_NAME = "data/transactions.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT,
            role TEXT,
            action TEXT,
            timestamp TEXT
        )
    """)

    conn.commit()
    conn.close()


def log_action(username, role, action):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute("""
        INSERT INTO audit_logs (username, role, action, timestamp)
        VALUES (?, ?, ?, ?)
    """, (username, role, action, timestamp))

    conn.commit()
    conn.close()


def get_logs():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM audit_logs ORDER BY id DESC")
    rows = cursor.fetchall()

    conn.close()
    return rows


def compute_hash(row) -> str:
    # Compute a stable-ish md5 from the full row values for change detection.
    row_str = "".join(str(x) for x in row.values)
    return hashlib.md5(row_str.encode()).hexdigest()


def upsert_data(df, db_path: str = TRANSACTIONS_DB_NAME) -> None:
    # Ensure target directory exists before SQLite connection.
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id TEXT PRIMARY KEY,
            amount REAL,
            account_id TEXT,
            transaction_date TEXT,
            row_hash TEXT
        )
        """
    )

    for _, row in df.iterrows():
        row_hash = compute_hash(row)

        cursor.execute(
            "SELECT row_hash FROM transactions WHERE transaction_id=?",
            (row["transaction_id"],),
        )
        existing = cursor.fetchone()

        if not existing:
            cursor.execute(
                """
                INSERT INTO transactions (
                    transaction_id, amount, account_id, transaction_date, row_hash
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    row["transaction_id"],
                    row["amount"],
                    row["account_id"],
                    str(row["transaction_date"]),
                    row_hash,
                ),
            )
        elif existing[0] != row_hash:
            cursor.execute(
                """
                UPDATE transactions
                SET amount=?, account_id=?, transaction_date=?, row_hash=?
                WHERE transaction_id=?
                """,
                (
                    row["amount"],
                    row["account_id"],
                    str(row["transaction_date"]),
                    row_hash,
                    row["transaction_id"],
                ),
            )

    conn.commit()
    conn.close()