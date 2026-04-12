from pathlib import Path

import pandas as pd


def read_file(filepath: str) -> pd.DataFrame:
    # macOS creates AppleDouble files like "._sample.csv" when copying to Windows/OneDrive.
    # They are not real CSV data; skip them so Mac + Windows behave predictably.
    if Path(filepath).name.startswith("._"):
        print(f"Skipping macOS metadata file (not CSV data): {filepath}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(filepath, encoding="utf-8")
        return df
    except UnicodeDecodeError:
        df = pd.read_csv(filepath, encoding="latin-1")
        return df
    except Exception as e:
        print(f"Error reading file: {e}")
        return pd.DataFrame()

