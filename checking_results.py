import logging
import pandas as pd
import os
import sys
from src.utils.paths import Config
from transforms.reusable_mapping import standardize_column_values
from src.utils.saved_files import saved_files

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

config = Config()
file_path = "data/processed/cleaned_data.csv"


def read(file_path: str) -> pd.DataFrame:

    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        if not file_path.lower().endswith((".csv")):
            raise ValueError("File must be CSV format (.csv )")

        df = pd.read_csv(file_path, engine="python", encoding="unicode_escape")

        logging.info(f"Successfully loaded data from {file_path}")
        return df

    except Exception as e:
        logging.error(f"Error reading file {file_path}: {str(e)}")
        raise


def fetch_data(file_path: str) -> pd.DataFrame:

    df = read(file_path)

    # Basic data validation
    if df.empty:
        logging.warning("Loaded DataFrame is empty")
    else:
        logging.info(f"Loaded DataFrame shape: {df.shape}")

    return df


df = fetch_data(file_path)

extra_duplicates = df[df.duplicated()]
print(
    f"Duplicate rows excluding first occurrence (to be removed): {len(extra_duplicates)}"
)
