import logging
import pandas as pd
import os
import sys
from src.utils.paths import Config
from transforms.reusable_mapping import standardize_column_values
from src.utils.saved_files import saved_files
from typing import Tuple

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

config = Config()
file_path = config.raw_file_path


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


# Task 01 [Fetch data]
# df = fetch_data(file_path)
# print(df.shape)


def col_mutation(col_name: str) -> str:

    try:
        # Convert to string and strip whitespace
        col = str(col_name).strip()

        # Replace spaces and special characters
        col = col.lower()
        col = col.replace(" ", "_")
        col = "".join(c if c.isalnum() or c == "_" else "_" for c in col)

        # Remove consecutive underscores
        col = "_".join(filter(None, col.split("_")))

        # Ensure valid Python identifier
        if not col.isidentifier():
            col = "col_" + col

        logging.debug(f"Standardized column name: {col_name} -> {col}")
        return col

    except Exception as e:
        logging.error(f"Error standardizing column name '{col_name}': {str(e)}")
        return str(col_name)  # Return original if error occurs


# Task 02 [Change Col_case]
# df = fetch_data(file_path)
# df = df.rename(col_mutation, axis=1)
# print(df.head())

# Special case


#

# Task 03 [remove Un_wanted Col]
df = fetch_data(file_path)
df = df.rename(col_mutation, axis=1)
df = df.drop(["status", "unnamed1"], axis=1)

# List all col's and check "Null"
df = fetch_data(file_path)


df = df.rename(col_mutation, axis=1)
df = df.drop(["status", "unnamed1"], axis=1)

print(len(df), "starting")


# Special
def remove_and_track_nulls(
    df: pd.DataFrame, id_col: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # Step 1: Get rows where id_col is null (to track)
    null_records = df[df[id_col].isnull()]

    # Step 2: Remove those rows from the original DataFrame
    cleaned_df = df[df[id_col].notnull()]

    # Step 3: Return both
    return cleaned_df, null_records


df, null_log = remove_and_track_nulls(df, id_col="amount")

print(len(null_log), "isNull_count")
print(len(df), "df")

# Task 04 [Change abbrevations]

# gender-col
df = standardize_column_values(
    df,
    column_name="gender",
    # value_map=config["gender_map"],
    value_map={"f": "Female", "m": "Male", "o": "Other"},
    label="Gender",
    clean_encoding=True,
    clean_whitespace=True,
    convert_dtype=None,
)

# marital_status-col

df = standardize_column_values(
    df,
    column_name="marital_status",
    # value_map=config["marital_status_map"],
    value_map={"0": "Single", "1": "Married"},
    label="Marital_Status",
    clean_encoding=True,
    clean_whitespace=True,
    convert_dtype=None,
    missing_values=None,
)
print(df.head(), df.shape)


# Task 05 [Dealing with duplicates]
logging.info("Handling duplicates")

#  So here we are dealing with exact replica of records so we are trying to identify them, remove them and re-check them for confirmation , if you got all 0's you won.
# Step 1: Identify all exact duplicate rows (including first occurrence)
complete_duplicates = df[df.duplicated(keep=False)]
print(
    f"Total exact duplicate rows (including first occurrence): {len(complete_duplicates)}"
)

# Step 2: Identify duplicate rows excluding the first occurrence and save them as duplicates
extra_duplicates = df[df.duplicated()]
print(
    f"Duplicate rows excluding first occurrence (to be removed): {len(extra_duplicates)}"
)
duplicate_df = pd.DataFrame(extra_duplicates)
if not duplicate_df.empty:
    saved_files(duplicate_df, folder="data/outputs", file_name="duplicates.csv")

# Step 3: Drop extra duplicates and keep only the first occurrence
df_cleaned = df.drop(extra_duplicates.index)
print(df_cleaned.shape, "hey")
# Step 4: Confirm cleanup by checking row count before and after
print(f"Original row count: {df.shape[0]}")
print(f"Row count after removing duplicates: {df_cleaned.shape[0]}")

# Step 5: Re-check for any remaining exact duplicates in cleaned data
remaining_dupes_all = df_cleaned[df_cleaned.duplicated(keep=False)]
print(f"Remaining exact duplicate rows after cleanup: {len(remaining_dupes_all)}")

# Step 6: Re-check for duplicates excluding first occurrence in cleaned data
remaining_dupes_excl_first = df_cleaned[df_cleaned.duplicated()]
print(
    f"Remaining duplicates excluding first occurrence: {len(remaining_dupes_excl_first)}"
)


# Task 06 [Save the file]

# In the duplicate handling final section:
if len(remaining_dupes_excl_first) == 0:
    logging.info("No duplicates remaining - saving cleaned data")
    saved_files(
        df_cleaned, folder="data/processed", file_name="cleaned_data.csv"
    )  # Save df_cleaned instead of df
else:
    logging.warning(
        f"Still found {len(remaining_dupes_excl_first)} duplicates - not saving"
    )
