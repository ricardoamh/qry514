import pandas as pd
import os
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def setup_file_paths():
    """Set up and validate directory paths."""
    try:
        directory_path = Path(__file__).parent
        raw_folder_path = directory_path / "raw"
        output_folder_path = directory_path / "output"

        # Create output directory if it doesn't exist
        output_folder_path.mkdir(exist_ok=True)

        return raw_folder_path, output_folder_path
    except Exception as e:
        logging.error(f"Error setting up directories: {e}")
        raise


def read_excel_file(file_path):
    """Read a single Excel file and return a dataframe."""
    try:
        df = pd.read_excel(file_path, sheet_name="Sheet2")
        df["source_file"] = file_path.name
        return df
    except Exception as e:
        logging.error(f"Error reading file {file_path}: {e}")
        return None


def process_dates(df):
    """Process all date columns in the dataframe."""
    try:
        # Find date columns
        date_columns = [col for col in df.columns if "date" in col]
        logging.info(f"Processing date columns: {date_columns}")

        # Handle special case for conf_dely_date
        df["conf_dely_date"] = df["conf_dely_date"].replace("--0", pd.NA)

        # Convert to datetime
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

        # Impute conf_dely_date
        df["conf_dely_date"] = df["conf_dely_date"].fillna(
            df["po_requested_delivery_date"]
        )

        return df
    except Exception as e:
        logging.error(f"Error processing dates: {e}")
        raise


def process_text_columns(df):
    """Convert specified columns to string type."""
    text_columns = [
        "supplier",
        "supplier_name",
        "item_number",
        "item_type",
        "po_number",
        "buyer",
        "source_file",
    ]
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].astype("string")
    return df


def export_data(df, output_path, filename):
    """Export dataframe to multiple formats."""
    try:
        base_path = output_path / filename

        # Export to different formats
        df.to_excel(f"{base_path}.xlsx", index=False)
        df.to_parquet(f"{base_path}.parquet", index=False)
        df.to_csv(f"{base_path}.csv", index=False)

        logging.info(f"Data exported successfully to {output_path}")
    except Exception as e:
        logging.error(f"Error exporting data: {e}")
        raise


def analyze_data_quality(df):
    """Analyze and log data quality metrics."""
    logging.info("\n=== Data Quality Report ===")
    logging.info(f"Total rows: {len(df)}")
    logging.info("\nNull Values Count:")
    logging.info(df.isnull().sum())
    logging.info("\nData Types:")
    logging.info(df.dtypes)
    return df


def combine_excel_files():
    """Main function to combine and process Excel files."""
    try:
        raw_path, output_path = setup_file_paths()
        all_dataframes = []

        # Read all Excel files
        for file_path in raw_path.glob("*.xls*"):
            df = read_excel_file(file_path)
            if df is not None:
                all_dataframes.append(df)

        if not all_dataframes:
            raise ValueError("No valid Excel files found")

        # Combine dataframes
        combined_df = pd.concat(all_dataframes, ignore_index=True)

        # Process the combined dataframe
        combined_df.columns = combined_df.columns.str.lower().str.replace(" ", "_")
        combined_df = combined_df.rename(
            columns={"orderedquantity": "ordered_quantity"}
        )
        combined_df = process_dates(combined_df)
        combined_df = process_text_columns(combined_df)
        combined_df = analyze_data_quality(combined_df)

        # Export the results
        export_data(combined_df, output_path, "combined_data")

        return combined_df

    except Exception as e:
        logging.error(f"Error in main process: {e}")
        raise


if __name__ == "__main__":
    try:
        combined_data = combine_excel_files()
        logging.info("Process completed successfully")
    except Exception as e:
        logging.error(f"Process failed: {e}")
