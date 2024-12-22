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


def get_past_due_orders(df):
    """Find orders where planning date is past due and status is in specified range."""
    try:
        # Get raw folder path
        raw_path = Path(__file__).parent / "raw"

        # Get list of Excel files with their modification times
        excel_files = [(f, f.stat().st_mtime) for f in raw_path.glob("*.xls*")]

        # Sort by modification time and get the latest file
        latest_file = sorted(excel_files, key=lambda x: x[1], reverse=True)[0][0]
        logging.info(f"Processing latest file: {latest_file}")

        # Read the latest file
        latest_df = pd.read_excel(latest_file, sheet_name="Sheet2")

        # Convert column names to lowercase
        latest_df.columns = latest_df.columns.str.lower().str.replace(" ", "_")

        # Add source file column
        latest_df["source_file"] = latest_file.name

        # Convert planning_date back to datetime if it's not already
        latest_df["planning_date"] = pd.to_datetime(latest_df["planning_date"])

        # Get current date
        current_date = pd.Timestamp.now().normalize()

        # Filter past due orders with specific status codes
        past_due = latest_df[
            (latest_df["planning_date"] < current_date)
            & (latest_df["po_line_low_sts"].isin([20, 35, 40, 50]))
        ][
            [
                "po_number",
                "planning_date",
                "po_line_low_sts",
                "buyer",
                "item_number",
                "source_file",
            ]
        ]

        # Calculate days past due
        past_due["days_past_due"] = (current_date - past_due["planning_date"]).dt.days

        # Sort by days past due (most overdue first)
        past_due = past_due.sort_values("days_past_due", ascending=False)

        # Export to CSV
        output_path = Path(__file__).parent / "output"
        past_due.to_csv(output_path / "past_due_orders.csv", index=False)

        logging.info(
            f"Found {len(past_due)} past due orders with status codes 20, 35, 40, and 50"
        )
        return past_due

    except Exception as e:
        logging.error(f"Error processing past due orders: {e}")
        raise


if __name__ == "__main__":
    try:
        combined_data = combine_excel_files()
        past_due = get_past_due_orders(combined_data)
        print("\nSample of past due orders:")
        print(past_due.head())
        logging.info("Process completed successfully")
    except Exception as e:
        logging.error(f"Process failed: {e}")
