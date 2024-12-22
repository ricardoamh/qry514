import pandas as pd
import os


def combine_excel_files():
    # Get current directory and raw subfolder path
    directory_path = os.path.dirname(os.path.abspath(__file__))
    raw_folder_path = os.path.join(directory_path, "raw")
    all_dataframes = []

    # Iterate through all files in the raw directory
    for filename in os.listdir(raw_folder_path):
        if filename.endswith((".xlsx", ".xls")):
            file_path = os.path.join(raw_folder_path, filename)

            # Special handling for qry514-fac400 file
            if "qry514-fac400" in filename:
                df = pd.read_excel(
                    file_path,
                    sheet_name="Sheet2",
                    dtype={"Supplier": str, "Item Number": str},
                )
                # Format supplier as 5-digit string with leading zeros
                df["Supplier"] = df["Supplier"].astype(str).str.zfill(5)
                # Preserve any leading zeros in item number without enforcing a specific length
                df["Item Number"] = df["Item Number"].astype(str)
            else:
                df = pd.read_excel(
                    file_path,
                    sheet_name="Sheet2",
                    dtype={"Supplier": str, "Item Number": str},
                )

            df["Source_File"] = filename
            all_dataframes.append(df)

    # Combine all dataframes
    combined_df = pd.concat(all_dataframes, ignore_index=True)

    # Rename columns: convert to lowercase and replace spaces with underscores
    combined_df.columns = combined_df.columns.str.lower().str.replace(" ", "_")

    # Rename 'orderedquantity' to 'ordered_quantity'
    combined_df = combined_df.rename(columns={"orderedquantity": "ordered_quantity"})

    # Find all columns with 'date' in the name
    date_columns = [col for col in combined_df.columns if "date" in col]
    print("Date columns found:", date_columns)  # For verification

    # Convert '--0' to NaN in conf_dely_date column
    combined_df["conf_dely_date"] = combined_df["conf_dely_date"].replace("--0", pd.NA)

    # Convert all date columns to datetime
    for col in date_columns:
        combined_df[col] = pd.to_datetime(combined_df[col], errors="coerce")

    # Impute conf_dely_date with po_requested_delivery_date where it's null
    combined_df["conf_dely_date"] = combined_df["conf_dely_date"].fillna(
        combined_df["po_requested_delivery_date"]
    )

    return combined_df


# Create the combined dataframe
combined_data = combine_excel_files()

# Convert text columns to string type
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
    combined_data[col] = combined_data[col].astype("string")

# Check for null values
# print("\nNull Values Count in Each Column:")
# print(combined_data.isnull().sum())
#
## Get percentage of null values
# print("\nPercentage of Null Values in Each Column:")
# print((combined_data.isnull().sum() / len(combined_data)) * 100)
#
## Optional: Display rows with any null values
# print("\nRows containing null values:")
# print(combined_data[combined_data.isnull().any(axis=1)])
#
# print(combined_data.dtypes)
# print(combined_data.describe())
# print(combined_data.info())

# Export the processed dataset to CSV for further analysis
print("\nExporting processed data to Excel file...")
combined_data.to_excel("combined_data.xlsx", index=False)
print("Data successfully exported to 'combined_data.xlsx'")

# Export the processed dataset to parquet for further analysis
print("\nExporting processed data to Parquet file...")
combined_data.to_parquet(
    "combined_data.parquet", index=False, engine="pyarrow", compression="snappy"
)
print("Data successfully exported to 'combined_data.parquet'")

# Export the processed dataset to CSV for further analysis
print("\nExporting processed data to CSV file...")
combined_data.to_csv(
    "combined_data.csv", index=False, encoding="utf-8", quoting=1  # QUOTE_ALL
)
print("Data successfully exported to 'combined_data.csv'")
