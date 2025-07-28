import csv
from itertools import islice
from datetime import datetime
import google.cloud.spanner as spanner

def csv_insert_in_spanner(csv_file, table_name, instance_id, database_id):
    """
    Reads data from a CSV file in batches, converts multiple timestamp formats,
    and inserts the data into a Spanner table.
    """
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    # --- Configuration for Timestamp Conversion ---
    # A list of column names in your CSV that contain timestamps.
    timestamp_column_names = ['col1', 'col2','col3','col4']
    # The format of the timestamps as they appear in the CSV file.
    original_timestamp_format = "%d/%m/%Y %H:%M:%S"
    
    batch_size = 5000  # Adjust batch size as needed
    
    with open(csv_file, mode='r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        column_names = reader.fieldnames
        if not column_names:
            print("Error: CSV file has no header or is empty.")
            return
            
        print(f"CSV Columns detected: {column_names}")
        
        # Verify all specified timestamp columns exist in the CSV
        for col_name in timestamp_column_names:
            if col_name not in column_names:
                print(f"Error: The specified timestamp column '{col_name}' was not found in the CSV header.")
                return

        total_rows_inserted = 0
        while True:
            batch_data = list(islice(reader, batch_size))
            if not batch_data:
                break

            # --- MODIFICATION START: Process batch to convert all specified timestamps ---
            processed_batch_data = []
            for row in batch_data:
                # Iterate over the list of timestamp columns to convert each one
                for timestamp_column_name in timestamp_column_names:
                    original_timestamp = row.get(timestamp_column_name)

                    # Check if the timestamp value exists and is not empty
                    if original_timestamp:
                        try:
                            # 1. Parse the original timestamp string into a datetime object
                            dt_object = datetime.strptime(original_timestamp, original_timestamp_format)
                            # 2. Format the datetime object into the RFC 3339 string Spanner needs
                            row[timestamp_column_name] = dt_object.strftime("%Y-%m-%dT%H:%M:%SZ")
                        except ValueError:
                            # If parsing fails, set to None to insert a NULL value in Spanner
                            print(f"Warning: Could not parse date '{original_timestamp}' in column '{timestamp_column_name}'. It will be inserted as NULL.")
                            row[timestamp_column_name] = None
                    else:
                        # Handle cases where the timestamp cell is empty
                        row[timestamp_column_name] = None
                
                processed_batch_data.append(row)
            # --- MODIFICATION END ---

            # Convert processed rows to tuples for Spanner insertion
            values = [tuple(row.get(col) for col in column_names) for row in processed_batch_data]

            try:
                with database.batch() as batch:
                    batch.insert(
                        table=table_name,
                        columns=column_names,
                        values=values,
                    )
                total_rows_inserted += len(values)
                print(f"Successfully inserted batch of {len(values)} rows. Total inserted: {total_rows_inserted}")
            except Exception as e:
                print(f"An error occurred during batch insertion: {e}")
                # Optional: Print the first problematic row for debugging
                if values:
                    print(f"First row in failed batch: {values[0]}")
                break # Stop processing on error

    print("Data insertion complete.")

if __name__ == "__main__":
    # --- Configuration ---
    INSTANCE_ID = "inst"
    DATABASE_ID = "db"
    CSV_FILE_PATH = 'table.CSV'
    TABLE_NAME = 'table'
    csv_insert_in_spanner(CSV_FILE_PATH, TABLE_NAME, INSTANCE_ID, DATABASE_ID)
