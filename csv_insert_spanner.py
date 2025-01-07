import csv
from itertools import islice
import google.cloud.spanner as spanner
from google.cloud.spanner_v1 import param_types
import pandas as pd

def csv_insert_in_spanner(csv_file, table_name):
    instance_id = "xxxxx"
    database_id = "xxxxx"
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    # Read CSV using pandas for easier handling
    df = pd.read_csv(csv_file)

    # Convert DataFrame to a list of lists for Spanner insertion
    data_to_insert = df.values.tolist()

    # Define column names and types based on your table schema
    column_names = df.columns.tolist()
    column_types = [param_types.STRING]

    batch_size = 5000  # Adjust batch size as needed
    
    with open(csv_file, mode='r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        column_names = reader.fieldnames

        while True:
            batch_data = list(islice(reader, batch_size))
            if not batch_data:
                break

            # Convert DictReader rows to tuples for Spanner insertion
            values = [tuple(row[col] for col in column_names) for row in batch_data]

            with database.batch() as batch:
                batch.insert(
                    table=table_name,
                    columns=column_names,
                    values=values,
                )
# Ejemplo de uso:
# import_csv_to_spanner("mi-instancia", "mi-base-de-datos", "mi-tabla", "datos.csv")
# Example usage:
csv_file = 'ba_graph.csv'
table_name = 'ba_graph'
csv_insert_in_spanner(csv_file, table_name)
