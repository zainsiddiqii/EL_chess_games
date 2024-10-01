from google.cloud import bigquery
import os
from config import job_config

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getcwd() + r"\bigquery_service_account.json"

PROJECT_ID = "chessgames"
ENV = "dev"
PLATFORM = "lichess"
DATASET_ID = f"{ENV}_games"
TABLE_NAME = f"raw_games_{PLATFORM}"

client = bigquery.Client()
table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
print(f"Table ID: {table_id}")

for year in range(2020, 2025):
    for month in range(1, 13):
        
        year = str(year)
        month = str(month).zfill(2)
                
        print(f"Uploading {year}_{month}.ndjson")

        file_path = os.getcwd() + r"\games_data\\" +f"{PLATFORM}\\" + f"{year}_{month}.ndjson"

        try:
            with open(file_path, "rb") as source_file:
                job = client.load_table_from_file(source_file, table_id, job_config=job_config)
        except FileNotFoundError:
            print(f"File {year}_{month}.ndjson not found")
            continue

        job.result()  # Waits for the job to complete

        table = client.get_table(table_id)  # Make an API request.

        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )