from google.cloud import bigquery
from datetime import datetime
import os
from load.chesscom.config import job_config

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getcwd() + r"\bigquery_service_account.json"

ENV = "dev"
PLATFORM = "chesscom"
PROJECT_ID = "chesscom-433717"
DATASET_ID = f"{ENV}_games_{PLATFORM}"
TABLE_NAME = "raw_games"

client = bigquery.Client()
table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
print(f"Table ID: {table_id}")

year = str(datetime.now().year)
month = str(datetime.now().month - 1).zfill(2)
        
print(f"Uploading {year}_{month}.ndjson")

file_path = os.getcwd() + r"\monthly_data\\" + f"{year}_{month}.ndjson"

with open(file_path, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id, job_config=job_config)

job.result()  # Waits for the job to complete

table = client.get_table(table_id)  # Make an API request.

print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)