from utils import extract_monthly_data
from datetime import datetime
from pathlib import Path
import os

games_data_path = Path.cwd() / 'games_data' / 'chesscom'
CHESSCOM_USER = os.getenv('CHESSCOM_USER')

# Get Year and Month for Extract
year = datetime.now().year
# Get Last month's data on 1st of the current month
month = datetime.now().month - 1 

print(f"Extracting data for {year}-{month}")
monthly_data = extract_monthly_data(year, month, CHESSCOM_USER)
print(f"Saving data for {year}-{month}")
monthly_data.write_ndjson(games_data_path / f"{year}_{str(month).zfill(2)}.ndjson")
print(f"Data saved for {year}-{month}")