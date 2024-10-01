from utils import extract_monthly_data
import os
from pathlib import Path

year_strt = 2020
year_end = 2025
month_strt = 1
month_end = 13

CHESSCOM_USER = os.getenv('CHESSCOM_USER')

games_data_path = Path.cwd() / 'games_data' / 'chesscom'

for year in range(year_strt, year_end):
    for month in range(month_strt, month_end):
        
        if year == 2020 and month < 12:
            continue
        if year == 2024 and month > 8:
            continue
        
print(f"Extracting data for {year}-{month}")
monthly_data = extract_monthly_data(year, month, CHESSCOM_USER)
print(f"Saving data for {year}-{month}")
monthly_data.write_ndjson(games_data_path / f"{year}_{str(month).zfill(2)}.ndjson")
print(f"Data saved for {year}-{month}")