from utils import get_games, extract_game_data
from pathlib import Path
import polars as pl
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta

games_data_path = Path.cwd() / 'games_data' / 'lichess'
LICHESS_USER = os.getenv('LICHESS_USER')

# Create a datetime object for August 31, 2024, at 23:59:59
now = datetime.now()
previous_month = now.month - 1
current_year = now.year

since = datetime(current_year, previous_month, 1, 0, 0, 0)
until = since + relativedelta(months=1) - relativedelta(seconds=1)
since_timestamp = int(since.timestamp() * 1000)
until_timestamp = int(until.timestamp() * 1000)

year_month = f"{since.year}_{str(since.month).zfill(2)}"

print(f"Extracting data from {since} to {until}")

games = get_games(LICHESS_USER, since=since_timestamp, until=until_timestamp)

if games:
    print(f"Extracted {len(games)} games.")
    df = pl.DataFrame({})

    for game in games:
        
        game_df = extract_game_data(game)
        df = df.vstack(game_df)
        
    print(f"Saving data for {year_month}")
    df.write_ndjson(games_data_path / f"{year_month}.ndjson")
    print(f"Data saved for {year_month}")
        
else:
    print("No games found. Exiting...")