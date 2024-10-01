from utils import get_games, extract_game_data
from pathlib import Path
import polars as pl
import os
from datetime import datetime

games_data_path = Path.cwd() / 'games_data' / 'lichess'
LICHESS_USER = os.getenv('LICHESS_USER')

# Create a datetime object for August 31, 2024, at 23:59:59
until = datetime(2024, 8, 31, 23, 59, 59)

# Convert to timestamp
until_timestamp = int(until.timestamp() * 1000)

games = get_games(LICHESS_USER, since=None, until=until_timestamp)
games_store = dict()

for game in games:
    
    df = extract_game_data(game)
    
    str_date = df['start_datetime'].str.split(' ').list.get(0).item()
    year_month = f"{str_date.split('-')[0]}_{str_date.split('-')[1]}"
    
    if year_month not in games_store:
        games_store[year_month] = [df]
    else:
        games_store[year_month].append(df)
    
    print(f'Extracted and stored data for game {df['game_id'].item()} from {year_month}.')

for year_month, games in games_store.items():
    
    print(f"Saving data for {year_month}")
    df = pl.concat(games)
    df.write_ndjson(games_data_path / f"{year_month}.ndjson")
    print(f"Data saved for {year_month}")