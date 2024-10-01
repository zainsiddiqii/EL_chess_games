import polars as pl
import requests
from polars.exceptions import SchemaError
from datetime import datetime

headers = {'User-Agent': 'Mozilla/5.0'}

def get_archives(username: str) -> dict:
    
    base_url = "https://api.chess.com/pub/player"
    url = f"{base_url}/{username}/games/archives"
    response = requests.get(url, headers=headers).json()
    
    return response

def get_monthly_archive(year: int, month: int, username: str) -> dict:
    
    year = str(year)
    month = str(month).zfill(2)
    
    base_url = f"https://api.chess.com/pub/player/{username}/games/"
    url = f"{base_url}{year}/{month}"
    response = requests.get(url, headers=headers).json()
    
    return response

def extract_game_data(game: dict) -> pl.DataFrame:
    
    url = game['url']
    print('Extracting data from:', url)
    game_id = game['uuid']
    time_class = game['time_class'] # rapid, blitz, bullet
    time_control = game['time_control'] # in seconds
    is_rated = game['rated'] # boolean
    white_rating = game['white']['rating']
    black_rating = game['black']['rating']
    white_result = game['white']['result']
    black_result = game['black']['result']
    
    try:
        white_accuracy = game['accuracies']['white']
        black_accuracy = game['accuracies']['black']
    except KeyError:
        white_accuracy = None
        black_accuracy = None

    if game['white']['username'] == 'zainsiddiqii':
        colour = 'white'
        opponent_api_link = game['black']['@id']

    else:
        colour = 'black'
        opponent_api_link = game['white']['@id']

    opponent_data = requests.get(opponent_api_link, headers=headers).json()
    opponent_username = opponent_data['username']
    opponent_country = requests.get(opponent_data['country'], headers=headers).json()['name']
    opponent_is_verified = opponent_data['verified']
    opponent_status = opponent_data['status']
    opponent_id = opponent_data['player_id']
    
    pgn_data = game['pgn'].split('\n')
    
    start_date = pgn_data[2].strip('[]').split(' ')[1].strip('"')
    start_time = pgn_data[17].strip('[]').split(' ')[1].strip('"')
    end_date = pgn_data[18].strip('[]').split(' ')[1].strip('"')
    try:
        end_time = pgn_data[19].strip('[]').split(' ')[1].strip('"')
    except IndexError:
        end_time = None
        print('Game was not started. Skipping...')
    
    if end_time is None:
        start_datetime = None
        end_datetime = None
        opening_code = None
        opening_url = None
        opening_name = None
        move_times = None
        moves = None
        total_moves = None
    else:
        start_datetime = start_date.replace('.', '-') + ' ' + start_time
        end_datetime = end_date.replace('.', '-') + ' ' + end_time
        opening_code = pgn_data[9].strip('[]').split(' ')[1].strip('"')
        opening_url = pgn_data[10].strip('[]').split(' ')[1].strip('"')
        opening_name = pgn_data[10].strip('[]').split(' ')[1].strip('"').split('/')[-1].replace('-', ' ')
        
        pgn = game['pgn'].split('\n')[-2]
        pgn = pgn.split('}')
        moves = [moves.split('{')[0].strip() for moves in pgn]
        move_times = [moves.split('{')[-1] for moves in pgn]
        move_times = [move_times.split(' ')[-1].strip(']') for move_times in move_times]

        moves_list = [move.split(' ')[-1] for move in moves]
        move_times_list = [move.split(' ')[-1] for move in move_times]

        move_times = ','.join(move_times_list[:-1])
        moves = ','.join(moves_list[:-1])

        total_moves = len(moves_list) // 2

    row = {
        'game_id': game_id,
        'url': url,
        'time_class': time_class,
        'time_control': time_control,
        'is_rated': is_rated,
        'white_rating': white_rating,
        'black_rating': black_rating,
        'white_accuracy': white_accuracy,
        'black_accuracy': black_accuracy,
        'white_result': white_result,
        'black_result': black_result,
        'colour': colour,
        'opponent_id': opponent_id,
        'opponent_username': opponent_username,
        'opponent_country': opponent_country,
        'opponent_is_verified': opponent_is_verified,
        'opponent_status': opponent_status,
        'start_datetime': start_datetime,
        'end_datetime': end_datetime,
        'opening_code': opening_code,
        'opening_url': opening_url,
        'opening_name': opening_name,
        'total_moves': total_moves,
        'moves': moves,
        'move_times': move_times
    }
    
    df = pl.DataFrame(row).with_columns(
        pl.col('moves').str.split(','),
        pl.col('move_times').str.split(',')
    ).select(
        pl.col('game_id').cast(pl.Utf8),
        pl.col('url').cast(pl.Utf8),
        pl.col('time_class').cast(pl.Utf8),
        pl.col('time_control').cast(pl.Utf8),
        pl.col('is_rated').cast(pl.Boolean),
        pl.col('white_rating').cast(pl.Utf8),
        pl.col('black_rating').cast(pl.Utf8),
        pl.col('white_accuracy').cast(pl.Utf8),
        pl.col('black_accuracy').cast(pl.Utf8),
        pl.col('white_result').cast(pl.Utf8),
        pl.col('black_result').cast(pl.Utf8),
        pl.col('colour').cast(pl.Utf8),
        pl.col('opponent_id').cast(pl.Utf8),
        pl.col('opponent_username').cast(pl.Utf8),
        pl.col('opponent_country').cast(pl.Utf8),
        pl.col('opponent_is_verified').cast(pl.Boolean),
        pl.col('opponent_status').cast(pl.Utf8),
        pl.col('start_datetime').cast(pl.Utf8),
        pl.col('end_datetime').cast(pl.Utf8),
        pl.col('opening_code').cast(pl.Utf8),
        pl.col('opening_url').cast(pl.Utf8),
        pl.col('opening_name').cast(pl.Utf8),
        pl.col('total_moves').cast(pl.Utf8),
        pl.col('moves').cast(pl.List(pl.Utf8)),
        pl.col('move_times').cast(pl.List(pl.Utf8)),
        pl.lit(datetime.now()).alias("_extracted_at")
    )
    
    return df

def extract_monthly_data(year: int, month: int) -> pl.DataFrame:
    
    monthly_data = get_monthly_archive(year, month)
    games = monthly_data['games']
    
    df = pl.DataFrame({})
    
    for game in games:
        try:
            game_df = extract_game_data(game)
            df = df.vstack(game_df)
        except SchemaError:
            continue
    
    return df