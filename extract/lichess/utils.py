import polars as pl
import requests
from polars.exceptions import SchemaError
from datetime import datetime
import os
import ndjson

token = os.getenv('LICHESS_TOKEN')

def convert_seconds_to_hhmmss(total_centiseconds: int):
    """Converts total seconds into hh:mm:ss format

    Args:
        total_centiseconds (int): total centiseconds

    Returns:
        str: returns total seconds in hh:mm:ss format
    """
    
    total_seconds = total_centiseconds / 100
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    return f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"

def get_games(username: str, since: int = None, until: int = None) -> dict:
    """Calls the lichess API to get games of a user.
    Args:
        username (str): lichess username of player.
        since (int, optional): timestamp for beginning of period in milliseconds. Defaults to None.
        until (int, optional): timestamp for end of period in milliseconds. Defaults to None.

    Returns:
        dict: returns dictionary of response from API.
    """
    
    query_params = {
        'pgnInJson': 'true',
        'accuracy': 'true',
        'clocks': 'true',
        'evals': 'true',
        'opening': 'true',
        'sort': 'dateAsc'  
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/x-ndjson"
    }
    
    if since is not None:
        query_params['since'] = since
    if until is not None:
        query_params['until'] = until
    
    base_url = "https://lichess.org/api/games/user"
    url = f"{base_url}/{username}"
    
    response = requests.get(
        url,
        headers=headers,
        params=query_params
    ).json(
        cls=ndjson.Decoder
    )
    
    return response

def extract_game_data(game: dict) -> pl.DataFrame:
    """Parses the game data and returns a polars DataFrame.

    Args:
        game (dict): A dictionary containing information for a single game from
        the lichess API.

    Returns:
        pl.DataFrame: DataFrame containing all useful data.
    """
    
    game_id = game['id']
    game_status = game['status']
    game_winner = game.get('winner', "draw")
    url = f'https://lichess.org/{game_id}'
    
    print('Extracting data from:', url)
    
    time_class = game['speed']
    time_control = str(game.get('clock').get('initial')) + '+' + str(game.get('clock').get('increment'))
    is_rated = game['rated']

    white_rating = game['players']['white']['rating']
    black_rating = game['players']['black']['rating']

    white_accuracy = game['players']['white'].get('analysis', {'accuracy': None}).get('accuracy', None)
    black_accuracy = game['players']['black'].get('analysis', {'accuracy': None}).get('accuracy', None)
        
    opening_code = game['opening']['eco']
    opening_name = game['opening']['name']
    
    if game['players']['white']['user']['id'] == 'zainsiddiqi':
        colour = 'white'
        opponent_username = game['players']['black']['user']['name']
        opponent_id = game['players']['black']['user']['id']

    else:
        colour = 'black'
        opponent_username = game['players']['white']['user']['name']
        opponent_id = game['players']['white']['user']['id']
        

    opp_headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json"
    }

    opponent_api_url = f'https://lichess.org/api/user/{opponent_id}'
    opp_response = requests.get(opponent_api_url, headers=opp_headers)
    opp_info = opp_response.json()
    opponent_country = opp_info.get('profile', {'country': None}).get('country', None)
    opponent_is_verified = opp_info.get('verified', None)

    opp_disabled = opp_info.get('disabled', None)
    opp_tosViolation = opp_info.get('tosViolation', None)
    opp_patron = opp_info.get('patron', None)

    if opp_disabled:
        opponent_status = 'closed'
    elif opp_tosViolation:
        opponent_status = 'closed:fair_play_violations'
    elif opp_patron:
        opponent_status = 'patron'
    else:
        opponent_status = 'lichess member'
        
    start_datetime_dt = datetime.fromtimestamp(game['createdAt'] / 1000)
    end_datetime_dt = datetime.fromtimestamp(game['lastMoveAt'] / 1000)
    start_datetime = datetime.strftime(start_datetime_dt, '%Y-%m-%d %H:%M:%S')
    end_datetime = datetime.strftime(end_datetime_dt, '%Y-%m-%d %H:%M:%S')
    
    moves_list = game['moves'].split(' ')
    move_times_list = [convert_seconds_to_hhmmss(move_time) for move_time in game['clocks']][:-1]
    moves = ','.join(moves_list)
    move_times = ','.join(move_times_list)
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
        'game_winner': game_winner,
        'game_status': game_status,
        'colour': colour,
        'opponent_id': opponent_id,
        'opponent_username': opponent_username,
        'opponent_country': opponent_country,
        'opponent_is_verified': opponent_is_verified,
        'opponent_status': opponent_status,
        'start_datetime': start_datetime,
        'end_datetime': end_datetime,
        'opening_code': opening_code,
        'opening_name': opening_name,
        'total_moves': total_moves,
        'moves': moves,
        'move_times': move_times
    }
    
    df = pl.DataFrame(
        row
    ).with_columns(
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
        pl.col('game_winner').cast(pl.Utf8),
        pl.col('game_status').cast(pl.Utf8),
        pl.col('colour').cast(pl.Utf8),
        pl.col('opponent_id').cast(pl.Utf8),
        pl.col('opponent_username').cast(pl.Utf8),
        pl.col('opponent_country').cast(pl.Utf8),
        pl.col('opponent_is_verified').cast(pl.Boolean),
        pl.col('opponent_status').cast(pl.Utf8),
        pl.col('start_datetime').cast(pl.Utf8),
        pl.col('end_datetime').cast(pl.Utf8),
        pl.col('opening_code').cast(pl.Utf8),
        pl.col('opening_name').cast(pl.Utf8),
        pl.col('total_moves').cast(pl.Utf8),
        pl.col('moves').cast(pl.List(pl.Utf8)),
        pl.col('move_times').cast(pl.List(pl.Utf8)),
        pl.lit(datetime.now()).alias("_extracted_at")
    )
    
    return df