from google.cloud.bigquery import (
    LoadJobConfig,
    SchemaField,
    SourceFormat,
    WriteDisposition,
    TimePartitioning,
    TimePartitioningType,
    SchemaUpdateOption
)

job_config = LoadJobConfig(
    schema=[
        # SchemaField("name", "field_type", "mode", "default_value_expression", "description")
        SchemaField("game_id", "STRING", "Required", None, "id provided by licess"),
        SchemaField("url", "STRING", "NULLABLE", None, "game url on lichess.org"),
        SchemaField("time_class", "STRING", "NULLABLE", None, "time class of the game"),
        SchemaField("time_control", "STRING", "NULLABLE", None, "time control of the game"),
        SchemaField("is_rated", "BOOLEAN", "NULLABLE", None, "whether the game is rated or not"),
        SchemaField("white_rating", "INTEGER", "NULLABLE", None, "white player's rating"),
        SchemaField("black_rating", "INTEGER", "NULLABLE", None, "black player's rating"),
        SchemaField("white_accuracy", "FLOAT", "NULLABLE", None, "white player's accuracy"),
        SchemaField("black_accuracy", "FLOAT", "NULLABLE", None, "black player's accuracy"),
        SchemaField("game_winner", "STRING", "NULLABLE", None, "winner of the game. Can be 'white', 'black', or 'draw'"),
        SchemaField("game_status", "STRING", "NULLABLE", None, "can be draw, mate, resign, outoftime, aborted, stalemate, timeout, cheat"),
        SchemaField("colour", "STRING", "NULLABLE", None, "colour of the player"),
        SchemaField("opponent_id", "STRING", "NULLABLE", None, "opponent's id as provided by lichess"),
        SchemaField("opponent_username", "STRING", "NULLABLE", None, "opponent's username"),
        SchemaField("opponent_country", "STRING", "NULLABLE", None, "opponent's country"),
        SchemaField("opponent_is_verified", "BOOLEAN", "NULLABLE", None, "whether the opponent is verified or not"),
        SchemaField("opponent_status", "STRING", "NULLABLE", None, "opponent's status"),
        SchemaField("start_datetime", "STRING", "NULLABLE", None, "start datetime of the game in UTC"),
        SchemaField("end_datetime", "STRING", "NULLABLE", None, "end datetime of the game in UTC"),
        SchemaField("opening_code", "STRING", "NULLABLE", None, "opening code of the game"),
        SchemaField("opening_name", "STRING", "NULLABLE", None, "opening name of the game"),
        SchemaField("total_moves", "INTEGER", "NULLABLE", None, "total moves in the game"),
        SchemaField("moves", "STRING", "REPEATED", None, "moves of the game"),
        SchemaField("move_times", "STRING", "REPEATED", None, "move times of the game"),
        SchemaField("_extracted_at", "DATETIME", "NULLABLE", None, "datetime when the data was extracted")
    ],
    source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
    write_disposition=WriteDisposition.WRITE_APPEND,
    time_partitioning=TimePartitioning(
        type_=TimePartitioningType.MONTH
    ),
    schema_update_options=[
        SchemaUpdateOption.ALLOW_FIELD_RELAXATION
    ]
)