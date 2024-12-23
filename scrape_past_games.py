import pandas as pd
import main
from datetime import datetime as dt
from nba_api.stats.endpoints import LeagueGameFinder, BoxScoreTraditionalV2
from nba_api.stats.library.parameters import SeasonAll
import pandas as pd
import time
import pandas_gbq
from google.cloud import bigquery

# Basketball Reference URLs
seasons = ['2021-22','2022-23','2023-24','2024-25']

data = []

for season in seasons:
    game_id = []
    failed_games = []
    game_id.append(main.get_past_games(season))

    for game in game_id:
        time.sleep(1)
        g = main.get_box_score(game)
        if isinstance(g,pd.DataFrame):
            data.append(main.get_box_score(game))
        else:
            failed_games.append(game)
    while failed_games:
        game = failed_games.pop(0)
        g = main.get_box_score(game)
        if isinstance(g,pd.DataFrame):
            data.append(main.get_box_score(game))
        else:
            failed_games.append(game)

    combined_df = pd.concat(data,ignore_index= True)

    pandas_gbq.to_gbq(combined_df,project_id='miscellaneous-projects-444203',destination_table=f'basketball_reference.{season}',if_exists='replace')

