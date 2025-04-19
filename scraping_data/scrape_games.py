"""Module for scraping NBA game data and uploading to BigQuery."""

import time
import random
import traceback
from datetime import datetime as dt,timedelta

import pandas as pd
import pandas_gbq
from scraping_data import utils
from google.oauth2 import service_account
from nba_api import boxscoretraditionalv3


def scrape_current_games():
    """Scrapes NBA game data and uploads it to BigQuery."""
    
    try:
        credentials = service_account.Credentials.from_service_account_file(
            "/home/aportra99/scraping_key.json"
        )
        local = False
    except FileNotFoundError:
        print("File not found. Continuing as if on local.")
        local = True
        credentials = None

    query ="""
        select *
        from `capstone_data.schedule`
        """
    schedule = pandas_gbq.read_gbq(query,credentials=credentials)

    try:
        scrape_date = dt.today() 

        if scrape_date <= max(schedule['date']):
            url = {
                "2024-2025_uncleaned": "https://stats.nba.com/stats/leaguegamelog?LeagueID=00&Season=2024-25&SeasonType=Regular%20Season&PlayerOrTeam=T&Counter=0&Sorter=DATE&Direction=DESC"

            }
        else:
            url = {
                "2024-2025_uncleaned": "https://stats.nba.com/stats/leaguegamelog?LeagueID=00&Season=2024-25&SeasonType=Playoffs&PlayerOrTeam=T&Counter=0&Sorter=DATE&Direction=DESC"

            } 

        response = utils.establish_requests(url["2024-2025_uncleaned"])
        year = scrape_date.year if scrape_date.month >= 10 else scrape_date.year - 1
        season = f'{year}-{year+1}'
        time.sleep(5)

        if response.status_code == 200:
            print(response.status_code)
            data = response.json()

            headers = [header.lower() for header in data['resultSets'][0]['headers']]
        
            rows = data['resultSets'][0]['rowSet']
            
            df = pd.DataFrame(rows,columns=headers)
            print(df)
            df = df.drop(columns=['video_available'])
            print(df[['game_date','matchup']])
            df['game_date'] = pd.to_datetime(df['game_date']).dt.date
            print(df['game_date'])

            team_table_id = f"capstone_data.{season}_team_ratings"
            team_table_schema = [{"name": "game_date", "type": "DATE"}]  
            
            df = df[df['game_date'] == scrape_date.date()]

            date = df['game_date'].iloc[0]


            # Upload team data
            if local:
                    pandas_gbq.to_gbq(
                    df,
                    project_id="miscellaneous-projects-444203",
                    destination_table=team_table_id,
                    if_exists="append",
                    table_schema=team_table_schema,
                )

            else:
                pandas_gbq.to_gbq(
                    df,
                    project_id="miscellaneous-projects-444203",
                    destination_table=team_table_id,
                    if_exists="append",
                    credentials=credentials,
                    table_schema=team_table_schema,)

            game_ids = list(df[df['game_date'] == scrape_date.date()]['game_id'])

            

            games = []

            for game in game_ids:
                print(game)
                game_response = utils.establish_requests(f"https://stats.nba.com/stats/boxscoretraditionalv3?GameID={game}&StartPeriod=0&EndPeriod=10")
                game_response = game_response.json()

                # Home team players
                home_players = game_response['boxScoreTraditional']['homeTeam']['players']
                home_df = pd.json_normalize(home_players)

                # Away team players
                away_players = game_response['boxScoreTraditional']['awayTeam']['players']
                away_df = pd.json_normalize(away_players)

                # Add context
                home_df['team'] = game_response['boxScoreTraditional']['homeTeam']['teamTricode']
                away_df['team'] = game_response['boxScoreTraditional']['awayTeam']['teamTricode']

                # Combine
                game_data = pd.concat([home_df, away_df], ignore_index=True) 

                
                rename_map = {
                    'personId': 'player_id',
                    'statistics.minutes': 'min',
                    'statistics.fieldGoalsMade': 'fgm',
                    'statistics.fieldGoalsAttempted': 'fga',
                    'statistics.fieldGoalsPercentage': 'fg_pct',
                    'statistics.threePointersMade': 'fg3m',
                    'statistics.threePointersAttempted': 'fg3a',
                    'statistics.threePointersPercentage': 'fg3_pct',
                    'statistics.freeThrowsMade': 'ftm',
                    'statistics.freeThrowsAttempted': 'fta',
                    'statistics.freeThrowsPercentage': 'ft_pct',
                    'statistics.reboundsOffensive': 'oreb',
                    'statistics.reboundsDefensive': 'dreb',
                    'statistics.reboundsTotal': 'reb',
                    'statistics.assists': 'ast',
                    'statistics.steals': 'stl',
                    'statistics.blocks': 'blk',
                    'statistics.turnovers': 'to',
                    'statistics.foulsPersonal': 'pf',
                    'statistics.points': 'pts',
                    'statistics.plusMinusPoints': 'plus_minus',
                    'team': 'team_abbreviation'
                }

                game_data.rename(columns=rename_map,inplace=True)

                game_data['min'] = game_data['min'].apply(lambda x: ''.join(x.split('.000000')) if isinstance(x, str) and '.000000' in x else x)


                game_data['player_name'] = game_data.apply(lambda row: f"{row['firstName']} {row['familyName']}", axis=1)
                game_data['game_id'] = game
                game_data.rename(columns={''})

                games.append(game_data)

            full_data = pd.concat(games)
            # Your desired columns
            desired_columns = [
                'game_id', 'team_abbreviation', 'player_id', 'player_name', 'min',
                'fgm', 'fga', 'fg_pct', 'fg3m', 'fg3a', 'fg3_pct',
                'ftm', 'fta', 'ft_pct', 'oreb', 'dreb', 'reb',
                'ast', 'stl', 'blk', 'to', 'pf', 'pts', 'plus_minus'
            ]

            # Drop all other columns
            full_data = full_data[[col for col in desired_columns if col in full_data.columns]]          

            if len(full_data) > 0:
                print(len(full_data))
                utils.send_email(
                    subject=f"NBA SCRAPING: COMPLETED # OF GAMES {len(game_data)}",
                    body=f"{len(game_data)} games scraped as of {scrape_date.date()}",
                )

                if local:
                    pandas_gbq.to_gbq(
                    full_data,
                    project_id="miscellaneous-projects-444203",
                    destination_table='capstone_data.2024-2025_uncleaned',
                    if_exists="append"
                    )

                else:
                    pandas_gbq.to_gbq(
                        full_data,
                        project_id="miscellaneous-projects-444203",
                        destination_table='capstone_data.2024-2025_uncleaned',
                        if_exists="append",
                        credentials=credentials)
                print("Scraping successful.")

                return df,full_data,date
        
        else:
            print('no games')
            utils.send_email(
                subject="NBA SCRAPING: NO GAMES",
                body=f"No games found as of {scrape_date.date()}",
            )

    except Exception as e:
        error_traceback = traceback.format_exc()
        error_message = f"""
        NBA SCRAPING: SCRIPT CRASHED

        The script encountered an error:
        Type: {type(e).__name__}
        Message: {str(e)}

        Full Traceback:
        {error_traceback}
        """
        print(error_message)

        utils.send_email(
            subject="NBA SCRAPING: SCRIPT CRASHED",
            body=f'{error_message} retrying',
        )

        time.sleep(10)
        scrape_current_games()




def scrape_past_games():
    """Scrapes past NBA game data from 2015 to 2025 and uploads to BigQuery.
    
    Args:
        multi_threading (bool): Whether to use multi-threading for scraping.
        max_workers (int): Number of workers to use for multi-threading.
    """

    urls = {
        f"{i}-{i+1}_uncleaned": f"https://stats.nba.com/stats/leaguegamelog?LeagueID=00&Season={i}-{str(i-2000+1)}&SeasonType=Regular%20Season&PlayerOrTeam=T&Counter=0&Sorter=DATE&Direction=DESC"
        for i in range(2015, 2025)
    }
    seasons = [f'{i}-{i+1}_team_ratings' for i in range(2015,2025)]

    try:
        credentials = service_account.Credentials.from_service_account_file(
            "/home/aportra99/scraping_key.json"
        )
        local = False
        print("Credentials file loaded.")
    except FileNotFoundError:
        local = True
        credentials = None
        print("Running with default credentials.")

    for url,season in zip(urls,seasons):
        print(url)
        print(season)
        response = utils.establish_requests(urls[url])
        time.sleep(random.randint(5,10))
        print(response.status_code)
        if response.status_code == 200:
            data = response.json()

            headers = [header.lower() for header in data['resultSets'][0]['headers']]
            rows = data['resultSets'][0]['rowSet']
            df = pd.DataFrame(rows,columns=headers)
            df = df.drop(columns=['video_available'])
            df['game_date'] = pd.to_datetime(df['game_date']).dt.date

                         
            team_table_id = f"miscellaneous-projects-444203.capstone_data.{season}"
            table_schema = [{"name": "game_date", "type": "DATE"}]  
        
            if local:
                pandas_gbq.to_gbq(
                    df,
                    project_id="miscellaneous-projects-444203",
                    destination_table=team_table_id,
                    if_exists="replace",
                    table_schema=table_schema,
                )

            else:
                pandas_gbq.to_gbq(
                    df,
                    project_id="miscellaneous-projects-444203",
                    destination_table=team_table_id,
                    if_exists="replace",
                    credentials=credentials,
                    table_schema=table_schema,)

            game_ids = list(df['game_id'].unique())

            games = []
            retries = []
            for game in game_ids:
                time.sleep(random.uniform(.01,1))
                game_response = utils.establish_requests(f"https://stats.nba.com/stats/boxscoretraditionalv3?GameID={game}&StartPeriod=0&EndPeriod=10")

                if game_response.status_code == 200:
                    game_response = game_response.json()
                    

                    # Home team players
                    home_players = game_response['boxScoreTraditional']['homeTeam']['players']
                    home_df = pd.json_normalize(home_players)

                    # Away team players
                    away_players = game_response['boxScoreTraditional']['awayTeam']['players']
                    away_df = pd.json_normalize(away_players)

                    # Add context
                    home_df['team'] = game_response['boxScoreTraditional']['homeTeam']['teamTricode']
                    away_df['team'] = game_response['boxScoreTraditional']['awayTeam']['teamTricode']

                    # Combine
                    game_data = pd.concat([home_df, away_df], ignore_index=True) 

                    
                    rename_map = {
                        'personId': 'player_id',
                        'statistics.minutes': 'min',
                        'statistics.fieldGoalsMade': 'fgm',
                        'statistics.fieldGoalsAttempted': 'fga',
                        'statistics.fieldGoalsPercentage': 'fg_pct',
                        'statistics.threePointersMade': 'fg3m',
                        'statistics.threePointersAttempted': 'fg3a',
                        'statistics.threePointersPercentage': 'fg3_pct',
                        'statistics.freeThrowsMade': 'ftm',
                        'statistics.freeThrowsAttempted': 'fta',
                        'statistics.freeThrowsPercentage': 'ft_pct',
                        'statistics.reboundsOffensive': 'oreb',
                        'statistics.reboundsDefensive': 'dreb',
                        'statistics.reboundsTotal': 'reb',
                        'statistics.assists': 'ast',
                        'statistics.steals': 'stl',
                        'statistics.blocks': 'blk',
                        'statistics.turnovers': 'to',
                        'statistics.foulsPersonal': 'pf',
                        'statistics.points': 'pts',
                        'statistics.plusMinusPoints': 'plus_minus',
                        'team': 'team_abbreviation'
                    }

                    game_data.rename(columns=rename_map,inplace=True)

                    game_data['min'] = game_data['min'].apply(lambda x: ''.join(x.split('.000000')) if isinstance(x, str) and '.000000' in x else x)
                    game_data['player_name'] = game_data.apply(lambda row: f"{row['firstName']} {row['familyName']}", axis=1)
                    game_data['game_id'] = game
                    games.append(game_data)

                else:
                    retries.append(game)

            if len(retries) > 0:
                while retries:
                        retry_id = retries.pop(0) 
                        game_response = utils.establish_requests(f"https://stats.nba.com/stats/boxscoretraditionalv3?GameID={retry_id}&StartPeriod=0&EndPeriod=10")
                        if game_response.status_code == 200:
                            game_response = game_response.json()

                            column = [header.lower() for header in game_response['resultSets'][0]['headers']]
                            row_data = game_response['resultSets'][0]['rowSet']

                            game_data = pd.DataFrame(row_data,columns=column)
                            game_data.drop(columns=['comment','start_position','nickname'],inplace=True)

                            game_data['min'] = game_data['min'].apply(lambda x: ''.join(x.split('.000000')) if isinstance(x, str) and '.000000' in x else x)


                            games.append(game_data)
                        else:
                            retries.append(retry_id)

            full_data = pd.concat(games)

            desired_columns = [
                'game_id', 'team_abbreviation', 'player_id', 'player_name', 'min',
                'fgm', 'fga', 'fg_pct', 'fg3m', 'fg3a', 'fg3_pct',
                'ftm', 'fta', 'ft_pct', 'oreb', 'dreb', 'reb',
                'ast', 'stl', 'blk', 'to', 'pf', 'pts', 'plus_minus'
            ]

            # Drop all other columns
            full_data = full_data[[col for col in desired_columns if col in full_data.columns]]

        table_id = f"miscellaneous-projects-444203.capstone_data.{url}"
        table_schema = [{"name": "game_date", "type": "DATE"}]

        if local:
            pandas_gbq.to_gbq(
                full_data,
                project_id="miscellaneous-projects-444203",
                destination_table=table_id,
                if_exists="replace",
                table_schema=table_schema,
            )

        else:
            pandas_gbq.to_gbq(
                full_data,
                project_id="miscellaneous-projects-444203",
                destination_table=table_id,
                if_exists="replace",
                credentials=credentials,
                table_schema=table_schema,)
            