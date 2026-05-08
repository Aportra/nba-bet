"""Module for scraping NBA game data and uploading to BigQuery."""

import time
import random
import traceback
from datetime import datetime as dt,timedelta

import pandas as pd
import pandas_gbq
from scraping_data import utils
from google.oauth2 import service_account


num_retries = 0


def scrape_current_games(retries):
    psql = utils.psql()

    try:
        # scrape_date = dt.today()
        date_string = "2026-05-07"
        scrape_date = dt.strptime(date_string, "%Y-%m-%d")
        game_date = dt(2026, 4, 13)

        if scrape_date <= game_date:
            url = {
                "2025-2026_uncleaned": "https://stats.nba.com/stats/leaguegamelog?LeagueID=00&Season=2025-26&SeasonType=Regular%20Season&PlayerOrTeam=T&Counter=0&Sorter=DATE&Direction=DESC"

            }
        else:
            url = {
                "2025-2026_uncleaned": "https://stats.nba.com/stats/leaguegamelog?LeagueID=00&Season=2025-26&SeasonType=Playoffs&PlayerOrTeam=T&Counter=0&Sorter=DATE&Direction=DESC"

            }

        response = utils.establish_requests(url["2025-2026_uncleaned"])
        print(response.status_code)
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

            psql_table_id = f"{season}_team_ratings"
            df = df[df['game_date'] == scrape_date.date()]

            print(df)
            date = list(df['game_date'])[0]


            # Upload team data
            psql.upload_data(df, psql_table_id)
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

                games.append(game_data)
                time.sleep(5)
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
            psql_data = full_data.copy()

            full_data.columns = full_data.columns.str.replace('%', '_pct')
            full_data.columns = full_data.columns.str.replace('3', 'three_')
            if 'to' in full_data.columns:
                full_data.rename(columns={'to': 'turnovers'}, inplace=True)
            df.columns = df.columns.str.replace('%', '_pct')
            df.columns = df.columns.str.replace('3', 'three_')
            if 'to' in df.columns:
                df.rename(columns={'to': 'turnovers'}, inplace=True)
            print(psql_data)
            if len(full_data) > 0:
                print(len(full_data))
                utils.send_message('scraping of new games complete')
                print("Scraping successful.")
                for i in range(0, 3):
                    try:
                        psql.upload_data(psql_data, '2025-2026_uncleaned')
                        return df, full_data, date
                    except Exception as e:
                        utils.send_message(f'NBA SCRAPING:Upload Failed Retry num: {i+1} Error: {e}') 
                        time.sleep(10)
                        continue
                utils.send_message('NBA SCRAPING:Upload Failed Returning Data')
                return df, full_data, date
        else:
            print('no games')
            utils.send_message('no games to scrape')
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

        utils.send_message(f'NBA SCRAPING: SCRIPT CRASHED {error_message} retrying')
        time.sleep(10)
        retries += 1
        if retries < 5:
            scrape_current_games(retries)


