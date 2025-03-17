"""Module for scraping NBA game data and uploading to BigQuery."""

import time
import random
import gc
import traceback
from datetime import datetime as dt,timedelta

import pandas as pd
import pandas_gbq
import utils as utils
from google.cloud import bigquery
from google.oauth2 import service_account
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from tqdm import tqdm


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

    try:
        scrape_date = dt.today() - timedelta(1)
        url = {
            "2024-2025_uncleaned": "https://stats.nba.com/stats/teamgamelogs?LeagueID=00&Season=2024-25&SeasonType=Regular%20Season"

        }

        response = utils.establish_requests(url["2024-2025_uncleaned"])
        year = scrape_date.year if scrape_date.month >= 10 else scrape_date.year - 1
        season = f'{year}-{year+1}'
        time.sleep(5)

        if response.status_code == 200:
            data = response.json()

            headers = [header.lower() for header in data['resultSets'][0]['headers']]
            rows = data['resultSets'][0]['rowSet']
            df = pd.DataFrame(rows,columns=headers)
            df = df.drop(columns=[col for col in df.columns if '_rank' in col or col == 'available_flag'])
            df['game_date'] = pd.to_datetime(df['game_date']).dt.date

            team_table_id = f"miscellaneous-projects-444203.capstone_data.{season}_team_ratings"
            table_schema = [{"name": "game_date", "type": "DATE"}]  
            
            df = df[df['game_date'] == scrape_date.date()]
            
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

            game_ids = list(df[df['game_date'] == scrape_date.date()]['game_id'])

            

            games = []

            for game in game_ids:
                game_response = utils.establish_requests(f"https://stats.nba.com/stats/boxscoretraditionalv2?GameID={game}&StartPeriod=0&EndPeriod=10")
                game_response = game_response.json()
                column = [header.lower() for header in game_response['resultSets'][0]['headers']]
                row_data = game_response['resultSets'][0]['rowSet']

                game_data = pd.DataFrame(row_data,columns=column)
                game_data.drop(columns=['comment','start_position','nickname'],inplace=True)

                game_data['min'] = game_data['min'].apply(lambda x: ''.join(x.split('.000000')) if isinstance(x, str) and '.000000' in x else x)
                
                games.append(game_data)

            full_data = pd.concat(games)

            table_id = f"miscellaneous-projects-444203.capstone_data.{url}_team_ratings"
            table_schema = [{"name": "game_date", "type": "DATE"}]  


            if full_data:
                utils.send_email(
                    subject=f"NBA SCRAPING: COMPLETED # OF GAMES {len(game_data)}",
                    body=f"{len(game_data)} games scraped as of {scrape_date.date()}",
                )

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
                print("Scraping successful.")

                return full_data, df
        
        else:
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
            body=error_message,
        )




def scrape_past_games():
    """Scrapes past NBA game data from 2015 to 2025 and uploads to BigQuery.
    
    Args:
        multi_threading (bool): Whether to use multi-threading for scraping.
        max_workers (int): Number of workers to use for multi-threading.
    """
    urls = {
        f"{i}-{i+1}_uncleaned": f"https://stats.nba.com/stats/teamgamelogs?LeagueID=00&Season={i}-{str(i-2000+1)}&SeasonType=Regular%20Season"
        for i in range(2017, 2025)
    }
    seasons = [f'{i}-{i+1}_team_ratings' for i in range(2017,2025)]

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
            df = df.drop(columns=['available_flag'])
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
            for game in tqdm(game_ids,desc='Processing games:'):
                time.sleep(random.uniform(.01,1))
                game_response = utils.establish_requests(f"https://stats.nba.com/stats/boxscoretraditionalv2?GameID={game}&StartPeriod=0&EndPeriod=10")

                if game_response.status_code == 200:
                    game_response = game_response.json()
    

                    column = [header.lower() for header in game_response['resultSets'][0]['headers']]
                    row_data = game_response['resultSets'][0]['rowSet']

                    game_data = pd.DataFrame(row_data,columns=column)
                    game_data.drop(columns=['comment','start_position','nickname'],inplace=True)

                    game_data['min'] = game_data['min'].apply(lambda x: ''.join(x.split('.000000')) if isinstance(x, str) and '.000000' in x else x)


                    games.append(game_data)

                else:
                    retries.append(game)

            if len(retries) > 0:
                while retries:
                        retry_id = retries.pop(0) 
                        game_response = utils.establish_requests(f"https://stats.nba.com/stats/boxscoretraditionalv2?GameID={retry_id}&StartPeriod=0&EndPeriod=10")
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
            

scrape_past_games()