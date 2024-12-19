import pandas as pd
import main
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from google.cloud import bigquery
import regex as re
import time
from datetime import datetime as date
#For email notifications



driver = main.establish_driver()


nba_games = 'https://www.nba.com/stats/teams/boxscores?Season=2024-25'

driver.get(nba_games)

rows = driver.find_elements(By.XPATH, "/html/body/div[1]/div[2]/div[2]/div[3]/section[2]/div/div[2]/div[3]/table/tbody/tr")
game_data = []
for row in rows:
    date_element = row.find_element(By.XPATH, "./td[3]/a")
    game_date_text = date_element.text.strip()
    
    # Convert the extracted date text to a datetime.date object
    game_date = date.strptime(game_date_text, "%m/%d/%Y").date()
    if game_date == date.today():
        #get matchup data
        matchup_element = row.find_element(By.XPATH, "./td[2]/a")
        game_id = matchup_element.get_attribute('href')
        matchup_text = matchup_element.text.strip()
        matchup_element.get_attribute('')
        if "@" in matchup_text:
            teams = matchup_text.split(" @ ")
        elif "vs." in matchup_text:
            teams = matchup_text.split(" vs. ")
        
        game_data.append((game_id,game_date,teams[1]))

data = []
failed_pages = []
i = 0
try:
    if game_data:
        for game_id,date,matchup in game_data:
            page = game_id
            i += 1
            if i %100 == 0:
                print(f'processing the {i} request {round(len(data)/len(game_data)*100,2)}% complete')
            result = main.process_page(page,game_id,game_date,matchup,driver)
            if isinstance(result, pd.DataFrame):
                data.append(result)
            else:
                failed_pages.append(result)
                print(f'Failed Pages length: {len(failed_pages)}')


        retries = {}
        while failed_pages:
            game_id,game_date,matchup = failed_pages.pop(0)
            if (game_id,game_date) in retries:
                retries[(game_id,game_date)] += 1
                print(f'Retry Count:{retries[game_id]}')
            else:
                retries[(game_id,game_date)] = 0
                retries[(game_id,game_date)] += 1
                print(f'Retry Count:{retries[game_id]}')

            print(f'processing # {game_id} from failed pages')
            page = f'https://www.nba.com{game_id}/box-score'
            result = main.process_page(page,game_id,game_date,matchup,driver)

            if isinstance(result,pd.DataFrame):
                data.append(result)
                print(f'processed # {game_id} from failed pages')
            else:
                failed_pages.append((game_id))
                print(f'failed # {game_id} from failed pages, readded to be processed')
        
            combined_dataframes = pd.concat(data,ignore_index= True)

        client = bigquery.Client('miscellaneous-projects-444203')

        combined_dataframes.columns

        combined_dataframes.rename(columns={'+/-':'plus_mins'},inplace=True)

        invalid_rows = ~combined_dataframes['MIN'].str.match(valid_time_pattern)

        columns_to_swap = ['FGM','FGA','FG%','3PM','3PA']
        valid_columns = ['game_id','game_date','matchup','url','last_updated']

        combined_dataframes.loc[invalid_rows,valid_columns] = combined_dataframes.loc[invalid_rows,columns_to_swap].to_numpy()

        combined_dataframes.loc[invalid_rows,columns_to_swap] = None

        combined_dataframes['game_date'] = pd.to_datetime(combined_dataframes['game_date'])
        combined_dataframes['last_updated'] = pd.to_datetime(combined_dataframes['last_updated'])

        pandas_gbq.to_gbq(combined_dataframes,project_id= 'miscellaneous-projects-444203',destination_table= f'miscellaneous-projects-444203.capstone_data.{url}',if_exists = 'append')

        send_email(
        subject = f"NBA SCRAPING: COMPLTETED # OF GAME {len(game_data)}",
        body = f'{len(game_data)} games scraped as of {date.today()}'
    )
    else:
        send_email(
        subject = "NBA SCRAPING: NO GAMES",
        body = f'No games as of {date.today()}'
    )

except Exception as e:
    send_email(
        subject = "NBA SCRAPING: SCIRPT CRASHED",
        body = f'The script encountered an error: \n{str(e)}'
    )
