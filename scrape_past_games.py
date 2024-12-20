import pandas as pd
import main
from datetime import datetime as date
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from google.cloud import bigquery
import regex as re
import time
import pandas_gbq

#need to rerun 2022-2023, 2023-2024
urls = {'NBA_Season_2021-2022_uncleaned':'https://www.nba.com/stats/teams/boxscores?Season=2021-22',
        'NBA_Season_2022-2023_uncleaned':'https://www.nba.com/stats/teams/boxscores?Season=2022-23',
        'NBA_Season_2023-2024_uncleaned':'https://www.nba.com/stats/teams/boxscores?Season=2023-24',
        'NBA_Season_2024-2025_uncleaned':'https://www.nba.com/stats/teams/boxscores?Season=2024-25'
}
valid_time_pattern = r"^\d{2}:\d{2}$"

driver = main.establish_driver()

for url in urls:

    driver.get(urls[url])
    main.select_all_option(driver)
    source = driver.page_source

    #For each row collect game_date,game_id, and matchup
    rows = driver.find_elements(By.XPATH, "/html/body/div[1]/div[2]/div[2]/div[3]/section[2]/div/div[2]/div[3]/table/tbody/tr")

    game_data =[]
    unique_game_id = set()
    for row in rows:
        date_element = row.find_element(By.XPATH, "./td[3]/a")
        game_date_text = date_element.text.strip()
        
        # Convert the extracted date text to a datetime.date object
        game_date = date.strptime(game_date_text, "%m/%d/%Y")

        #Get matchup data
        matchup_element = row.find_element(By.XPATH, "./td[2]/a")
        game_id = matchup_element.get_attribute('href')
        if game_id in unique_game_id:
            continue
        unique_game_id.add(game_id)
        matchup_text = matchup_element.text.strip()
        matchup_element.get_attribute('')
        if "@" in matchup_text:
            matchup = matchup_text.split(" @ ")
            away, home = matchup
        elif "vs." in matchup_text:
            matchup = matchup_text.split(" vs. ")
            home, away = matchup

        game_data.append((game_id,game_date,home,away))
    

    data = []
    failed_pages = []
    i = 0
    for game_id,date,home,away in game_data:
        page = f'{game_id}/box-score'
        i += 1
        if i %100 == 0:
            print(f'processing the {i} request {round(len(data)/len(game_data)*100,2)}% complete')
        result = main.process_page(page,game_id,game_date,home,away,driver)
        if isinstance(result, pd.DataFrame):
            data.append(result)
        else:
            failed_pages.append(result)
            print(f'Failed Pages length: {len(failed_pages)}')

    #Tracking number of retries per failed page
    retries = {}

    #Rerunning failed pages
    while failed_pages:
        game_id,game_date,home,away = failed_pages.pop(0)

        key = (game_id,game_date,home,away)

        if key in retries:
            retries[key] += 1
            print(f'Retry Count:{retries[key]}')
        else:
            retries[key] = 1
            print(f'Retry Count:{retries[key]}')

        print(f'processing # {game_id} from failed pages')
        page = f'{game_id}/box-score'
        result = main.process_page(page,game_id,game_date,home,away,driver)

        if isinstance(result,pd.DataFrame):
            data.append(result)
            print(f'processed # {game_id} from failed pages')
        #Catch for if they fail again
        else:
            failed_pages.append((game_id,game_date,home,away))
            print(f'failed # {game_id} from failed pages, readded to be processed')

    #Combine dataframes to upload to GBQ
    combined_dataframes = pd.concat(data,ignore_index= True)

    client = bigquery.Client('miscellaneous-projects-444203')

    combined_dataframes.rename(columns={'+/-':'plus_mins'},inplace=True)

    invalid_rows = ~combined_dataframes['MIN'].str.match(valid_time_pattern)

    columns_to_swap = ['FGM','FGA','FG%','3PM','3PA','3P%']
    valid_columns = ['team','game_id','game_date','matchup','url','last_updated']

    combined_dataframes.loc[invalid_rows,valid_columns] = combined_dataframes.loc[invalid_rows,columns_to_swap].to_numpy()

    combined_dataframes.loc[invalid_rows,columns_to_swap] = None

    combined_dataframes['game_date'] = pd.to_datetime(combined_dataframes['game_date'],errors='coerce')
    combined_dataframes['last_updated'] = pd.to_datetime(combined_dataframes['last_updated'],errors='coerce')
    combined_dataframes['url'] = combined_dataframes['url'].astype(str).str.strip()
    combined_dataframes['game_id'] = combined_dataframes['game_id'].str.lstrip('https://www.nba.com/game/')

    combined_dataframes[['FGM','FGA','FG%','3PM','3PA','3P%','FTM','FTA','FT%','OREB','DREB','REB','AST','STL','BLK','TO','PF','PTS','plus_mins']] = combined_dataframes[['FGM','FGA','FG%','3PM','3PA','3P%','FTM','FTA','FT%','OREB','DREB','REB','AST','STL','BLK','TO','PF','PTS','plus_mins']].astype('float64')
    pandas_gbq.to_gbq(combined_dataframes,project_id= 'miscellaneous-projects-444203',destination_table= f'miscellaneous-projects-444203.capstone_data.{url}',if_exists='replace')