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
urls = {#'NBA_Season_2021-2022_uncleaned':'https://www.nba.com/stats/teams/boxscores?Season=2021-22',
        #'NBA_Season_2022-2023_uncleaned':'https://www.nba.com/stats/teams/boxscores?Season=2022-23',
        #'NBA_Season_2023-2024_uncleaned':'https://www.nba.com/stats/teams/boxscores?Season=2023-24',
        'NBA_Season_2023-2024_uncleaned':'https://www.nba.com/stats/teams/boxscores?Season=2024-25'}

driver = main.establish_driver()

for url in urls:

    driver.get(urls[url])
    main.select_all_option(driver)
    source = driver.page_source

    #For each row collect game_date,game_id, and matchup
    rows = driver.find_elements(By.XPATH, "/html/body/div[1]/div[2]/div[2]/div[3]/section[2]/div/div[2]/div[3]/table/tbody/tr")
    game_data = []
    for row in rows:
        date_element = row.find_element(By.XPATH, "./td[3]/a")
        game_date_text = date_element.text.strip()
        
        # Convert the extracted date text to a datetime.date object
        game_date = date.strptime(game_date_text, "%m/%d/%Y").date() 

        #Get matchup data
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
    for game_id,date,matchup in game_data:
        page = f'{game_id}/box-score'
        i += 1
        if i %100 == 0:
            print(f'processing the {i} request {round(len(data)/len(game_data)*100,2)}% complete')
        result = main.process_page(page,game_id,game_date,matchup,driver)
        if isinstance(result, pd.DataFrame):
            data.append(result)
        else:
            failed_pages.append(result)
            print(f'Failed Pages length: {len(failed_pages)}')

    #Tracking number of retries per failed page
    retries = {}

    #Rerunning failed pages
    while failed_pages:
        game_id,game_date,matchup = failed_pages.pop(0)
        if (game_id,game_date,matchup) in retries:
            retries[(game_id,game_date,matchup)] += 1
            print(f'Retry Count:{retries[game_id]}')
        else:
            retries[(game_id,game_date,matchup)] = 0
            retries[(game_id,game_date,matchup)] += 1
            print(f'Retry Count:{retries[game_id]}')

        print(f'processing # {game_id} from failed pages')
        page = f'{game_id}/box-score'
        result = main.process_page(page,game_id,game_date,matchup,driver)

        if isinstance(result,pd.DataFrame):
            data.append(result)
            print(f'processed # {game_id} from failed pages')
        #Catch for if they fail again
        else:
            failed_pages.append((game_id,game_date,matchup))
            print(f'failed # {game_id} from failed pages, readded to be processed')

    #Combine dataframes to upload to GBQ
    combined_dataframes = pd.concat(data,ignore_index= True)

    client = bigquery.Client('miscellaneous-projects-444203')

    combined_dataframes.columns

    combined_dataframes.rename(columns={'+/-':'plus_mins'},inplace=True)


    pandas_gbq.to_gbq(combined_dataframes,project_id= 'miscellaneous-projects-444203',destination_table= f'miscellaneous-projects-444203.capstone_data.{url}')