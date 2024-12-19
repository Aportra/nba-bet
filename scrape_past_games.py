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
import pandas_gbq as pgbq

#need to rerun 2022-2023, 2023-2024
urls = {#'NBA_Season_2021-2022_uncleaned':'https://www.nba.com/stats/teams/boxscores?Season=2021-22',
        'NBA_Season_2022-2023_uncleaned':'https://www.nba.com/stats/teams/boxscores?Season=2022-23',
        'NBA_Season_2023-2024_uncleaned':'https://www.nba.com/stats/teams/boxscores?Season=2023-24'}

driver = main.establish_driver()

for url in urls:

    driver.get(urls[url])
    main.select_all_option(driver)
    source = driver.page_source


    soup = BeautifulSoup(source, 'html5lib')

    text = soup.find_all('a',class_ = 'Anchor_anchor__cSc3P')

    href = [str(h.get('href')) for h in text if '/game' in h.get('href')]

    rows = driver.find_elements(By.XPATH, "/html/body/div[1]/div[2]/div[2]/div[3]/section[2]/div/div[2]/div[3]/table/tbody/tr")
    dates = []
    matchups = []
    game_ids = []
    for row in rows:
        date_element = row.find_element(By.XPATH, "./td[3]/a")
        game_date_text = date_element.text.strip()
        
        # Convert the extracted date text to a datetime.date object
        game_date = date.strptime(game_date_text, "%m/%d/%Y").date()
        print(f"Game Date: {game_date}")  # Debugging output

        matchup_element = row.find_element(By.XPATH, "./td[2]/a")
        game_id = matchup_element.get_attribute('href')
        matchup_text = matchup_element.text.strip()
        matchup_element.get_attribute('')
        if "@" in matchup_text:
            teams = matchup_text.split(" @ ")
        elif "vs." in matchup_text:
            teams = matchup_text.split(" vs. ")

        game_ids.append(game_id)
        matchups.append(teams[1])
        dates.append(game_date)

    # matches = [re.findall('\/game\/[0-9]+',h) for h in href]

    # game_ids = [item for sublist in matches for item in sublist]


    data = []
    failed_pages = []
    i = 0
    for game_id,date,matchup in zip(game_ids,dates,matchups):
        page = f'https://www.nba.com{game_id}/box-score'
        i += 1
        print(date,game_id,matchup)
        if i %100 == 0:
            print(f'processing the {i} request {round(len(data)/len(game_ids)*100,2)}% complete')
        result = main.process_page(page,game_id,driver,matchup,date)
        if isinstance(result, pd.DataFrame):
            data.append(result)
        else:
            failed_pages.append(result)
            print(f'Failed Pages length: {len(failed_pages)}')


    retries = {}
    while failed_pages:
        game_id,game_date = failed_pages.pop(0)
        if (game_id,game_date) in retries:
            retries[(game_id,game_date)] += 1
            print(f'Retry Count:{retries[game_id]}')
        else:
            retries[(game_id,game_date)] = 0
            retries[(game_id,game_date)] += 1
            print(f'Retry Count:{retries[game_id]}')

        print(f'processing # {game_id} from failed pages')
        page = f'https://www.nba.com{game_id}/box-score'
        result = main.process_page(page,game_id,driver,date,)

        if isinstance(result,pd.DataFrame):
            data.append(result)
            print(f'processed # {game_id} from failed pages')
        else:
            failed_pages.append((game_id))
            print(f'failed # {game_id} from failed pages, readded to be processed')

    #combine dataframes to upload to GBQ
    combined_dataframes = pd.concat(data,ignore_index= True)

    client = bigquery.Client('miscellaneous-projects-444203')

    combined_dataframes.columns

    combined_dataframes.rename(columns={'+/-':'plus_mins'},inplace=True)
    pgbq.to_gbq(combined_dataframes,project_id= 'miscellaneous-projects-444203',destination_table= f'miscellaneous-projects-444203.capstone_data.{url}')