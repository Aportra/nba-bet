#!/home/aportra99/venv/bin/activate
import pandas as pd
import scraping_data.utils as utils
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException 
from selenium.webdriver.firefox.options import Options
from selenium import webdriver
from bs4 import BeautifulSoup
from google.cloud import bigquery
import regex as re
import time
from datetime import datetime as date
from datetime import timedelta
import pandas_gbq
import traceback
from google.oauth2 import service_account

def scrape_current_data():
    credentials = service_account.Credentials.from_service_account_file('/home/aportra99/scraping_key.json') #For Google VM

    scoped_credentials = credentials.with_scopes(
        ['https://www.googleapis.com/auth/cloud-platform'])

    driver = utils.establish_driver()
    # driver = webdriver.Firefox()


    scrape_date = date.today() - timedelta(1)

    url = {'NBA_Season_2024-2025_uncleaned':'https://www.nba.com/stats/teams/boxscores?Season=2024-25'}

    driver.get(url['NBA_Season_2024-2025_uncleaned'])


    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

    valid_time_pattern = r"^\d{1,2}:\d{1,2}$"

    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/div[2]/div[2]/div[3]/section[2]/div/div[2]/div[3]/table"))
        )
        print("The section has loaded!")
    except TimeoutException:
            utils.send_email(
            subject = "NBA SCRAPING: DATE ERRORS",
            body = str("The section did not load in time."))


    rows = driver.find_elements(By.XPATH, "//tbody[@class='Crom_body__UYOcU']/tr")
    game_data = []
    unique_game_id = set()
    for row in rows:
        date_element = row.find_element(By.XPATH, "./td[3]/a")
        game_date_text = date_element.text.strip()
        
        # Convert the extracted date text to a datetime.date object
        try:
        # First, try parsing with the expected format
            game_date = date.strptime(game_date_text, "%m/%d/%Y").date()
        except ValueError:
            utils.send_email(
            subject = "NBA SCRAPING: DATE ERRORS",
            body = str(f"Unrecognized date format: {game_date_text}"))
        if game_date == scrape_date.date():
            #get matchup data
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
    try:
        if game_data:
            for game_id,g_date,home,away in game_data:
                page = game_id
                i += 1
                if i %100 == 0:
                    print(f'processing the {i} request {round(len(data)/len(game_data)*100,2)}% complete')
                result = utils.process_page(page,game_id,g_date,home,away,driver)
                if isinstance(result, pd.DataFrame):
                    data.append(result)
                else:
                    failed_pages.append(result)
                    print(f'Failed Pages length: {len(failed_pages)}')


            retries = {}
            while failed_pages:
                game_id,g_date,home,away = failed_pages.pop(0)

                key = (game_id,g_date,home,away)

                if key in retries:
                    retries[key] += 1
                    print(f'Retry Count:{retries[key]}')
                else:
                    retries[key] = 1
                    print(f'Retry Count:{retries[key]}')

                print(f'processing # {game_id} from failed pages')
                page = f'{game_id}/box-score'
                result = utils.process_page(page,game_id,g_date,home,away,driver)

                if isinstance(result,pd.DataFrame):
                    data.append(result)
                    print(f'processed # {game_id} from failed pages')
                #Catch for if they fail again
                else:
                    failed_pages.append((game_id,g_date,home,away))
                    print(f'failed # {game_id} from failed pages, readded to be processed')
            
            combined_dataframes = pd.concat(data,ignore_index= True)

            client = bigquery.Client('miscellaneous-projects-444203',credentials= credentials)

            combined_dataframes.rename(columns={'+/-':'plus_mins'},inplace=True)

            invalid_rows = ~combined_dataframes['MIN'].str.match(valid_time_pattern)

            columns_to_swap = ['FGM','FGA','FG%','3PM','3PA','3P%']
            valid_columns = ['team','game_id','game_date','matchup','url','last_updated']

            combined_dataframes.loc[invalid_rows, valid_columns] = combined_dataframes.loc[invalid_rows, columns_to_swap].values

            combined_dataframes.loc[invalid_rows,columns_to_swap] = None

            combined_dataframes['game_date'] = pd.to_datetime(combined_dataframes['game_date'],errors='coerce')
            combined_dataframes['last_updated'] = pd.to_datetime(combined_dataframes['last_updated'],errors='coerce')
            combined_dataframes['url'] = combined_dataframes['url'].astype(str).str.strip()
            combined_dataframes['game_id'] = combined_dataframes['game_id'].str.lstrip('https://www.nba.com/game/')

            num_columns = ['FGM', 'FGA', 'FG%', '3PM', '3PA', '3P%', 'FTM', 'FTA', 'FT%', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TO', 'PF', 'PTS', 'plus_mins']
            combined_dataframes[num_columns] = combined_dataframes[num_columns].apply(pd.to_numeric, errors='coerce')

            pandas_gbq.to_gbq(combined_dataframes,project_id= 'miscellaneous-projects-444203',destination_table= f'miscellaneous-projects-444203.capstone_data.NBA_Season_2024-2025_uncleaned',if_exists = 'append',credentials=credentials)
            
            #pandas_gbq.to_gbq(combined_dataframes,project_id= 'miscellaneous-projects-444203',destination_table= f'miscellaneous-projects-444203.capstone_data.test',if_exists = 'append',credentials=credentials)
            utils.send_email(
            subject = str(f"NBA SCRAPING: COMPLTETED # OF GAMES {len(game_data)}"),
            body = str(f'{len(game_data)} games scraped as of {scrape_date.date()}')
        )
        else:
            utils.send_email(
            subject = "NBA SCRAPING: NO GAMES",
            body = str(f'No games as of {scrape_date.date()}')
        )

    except Exception as e:
        error_traceback = traceback.format_exc()
        
        # Prepare a detailed error message
        error_message = f"""
        NBA SCRAPING: SCRIPT CRASHED

        The script encountered an error:
        Type: {type(e).__name__}
        Message: {str(e)}

        Full Traceback:
        {error_traceback}
        """
        print(error_message)
        #Send the email with detailed information
        utils.send_email(
            subject="NBA SCRAPING: SCRIPT CRASHED",
            body=error_message
        )


    driver.quit()

def scrape_past_games():
    
    urls = {'NBA_Season_2021-2022_uncleaned':'https://www.nba.com/stats/teams/boxscores?Season=2021-22',
            'NBA_Season_2022-2023_uncleaned':'https://www.nba.com/stats/teams/boxscores?Season=2022-23',
            'NBA_Season_2023-2024_uncleaned':'https://www.nba.com/stats/teams/boxscores?Season=2023-24',
            'NBA_Season_2024-2025_uncleaned':'https://www.nba.com/stats/teams/boxscores?Season=2024-25'
    }
    valid_time_pattern = r"^\d{1,2}:\d{1,2}$"

    options = Options()

    options.add_argument("--headless")

    driver = webdriver.Firefox(options=options)
    driver.set_window_size(1920, 1080)


    for url in urls:

        driver.get(urls[url])
        utils.select_all_option(driver)
        source = driver.page_source

        #For each row collect game_date,game_id, and matchup
        rows = driver.find_elements(By.XPATH, "//tbody[@class='Crom_body__UYOcU']/tr")
        game_data =[]
        unique_game_id = set()
        for idx,row in enumerate(rows):
            if (idx+1) % 10 == 0:
                print(f'{round((idx+1)/len(rows)*100,2)}% gathered')
            date_element = row.find_element(By.XPATH, ".//td[3]/a")
            game_date_text = date_element.text.strip()    
            
            # Convert the extracted date text to a datetime.date object
            game_date = date.strptime(game_date_text, "%m/%d/%Y")
            #Get matchup data
            matchup_element = row.find_element(By.XPATH, ".//td[2]/a")
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
            result = utils.process_page(page,game_id,date,home,away,driver)
            if isinstance(result, pd.DataFrame):
                data.append(result)
            else:
                failed_pages.append(result)
                print(f'Failed Pages length: {len(failed_pages)}')

        #Tracking number of retries per failed page
        retries = {}

        #Rerunning failed pages
        while failed_pages:
            game_id,date,home,away = failed_pages.pop(0)

            key = (game_id,date,home,away)

            if key in retries:
                retries[key] += 1
                print(f'Retry Count:{retries[key]}')
            else:
                retries[key] = 1
                print(f'Retry Count:{retries[key]}')

            print(f'processing # {game_id} from failed pages')
            page = f'{game_id}/box-score'
            result = utils.process_page(page,game_id,date,home,away,driver)

            if isinstance(result,pd.DataFrame):
                data.append(result)
                print(f'processed # {game_id} from failed pages')
            #Catch for if they fail again
            else:
                failed_pages.append((game_id,date,home,away))
                print(f'failed # {game_id} from failed pages, readded to be processed')

        #Combine dataframes to upload to GBQ
        combined_dataframes = pd.concat(data,ignore_index= True)

        client = bigquery.Client('miscellaneous-projects-444203')

        combined_dataframes.rename(columns={'+/-':'plus_mins'},inplace=True)

        invalid_rows = ~combined_dataframes['MIN'].str.match(valid_time_pattern)

        columns_to_swap = ['FGM','FGA','FG%','3PM','3PA','3P%']
        valid_columns = ['team','game_id','game_date','matchup','url','last_updated']

        combined_dataframes.loc[invalid_rows, valid_columns] = combined_dataframes.loc[invalid_rows, columns_to_swap].values

        combined_dataframes.loc[invalid_rows,columns_to_swap] = None

        combined_dataframes['game_date'] = pd.to_datetime(combined_dataframes['game_date'],errors='coerce')
        combined_dataframes['last_updated'] = pd.to_datetime(combined_dataframes['last_updated'],errors='coerce')
        combined_dataframes['url'] = combined_dataframes['url'].astype(str).str.strip()
        combined_dataframes['game_id'] = combined_dataframes['game_id'].str.lstrip('https://www.nba.com/game/')

        num_columns = ['FGM', 'FGA', 'FG%', '3PM', '3PA', '3P%', 'FTM', 'FTA', 'FT%', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TO', 'PF', 'PTS', 'plus_mins']
        combined_dataframes[num_columns] = combined_dataframes[num_columns].apply(pd.to_numeric, errors='coerce')


        pandas_gbq.to_gbq(combined_dataframes,project_id= 'miscellaneous-projects-444203',destination_table= f'miscellaneous-projects-444203.capstone_data.{url}',if_exists='replace')

