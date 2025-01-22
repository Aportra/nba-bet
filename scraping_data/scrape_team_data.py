import pandas as pd
import scraping_data.utils as utils
from datetime import datetime as date
from datetime import timedelta
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from bs4 import BeautifulSoup
from google.cloud import bigquery
import regex as re
import time
import pandas_gbq
import traceback
from google.oauth2 import service_account

def scrape_current_team_data(length):
    scrape_date = date.today() - timedelta(1)

    credentials = service_account.Credentials.from_service_account_file('/home/aportra99/scraping_key.json') #For Google VM

    urls = {
            '2024-2025_team_ratings':'https://www.nba.com/stats/teams/boxscores-advanced?Season=2024-25'
    }


    # options = Options()

    # options.add_argument("--headless")

    # driver = webdriver.Firefox(options=options)
    # driver.set_window_size(1920, 1080)
    # ps = driver.page_source

    driver = utils.establish_driver()


    scrape_date = date.today() - timedelta(1)
    try:
        for url in urls:

            driver.get(urls[url]) 
            time.sleep(5)
            utils.select_all_option(driver)
            source = driver.page_source
            #For each row collect game_date,game_id, and matchup
            table_element = driver.find_element(By.XPATH, "//table[contains(@class, 'Crom_table__p1iZz')]")
            rows = table_element.find_elements(By.XPATH, ".//tr")

            headers = [th.text.strip() for th in rows[0].find_elements(By.XPATH,'.//th')] if rows else []
            headers = [header.lower() for header in headers]
            headers.extend(['game_id','home','away','last_updated'])
            game_data =[]

            for idx,row in enumerate(rows[1:-1]):
                if (idx+1) % 10 == 0:
                    print(f'{round((idx+1)/len(rows)*100,2)}% gathered')
                cols = row.find_elements(By.XPATH, ".//td")

                date_element = row.find_element(By.XPATH, ".//td[3]/a")
                game_date_text = date_element.text.strip()

                # Convert the extracted date text to a datetime.date object
                # First, try parsing with the expected format
                game_date = date.strptime(game_date_text, "%m/%d/%Y").date()
                if game_date < scrape_date.date():
                    break
                #Get matchup data
                matchup_element = row.find_element(By.XPATH, ".//td[2]/a")
                game_id = matchup_element.get_attribute('href')
                matchup_text = matchup_element.text.strip()
                matchup_element.get_attribute('')
                if "@" in matchup_text:
                    matchup = matchup_text.split(" @ ")
                    home_binary = 0
                    away_binary = 1
                elif "vs." in matchup_text:
                    matchup = matchup_text.split(" vs. ")
                    home_binary = 1
                    away_binary = 0

                row_data = [col.text.strip() for col in cols]
                row_data.extend([game_id,home_binary,away_binary,scrape_date])
                

                game_data.append(row_data)

            if len(game_data) < length * 2 and len(game_data) != 0:
                driver.quit()
                utils.send_email(
                subject = "NEEDED TO RESTART TEAM DATA SCRIPT",
                body = str(f'RESTARTING SCRIPT AS {date.today()}'))
                return scrape_current_team_data(length)
            data = pd.DataFrame(game_data,columns = headers)

            data.rename(columns={'w/l':'win_loss','ast/to':'ast_to','ast\nratio':'ast_ratio'},inplace=True)


            string_columns = ['home','away','game_id','win_loss','team','match up','game date','last_updated']
            
            data['game date'] = pd.to_datetime(data['game date'])
            
            for column in data.columns:
                if column not in string_columns:
                    data[column] = data[column].astype('float64')

            pandas_gbq.to_gbq(data,project_id= 'miscellaneous-projects-444203',destination_table= f'miscellaneous-projects-444203.capstone_data.{url}',if_exists='append',credentials=credentials)
            #pandas_gbq.to_gbq(combined_dataframes,project_id= 'miscellaneous-projects-444203',destination_table= f'miscellaneous-projects-444203.capstone_data.NBA_Season_2024-2025_uncleaned',if_exists = 'append',credentials=credentials)
        if (len(game_data)) > 0:
            utils.send_email(
            subject = str(f"TEAM RATINGS SCRAPING: COMPLTETED # OF GAMES {len(game_data)}"),
            body = str(f'{len(game_data)} games scraped as of {scrape_date.date()}')
            )
        else:
            utils.send_email(
            subject = "TEAM RATINGS SCRAPING: NO GAMES",
            body = str(f'No games as of {scrape_date.date()}'))
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

def scrape_past_team_data():
    

    urls = {'2021-2022_team_ratings':'https://www.nba.com/stats/teams/boxscores-advanced?Season=2021-22',
            '2022-2023_team_ratings':'https://www.nba.com/stats/teams/boxscores-advanced?Season=2022-23',
            '2023-2024_team_ratings':'https://www.nba.com/stats/teams/boxscores-advanced?Season=2023-24',
            '2024-2025_team_ratings':'https://www.nba.com/stats/teams/boxscores-advanced?Season=2024-25'
    }
    scrape_date = date.today() - timedelta(1)

    options = Options()

    options.add_argument("--headless")

    driver = webdriver.Firefox(options=options)
    driver.set_window_size(1920, 1080)
    ps = driver.page_source

    for url in urls:

        driver.get(urls[url]) 
        time.sleep(5)
        utils.select_all_option(driver)
        source = driver.page_source
        #For each row collect game_date,game_id, and matchup
        table_element = driver.find_element(By.XPATH, "//table[contains(@class, 'Crom_table__p1iZz')]")
        rows = table_element.find_elements(By.XPATH, ".//tr")

        headers = [th.text.strip() for th in rows[0].find_elements(By.XPATH,'.//th')] if rows else []
        headers = [header.lower() for header in headers]
        headers.extend(['game_id','home','away','last_updated'])
        game_data =[]
        
        for idx,row in enumerate(rows[1:-1]):
            if (idx+1) % 10 == 0:
                print(f'{round((idx+1)/len(rows)*100,2)}% gathered')
            cols = row.find_elements(By.XPATH, ".//td")
            #Get matchup data
            matchup_element = row.find_element(By.XPATH, ".//td[2]/a")
            game_id = matchup_element.get_attribute('href')
            matchup_text = matchup_element.text.strip()
            matchup_element.get_attribute('')
            if "@" in matchup_text:
                matchup = matchup_text.split(" @ ")
                home_binary = 0
                away_binary = 1
            elif "vs." in matchup_text:
                matchup = matchup_text.split(" vs. ")
                home_binary = 1
                away_binary = 0

            row_data = [col.text.strip() for col in cols]
            row_data.extend([game_id.lstrip('https://www.nba.com/game/'),home_binary,away_binary,scrape_date])
            

            game_data.append(row_data)

        data = pd.DataFrame(game_data,columns = headers)

        data.rename(columns={'w/l':'win_loss','ast/to':'ast_to','ast\nratio':'ast_ratio'},inplace=True)

        string_columns = ['home','away','game_id','win_loss','team','match up','game date','last_updated']
        
        data['game date'] = pd.to_datetime(data['game date'])

        for column in data.columns:
            if column not in string_columns:
                data[column] = data[column].astype('float64')

        
        pandas_gbq.to_gbq(data,project_id= 'miscellaneous-projects-444203',destination_table= f'miscellaneous-projects-444203.capstone_data.{url}',if_exists='replace')


    driver.quit()

