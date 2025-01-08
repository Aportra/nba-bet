#!/home/aportra99/venv/bin/activate
import pandas as pd
import scraping_data.utils as utils
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException 
from selenium.webdriver.firefox.options import Options
from bs4 import BeautifulSoup
from datetime import timedelta
from datetime import datetime as date
from google.cloud import bigquery
import regex as re
import pandas_gbq
import traceback
from google.oauth2 import service_account

def scrape_current_games():

    try:
        credentials = service_account.Credentials.from_service_account_file('/home/aportra99/scraping_key.json') #For Google VM
        local = False
    except FileNotFoundError:
        print("File not found continuing as if on local")
        local = True

    driver = utils.establish_driver(local)


    scrape_date = date.today() - timedelta(1)
    url = {'NBA_Season_2024-2025_uncleaned':'https://www.nba.com/stats/teams/boxscores?Season=2024-25'}

    driver.get(url['NBA_Season_2024-2025_uncleaned'])


    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")


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
    game_data = utils.gather_data(rows)

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

            combined_dataframes = utils.prepare_for_gbq(combined_dataframes)
            client = bigquery.Client('miscellaneous-projects-444203',credentials= credentials)

            
            pandas_gbq.to_gbq(combined_dataframes,project_id= 'miscellaneous-projects-444203',destination_table= f'miscellaneous-projects-444203.capstone_data.NBA_Season_2024-2025_uncleaned',if_exists = 'append',credentials=credentials)
            
            #pandas_gbq.to_gbq(combined_dataframes,project_id= 'miscellaneous-projects-444203',destination_table= f'miscellaneous-projects-444203.capstone_data.test',if_exists = 'append',credentials=credentials)
            utils.send_email(
            subject = str(f"Test NBA SCRAPING: COMPLTETED # OF GAMES {len(game_data)}"),
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

    driver = utils.establish_driver(local = True)

    for url in urls:

        driver.get(urls[url])
        utils.select_all_option(driver)
        source = driver.page_source

        #For each row collect game_date,game_id, and matchup
        rows = driver.find_elements(By.XPATH, "//tbody[@class='Crom_body__UYOcU']/tr")

        game_data = utils.gather_data(rows,current= False)
    
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

        combined_dataframes = utils.prepare_for_gbq(combined_dataframes)
        client = bigquery.Client('miscellaneous-projects-444203')


        pandas_gbq.to_gbq(combined_dataframes,project_id= 'miscellaneous-projects-444203',destination_table= f'miscellaneous-projects-444203.capstone_data.{url}',if_exists='replace')

