from google.cloud import bigquery
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium import webdriver
from google.oauth2 import service_account
from datetime import datetime as date
from selenium.webdriver.firefox.options import Options
from bs4 import BeautifulSoup

import time
import traceback
import pandas_gbq
import utils
import pandas as pd

driver = utils.establish_driver()

urls = {'points':'https://sportsbook.draftkings.com/nba-player-props?category=player-points&subcategory=points-o%2Fu',
       'rebounds':'https://sportsbook.draftkings.com/nba-player-props?category=player-rebounds&subcategory=rebounds-o%2Fu',
       'assists':'https://sportsbook.draftkings.com/nba-player-props?category=player-assists&subcategory=assists-o%2Fu',
        'steals':'https://sportsbook.draftkings.com/nba-player-props?category=player-defense&subcategory=steals-o%2Fu',
        'blocks':'https://sportsbook.draftkings.com/nba-player-props?category=player-defense&subcategory=blocks-o%2Fu',
        'threes_made':'https://sportsbook.draftkings.com/nba-player-props?category=player-threes&subcategory=threes-o%2Fu'}

scrape_date = date.today()

try:
    credentials = service_account.Credentials.from_service_account_file('/home/aportra99/scraping_key.json')
    default_creds = False
    print("Credentials Loaded")
except FileNotFoundError:
    default_creds = True
    print("No File to Load Using Default Credentials")

def process_categories(category):
    global driver
    try:
        driver.get(urls[category])
        driver.implicitly_wait(10)
        WebDriverWait(driver, 30).until(
            EC.presence_of_all_elements_located((By.XPATH, "//tbody[@class='sportsbook-table__body']/tr"))
        )


        rows = driver.find_elements(By.XPATH, "//tbody[@class='sportsbook-table__body']/tr")
        data = []
        for row in rows:
            print
            try:
                name_element = row.find_element(By.XPATH,"./th/div/div[1]/a/span")
                name = name_element.text

                over_element = row.find_element(By.XPATH,"./td[1]")
                over = over_element.text
                under_element = row.find_element(By.XPATH,"./td[2]")
                under = under_element.text

                over = over.split()
                under = under.split()

                over_value = over[2]
                under_value = under[2]
                points_value = over[1]

                data.append({
                    'Player': name,
                    f'{category}': points_value,
                    'Over': over_value,
                    'Under': under_value,
                    'Date_Updated':scrape_date
                })
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



        combined_data = pd.DataFrame(data)
        if not default_creds:
            pandas_gbq.to_gbq(combined_data,project_id= 'miscellaneous-projects-444203',destination_table= f'miscellaneous-projects-444203.capstone_data.player_{category}_odds',if_exists = 'append',credentials=credentials)
        else:
            pandas_gbq.to_gbq(combined_data,project_id= 'miscellaneous-projects-444203',destination_table= f'miscellaneous-projects-444203.capstone_data.player_{category}_odds',if_exists = 'append')

        # utils.send_email(
        # subject = str(f"{category} ODDS SCRAPING: COMPLTETED # OF PLAYERS {len(data)}"),
        # body = str(f'{len(data)} players odds scraped as of {scrape_date.date()}')
        # )
        print('job completed') 
    except Exception as e:
        # utils.send_email(
        # subject = str(f"{category} failed processing retrying"),
        # body = str(f'Retrying scraping script for {category}')
        # )
        try:
            driver.quit()
        except:
            pass

        driver = utils.establish_driver()
        process_categories(category)
        
        
for category in urls:
    process_categories(category)

driver.quit()
