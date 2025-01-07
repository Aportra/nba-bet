import pandas as pd
from google.cloud import bigquery
import pandas_gbq
from ... import main
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException 
from selenium import webdriver
from bs4 import BeautifulSoup
from google.oauth2 import service_account
from datetime import datetime as date
import traceback
from selenium.webdriver.firefox.options import Options

driver = main.establish_driver()
# options = Options()
# options.add_argument('--headless')

# driver = webdriver.Firefox(options=options)

url = 'https://sportsbook.draftkings.com/nba-player-props?category=player-points&subcategory=points-o%2Fu'

scrape_date = date.today()

credentials = service_account.Credentials.from_service_account_file('/home/aportra99/scraping_key.json')

driver.get(url)



WebDriverWait(driver, 300).until(
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
            'Points': points_value,
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
        main.send_email(
            subject="NBA SCRAPING: SCRIPT CRASHED",
            body=error_message
        )



combined_data = pd.DataFrame(data)

pandas_gbq.to_gbq(combined_data,project_id= 'miscellaneous-projects-444203',destination_table= f'miscellaneous-projects-444203.capstone_data.player_odds',if_exists = 'append',credentials=credentials)

main.send_email(
subject = str(f"ODDS SCRAPING: COMPLTETED # OF PLAYERS {len(data)}"),
body = str(f'{len(data)} players odds scraped as of {scrape_date.date()}')
)

driver.quit()