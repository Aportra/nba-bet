#!/home/aportra99/venv/bin/activate
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException 
from selenium.webdriver.firefox.options import Options
from bs4 import BeautifulSoup
from datetime import timedelta
from datetime import datetime as date
from google.cloud import bigquery
from google.oauth2 import service_account
from tqdm import tqdm

import pandas as pd
import utils
import regex as re
import pandas_gbq
import traceback
import time

driver = utils.establish_driver(local = True)
url = 'https://www.espn.com/nba/team/schedule/_/name/'

nba_teams = [
    "ATL", "BOS", "BKN", "CHA", "CHI", "CLE", "DAL", "DEN", "DET", "GSW",
    "HOU", "IND", "LAC", "LAL", "MEM", "MIA", "MIL", "MIN", "NO", "NYK",
    "OKC", "ORL", "PHI", "PHX", "POR", "SAC", "SAS", "TOR", "UTAH", "WSH"
]
scrape_date = date.today()

credentials = service_account.Credentials.from_service_account_file('/home/aportra99/scraping_key.json')
all_data = []
for team in nba_teams:
    team_url = url+team



    driver.get(team_url)

    time.sleep(10)
    WebDriverWait(driver, 1000).until(
        EC.presence_of_all_elements_located((By.XPATH, "//tbody[@class='Table__TBODY']"))
    )

    print('Webpage loaded')

    rows = driver.find_elements(By.XPATH, "//tbody[@class='Table__TBODY']/tr")
    data = []
    with tqdm(total=len(rows), desc=f"Processing team {team}", ncols=80) as pbar:
        for row in rows[2:]:
            print

            date_element = row.find_element(By.XPATH,"./td[1]/span")
            date_text = date_element.text

            if date_text.upper() == "DATE":
                continue
            converted_date = utils.convert_date(date_text)
            if converted_date > date.today().date():
                game_played = 0
            else:
                game_played = 1


            opponent_element = row.find_element(By.XPATH,"./td[2]/div/span[3]/a")
            opponent_text = opponent_element.text
            data.append({
                'team':team,
                'date':converted_date,
                'opponenent':opponent_text,
                'scrape_date':scrape_date,
                'game_played':game_played
            })
            pbar.update(1)
    all_data.extend(data)
combined_data = pd.DataFrame(all_data)
combined_data['date'] = combined_data['date'].astype(str)


pandas_gbq.to_gbq(combined_data,project_id= 'miscellaneous-projects-444203',destination_table= f'miscellaneous-projects-444203.capstone_data.schedule',if_exists = 'replace',credentials=credentials,table_schema= [{'name':'date','type':'DATE'},])

driver.quit()

