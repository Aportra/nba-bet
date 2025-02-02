#!/home/aportra99/venv/bin/activate
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from datetime import datetime as date
from google.oauth2 import service_account
from tqdm import tqdm

import pandas as pd
import utils
import pandas_gbq
import time

nba_teams = [
    "ATL", "BOS", "BKN", "CHA", "CHI", "CLE", "DAL", "DEN", "DET", "GSW",
    "HOU", "IND", "LAC", "LAL", "MEM", "MIA", "MIL", "MIN", "NO", "NYK",
    "OKC", "ORL", "PHI", "PHX", "POR", "SAC", "SAS", "TOR", "UTAH", "WSH"
]

def scrape_team_schedule(nba_teams):
    driver = utils.establish_driver(local = True)
    url = 'https://www.espn.com/nba/team/schedule/_/name/'


    scrape_date = date.today()

    # credentials = service_account.Credentials.from_service_account_file('/home/aportra99/scraping_key.json')
    all_data = []
    with tqdm(total=(len(nba_teams)), desc=f"Processing", ncols=80) as pbar:
        for team in nba_teams:
            team_url = url+team



            driver.get(team_url)

            #Waiting for webpage to load
            time.sleep(10)
            WebDriverWait(driver, 300).until(
                EC.presence_of_all_elements_located((By.XPATH, "//tbody[@class='Table__TBODY']")) 
            )

            #Gathering the rows in the table
            rows = driver.find_elements(By.XPATH, "//tbody[@class='Table__TBODY']/tr")
            data = []
        
            for row in rows[2:]: #skipping headers
                print

                date_element = row.find_element(By.XPATH,"./td[1]/span")
                date_text = date_element.text

                if date_text.upper() == "DATE": #skipping header in the middle of table
                    continue
                converted_date = utils.convert_date(date_text)
                if converted_date > date.today().date(): #Game played based on date of game
                    game_played = 0
                else:
                    game_played = 1


                opponent_element = row.find_element(By.XPATH,"./td[2]/div/span[3]/a")
                opponent_text = opponent_element.text

                if '@' in opponent_text:
                    away = 1
                    home = 0
                else:
                    away = 0
                    home = 1

                opponent_text = opponent_text.split(r'@|vs')[0].strip()

                data.append({
                    'team':team,
                    'date':converted_date,
                    'opponent':opponent_text,
                    'scrape_date':scrape_date,
                    'home':home,
                    'away': away
                })
            pbar.update(1)
            all_data.extend(data)
    combined_data = pd.DataFrame(all_data)
    combined_data['date'] = combined_data['date'].astype(str)
    
    all_nba_teams = {
    "Atlanta": "ATL",
    "Boston": "BOS",
    "Brooklyn": "BKN",
    "Charlotte": "CHA",
    "Chicago": "CHI",
    "Cleveland": "CLE",
    "Dallas": "DAL",
    "Denver": "DEN",
    "Detroit": "DET",
    "Golden State": "GSW",
    "Houston": "HOU",
    "Indiana": "IND",
    "Los Angeles": "LAL",  # Lakers
    "LA": "LAC",
    "Memphis": "MEM",
    "Miami": "MIA",
    "Milwaukee": "MIL",
    "Minnesota": "MIN",
    "New Orleans": "NOP",
    "New York": "NYK",
    "Oklahoma City": "OKC",
    "Orlando": "ORL",
    "Philadelphia": "PHI",
    "Phoenix": "PHX",
    "Portland": "POR",
    "Sacramento": "SAC",
    "San Antonio": "SAS",
    "Toronto": "TOR",
    "Utah": "UTA",
    "Washington": "WAS"
    }

    # combined_data['opponent'] = combined_data['opponent'].astype(str).str.strip().str.title()
    # all_nba_teams_fixed = {team: abbr[0] for team, abbr in all_nba_teams.items()}

    combined_data['opponent'] = combined_data['opponent'].replace(all_nba_teams)

    combined_data['team'] = combined_data['team'].replace({'NO':'NOP','UTAH':'UTA','WSH':'WAS'})

    pandas_gbq.to_gbq(combined_data,project_id= 'miscellaneous-projects-444203',destination_table= f'miscellaneous-projects-444203.capstone_data.schedule',if_exists = 'replace',table_schema= [{'name':'date','type':'DATE'},])

    driver.quit()

scrape_team_schedule(nba_teams)


