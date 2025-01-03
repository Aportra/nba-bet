import pandas as pd
import main
from datetime import datetime as date
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
    main.select_all_option(driver)
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