import pandas as pd
import main
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from google.cloud import bigquery
import regex as re
import time
from datetime import datetime

driver = main.establish_driver()


nba_games = 'https://www.nba.com/stats/teams/boxscores?Season=2024-25'

driver.get(nba_games)

page_source = driver.page_source

soup = BeautifulSoup(page_source,'html5lib')

source = driver.page_source


soup = BeautifulSoup(source, 'html5lib')

driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
time.sleep(3) 
WebDriverWait(driver, 10).until(
    EC.presence_of_all_elements_located((By.XPATH, "/html/body/div[1]/div[2]/div[2]/div[3]/section[2]/div/div[2]/div[3]/table/tbody"))
)

# Step 3: Use Selenium to locate rows
rows = driver.find_elements(By.XPATH, "/html/body/div[1]/div[2]/div[2]/div[3]/section[2]/div/div[2]/div[3]/table/tbody/tr")
print(f"Found {len(rows)} rows.")  # Debugging: Number of rows found

today = datetime.today()
games = []


for row in rows[1:2]:
    try:
        link_tag = row.find_element(By.XPATH, ".//a[contains(@class, 'Anchor_anchor__cSc3P')]")
        if link_tag:
            game_url = f"https://www.nba.com{link_tag.get_attribute('href')}" 
            date_element = row.find_element(By.XPATH, "./td[3]/a") # Full game URL
            game_date_text = date_element.text.strip()  # Extract the clean date
            game_date = datetime.strptime(game_date_text, "%m/%d/%Y").date()  # Convert to date object
            # print(game_date)
            # Filter games by today's date

        matchup_element = row.find_element(By.XPATH, "./td[2]/a")
        matchup_text = matchup_element.text.strip()  # Extracts "CHI @ TOR" or "CHI vs. TOR"

        if "@" in matchup_text:
            teams = matchup_text.split(" @ ")
        elif "vs." in matchup_text:
            teams = matchup_text.split(" vs. ")
        print(teams[1])
        if game_date == today:
            games.append({"url": game_url, "game_date": game_date})
    except Exception as e:
        print(f"Error parsing row: {e}")

        
text = soup.find_all('a',class_ = 'Anchor_anchor__cSc3P')

href = [str(h.get('href')) for h in text if '/game' in h.get('href')]

dates = [re.findall('\/games\?date=\d{4}-\d{2}-\d{2}',h) for h in href]

flat_dates = [item for sublist in dates for item in sublist]

matches = [re.findall('\/game\/[0-9]+',h) for h in href]

flat_matches = [item for sublist in matches for item in sublist]

# Initialize the dictionary to store data
data = []

