import pandas as pd
from google.cloud import bigquery
import pandas_gbq
import main
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException 
from selenium import webdriver
from bs4 import BeautifulSoup


# driver = main.establish_driver()
driver = webdriver.Firefox()

url = 'https://sportsbook.draftkings.com/nba-player-props?category=player-points&subcategory=points-o%2Fu'

driver.get(url)

soup = BeautifulSoup()

rows = driver.find_elements(By.XPATH, "//tbody[@class='sportsbook-table__body']/tr")

for row in rows:
    print(row)