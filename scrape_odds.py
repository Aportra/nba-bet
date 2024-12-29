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

WebDriverWait(driver, 10).until(
    EC.presence_of_all_elements_located((By.XPATH, "//tbody[@class='sportsbook-table__body']/tr"))
)


rows = driver.find_elements(By.XPATH, "//tbody[@class='sportsbook-table__body']/tr")

for row in rows:
    print
    try:
        name_element = row.find_element(By.XPATH,"./th/div/div[1]/a/span")
        name = name_element.text
        print(name)
    except Exception as e:
        print(f"Error processing row: {e}")

driver.quit()
