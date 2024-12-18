import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from google.cloud import bigquery
import regex as re
import time
import pandas_gbq as pgbq


class scraping:

    def __init__(self):
        self.driver = webdriver.Firefox()



def select_all_option(self):
    try:
        # Click the dropdown
        dropdown = WebDriverWait(self, 10).until(
            EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div[2]/div[2]/div[3]/section[2]/div/div[2]/div[2]/div[1]/div[3]/div/label"))
        )
        dropdown.click()

        # Click the "All" option
        all_option = WebDriverWait(self, 10).until(
            EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div[2]/div[2]/div[3]/section[2]/div/div[2]/div[2]/div[1]/div[3]/div/label/div/select/option[1]"))
        )
        all_option.click()

        print("Successfully selected the 'All' option.")
    except Exception as e:
        print(f"Error selecting the 'All' option: {e}")

def process_page(self,page,game_id,game_date):

    self.driver.get(f'{page}')
    
    self.driver.set_page_load_timeout(120)
    self.driver.implicitly_wait(10)

    ps = self.driver.page_source
    soup = BeautifulSoup(ps, 'html5lib')
    
    # Find all divs containing the data tables
    tables = soup.find_all('div', class_='StatsTable_st__g2iuW')
    
    # Check if tables exist
    if tables:
        for table_index, table in enumerate(tables):
            # Extract the table rows
            rows = table.find_all('tr')
            
            # Get the header row (if it exists)
            headers = [th.get_text(strip=True) for th in rows[0].find_all('th')] if rows else []
            
            # Get the data rows
            data = []
            for row in rows[1:-1]:  # Skip the header row
                cols = row.find_all('td')
                row_data = [col.get_text(strip=True) for col in cols]
                row_data.extend([game_id,game_date])
                data.append(row_data)
            
            # Create a DataFrame for this table
            if headers and data:
                headers.extend(['game_id','game_date'])
                df = pd.DataFrame(data, columns=headers)
            else:
                df = pd.DataFrame(data)  # Use generic column names if no headers
            
            # Append the DataFrame to the appropriate team entries in the dictionary
            return df

