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


def select_all_option():
    try:
        # Click the dropdown
        dropdown = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div[2]/div[2]/div[3]/section[2]/div/div[2]/div[2]/div[1]/div[3]/div/label"))
        )
        dropdown.click()

        # Click the "All" option
        all_option = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div[2]/div[2]/div[3]/section[2]/div/div[2]/div[2]/div[1]/div[3]/div/label/div/select/option[1]"))
        )
        all_option.click()

        print("Successfully selected the 'All' option.")
    except Exception as e:
        print(f"Error selecting the 'All' option: {e}")

def process_page(page,game_id,game_date):

    driver.get(f'{page}')
    
    driver.set_page_load_timeout(120)
    driver.implicitly_wait(10)

    ps = driver.page_source
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
        
    else:
        print(f"No stats tables found for: {page}")
        return game_id,game_date

urls = {'NBA_Season_2021-2022_uncleaned':'https://www.nba.com/stats/teams/boxscores?Season=2021-22',
        'NBA_Season_2022-2023_uncleaned':'https://www.nba.com/stats/teams/boxscores?Season=2022-23',
        'NBA_Season_2023-2024_uncleaned':'https://www.nba.com/stats/teams/boxscores?Season=2023-24'}

for url in urls:

    driver = webdriver.Firefox()

    driver.get(urls[url])
    select_all_option()
    source = driver.page_source


    soup = BeautifulSoup(source, 'html5lib')

    text = soup.find_all('a',class_ = 'Anchor_anchor__cSc3P')

    # page_number = driver.find_element(By.XPATH,'/html/body/div[1]/div[2]/div[2]/div[3]/section[2]/div/div[2]/div[2]/div[1]/div[4]').text
    # page_number = page_number.split()[1]
    # number_of_pages = int(page_number)


    href = [str(h.get('href')) for h in text if '/game' in h.get('href')]

    dates = [re.findall('\/games\?date=\d{4}-\d{2}-\d{2}',h) for h in href]

    flat_dates = [item for sublist in dates for item in sublist]

    matches = [re.findall('\/game\/[0-9]+',h) for h in href]

    flat_matches = [item for sublist in matches for item in sublist]


    data = []
    failed_pages = []
    i = 0
    for game_id,game_date in zip(flat_matches,flat_dates):
        page = f'https://www.nba.com{game_id}/box-score'
        i += 1
        if i %100 == 0:
            print(f'processing the {i} request {round(len(data)/len(flat_matches)*100,2)}% complete')
        result = process_page(page,game_id,game_date)
        if isinstance(result, pd.DataFrame):
            data.append(result)
        else:
            failed_pages.append(result)
            print(f'Failed Pages lenght: {len(failed_pages)}')

    for count,(game_id,game_date) in enumerate(failed_pages):
        print(f'processing # {count} from failed pages {round(count/len(failed_pages)*100)}% complete')
        page = f'https://www.nba.com{game_id}/box-score'
        result = process_page(page,game_id,game_date)

        data.append(result)

    combined_dataframes = pd.concat(data,ignore_index= True)

    client = bigquery.Client('miscellaneous-projects-444203')

    combined_dataframes.columns

    combined_dataframes.rename(columns={'+/-':'plus_mins'},inplace=True)
    pgbq.to_gbq(combined_dataframes,project_id= 'miscellaneous-projects-444203',destination_table= f'miscellaneous-projects-444203.capstone_data.{url}')