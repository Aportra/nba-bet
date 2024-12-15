import pandas as pd
from selenium import webdriver
from bs4 import BeautifulSoup
from google.cloud import bigquery
import regex as re
import time

driver = webdriver.Firefox()


nba_games = 'https://www.nba.com/games'

driver.get(nba_games)

page_source = driver.page_source

soup = BeautifulSoup(page_source,'html5lib')

links = soup.find_all('a')


hrefs = [link.get('href') for link in links if 'box-score' in link.get('href')]

# Initialize the dictionary to store data
data_l = []

for i in hrefs:
    # Visit the page
    print(f'Processing: {i}')
    driver.get(f'https://www.nba.com{i}')
    
    time.sleep(3)

    h = i

    # Extract team names using regex
    pattern = r'/game/([a-z]{3})-vs-([a-z]{3})-\d+/box-score#box-score'
    match = re.search(pattern, h)

    if not match:
        print(f"Regex did not match for: {h}")
        continue
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
            print(f"Headers for Table {table_index + 1}: {headers}")
            
            # Get the data rows
            data = []
            for row in rows[1:-1]:  # Skip the header row
                cols = row.find_all('td')
                data.append([col.get_text(strip=True) for col in cols])
            
            # Create a DataFrame for this table
            if headers and data:
                df = pd.DataFrame(data, columns=headers)
            else:
                df = pd.DataFrame(data)  # Use generic column names if no headers
            
            # Append the DataFrame to the appropriate team entries in the dictionary
            data_l.append(df)
    else:
        print(f"No stats tables found for: {h}")

        # Print the DataFrame (or save it for later)

for i in data_l:
        print(i)