import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from bs4 import BeautifulSoup
import regex as re
import time
from datetime import datetime as date
from datetime import timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
import os
from selenium.webdriver import Remote



def establish_driver(local = False):
    if not local: 
        options = Options()
        options.binary_location = '/usr/bin/firefox'
        options.add_argument("--headless")
        geckodriver_path = '/usr/local/bin/geckodriver'
        service = Service(executable_path=geckodriver_path, log_path="geckodriver.log")
        driver = webdriver.Firefox(service = service,options = options)
        
        return driver
    else: 
        options = Options()
        options.add_argument("--headless")
        driver = webdriver.Firefox(options=options)
        driver.set_window_size(1920, 1080)

        return driver

#Select all option only works when at least half screen due to blockage of the all option when not in headerless option

def select_all_option(driver):
    try:
        # Click the dropdown

        dropdown = WebDriverWait(driver,10).until(
            EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div[2]/div[2]/div[3]/section[2]/div/div[2]/div[2]/div[1]/div[3]/div/label"))
        )
        driver.execute_script("arguments[0].click();", dropdown)
        # Click the "All" option
        all_option = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div[2]/div[2]/div[3]/section[2]/div/div[2]/div[2]/div[1]/div[3]/div/label/div/select/option[1]"))
        )
        all_option.click()

        print("Successfully selected the 'All' option.")
    except Exception as e:
        print(f"Error selecting the 'All' option: {e}")

def gather_data(rows,current = True,scrape_date = date.today() - timedelta(1)):
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
        if current:
            if game_date.date() < scrape_date.date():
                break
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
    return game_data



def process_page(page,game_id,game_date,home,away,driver):
    
    driver.get(page)
    
    driver.set_page_load_timeout(120)
    driver.implicitly_wait(10)

    ps = driver.page_source
    soup = BeautifulSoup(ps, 'html5lib')
    # Find all divs containing the data tables
    tables = soup.find_all('div', class_='StatsTable_st__g2iuW')
    last_updated = date.today()
    df_data = []
    # Check if tables exist
    if tables:
        for table_index, table in enumerate(tables):
            # Extract the table rows
            rows = table.find_all('tr')
            if table_index == 1:
                t1 = home
                t2 = away
            else:
                t1 = away
                t2 = home
            # Get the header row (if it exists)
            headers = [th.get_text(strip=True) for th in rows[0].find_all('th')] if rows else []
            
            # Get the data rows
            data = []
            
            for row in rows[1:-1]:  # Skip the header row
                cols = row.find_all('td')
                row_data = [col.get_text(strip=True) for col in cols]
                row_data.extend([t1,game_id,game_date,t2,page,last_updated])
                data.append(row_data)
            
            # Create a DataFrame for this table
            if headers and data:
                headers.extend(['team','game_id','game_date','matchup','url','last_updated'])
                df = pd.DataFrame(data, columns=headers)
            else:
                df = pd.DataFrame(data)  # Use generic column names if no headers
            
            df_data.append(df)
        
        df = pd.concat(df_data,ignore_index=True)
            # Append the DataFrame to the appropriate team entries in the dictionary
        return df
    else:
        print(f'Could not process: {page}')
        return game_id,game_date,home,away

def prepare_for_gbq(combined_dataframes):
    valid_time_pattern = r"^\d{1,2}:\d{1,2}$"
    combined_dataframes.rename(columns={'+/-':'plus_mins'},inplace=True)

    invalid_rows = ~combined_dataframes['MIN'].str.match(valid_time_pattern)

    columns_to_swap = ['FGM','FGA','FG%','3PM','3PA','3P%']
    valid_columns = ['team','game_id','game_date','matchup','url','last_updated']

    combined_dataframes.loc[invalid_rows, valid_columns] = combined_dataframes.loc[invalid_rows, columns_to_swap].values

    combined_dataframes.loc[invalid_rows,columns_to_swap] = None

    combined_dataframes['last_updated'] = pd.to_datetime(combined_dataframes['last_updated'],errors='coerce').date.tz_localize('PST')
    combined_dataframes['url'] = combined_dataframes['url'].astype(str).str.strip()
    combined_dataframes['game_id'] = combined_dataframes['game_id'].str.lstrip('https://www.nba.com/game/')

    num_columns = ['FGM', 'FGA', 'FG%', '3PM', '3PA', '3P%', 'FTM', 'FTA', 'FT%', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TO', 'PF', 'PTS', 'plus_mins']
    combined_dataframes[num_columns] = combined_dataframes[num_columns].apply(pd.to_numeric, errors='coerce')

    for column in combined_dataframes.columns:
        combined_dataframes.rename(columns = {column:column.lower()},inplace = True)
    
    return combined_dataframes


def send_email(subject,body):
    try:
        load_dotenv('/home/aportra99/Capstone/.env')
        print('loaded the .env')
    except:
        print('could not load .env')
    sender_email = os.getenv('SERVER_EMAIL')
    receiver_email = os.getenv('EMAIL_USERNAME')
    password = os.getenv('EMAIL_PASSWORD')
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject
    
    msg.attach(MIMEText(body,'plain'))

    try:
        server = smtplib.SMTP('smtp.gmail.com',587)
        server.starttls()
        server.login(sender_email,password)
        server.send_message(msg)
    except Exception as e:
        print(f"failed due to send email: {e}")
    finally:
        server.quit()




#Makes it so we are not connecting to driver on import
if __name__ == "__main__":
    driver = establish_driver()
    select_all_option(driver)
