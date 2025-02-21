from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from bs4 import BeautifulSoup
from datetime import datetime as date
from datetime import timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
from selenium.webdriver import Remote
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

import os
import smtplib
import regex as re
import time
import pandas as pd
import signal
import psutil

def establish_driver(local = False):
    if not local: 
        options = Options()
        options.binary_location = '/usr/bin/firefox'
        # options.add_argument("--headless")
        geckodriver_path = '/usr/local/bin/geckodriver'
        service = Service(executable_path=geckodriver_path, log_path="geckodriver.log")
        # options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")
        driver = webdriver.Firefox(service = service,options = options)
        driver.set_window_size(1920, 1080)

        return driver
    else: 
        options = Options()
        # options.add_argument("--headless")
        driver = webdriver.Firefox(options=options)
        driver.set_window_size(1920, 1080)

        return driver

def terminate_firefox_processes(): #Used for memory efficieny
    """
    Forcefully terminates all lingering Firefox & Geckodriver processes.
    """
    for process in psutil.process_iter(attrs=['pid', 'name']): #gathers all the current active signals
        try:
            if process.info['name'].lower() in ('firefox-bin', 'geckodriver','firefox.exe'): 
                os.kill(process.info['pid'], signal.SIGTERM)  #sends termination signal
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass

#Select all option only works when at least half screen due to blockage of the all option when not in headerless option

def select_all_option(driver):
    time.sleep(5)
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

def gather_data(rows,current = True,scrape_date = date.today()):
    game_data =[]
    unique_game_id = set()

    for idx,row in enumerate(rows):
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



def process_page(page,game_id,game_date,home,away):
    driver = establish_driver()
    try:
        driver.get(page)
        
        driver.set_page_load_timeout(120)

        WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, 'StatsTable_st__g2iuW'))
        )

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
                    name_element = row.find('td')
                    if name_element and name_element.find('span', class_="GameBoxscoreTablePlayer_gbpNameFull__cf_sn"):
                        name_span = name_element.find('span', class_="GameBoxscoreTablePlayer_gbpNameFull__cf_sn")
                        player_name = name_span.get_text(strip=True)
                        player_name = player_name.replace('.','')
                    else:
                        player_name = "Unknown"
                    row_data = [player_name] + [col.get_text(strip=True) for col in cols[1:]]
                    row_data.extend([t1,game_id,game_date,t2,page,last_updated])
                    data.append(row_data)
    
                # Create a DataFrame for this table
                if headers and data:
                    headers = ['player'] + headers[1:]
                    headers.extend(['team','game_id','game_date','matchup','url','last_updated'])
                    df = pd.DataFrame(data, columns=headers)
                else:
                    print(f'Could not process: {page}')
                    driver.quit()
                    return page,game_id,game_date,home,away
                
                df_data.append(df)
            
            df = pd.concat(df_data,ignore_index=True)
                # Append the DataFrame to the appropriate team entries in the dictionary
            return df
        else:
            print(f'Could not process: {page}')
            return page,game_id,game_date,home,away
    finally:
        driver.quit()





def prepare_for_gbq(combined_dataframes):
    valid_time_pattern = r"^\d{1,2}:\d{1,2}$" 
    combined_dataframes.rename(columns={'+/-':'plus_mins'},inplace=True)

    invalid_rows = ~combined_dataframes['MIN'].str.match(valid_time_pattern)

    columns_to_swap = ['FGM','FGA','FG%','3PM','3PA','3P%'] #Adding these columns with the presence of NA columns caused misplaced values
    valid_columns = ['team','game_id','game_date','matchup','url','last_updated']

    combined_dataframes.loc[invalid_rows, valid_columns] = combined_dataframes.loc[invalid_rows, columns_to_swap].values

    combined_dataframes.loc[invalid_rows,columns_to_swap] = None #Filling with NA's

    combined_dataframes['last_updated'] = pd.to_datetime(combined_dataframes['last_updated'],errors='coerce').dt.tz_localize('UTC').dt.tz_convert('America/Los_Angeles') #Forcing to PST
    combined_dataframes['url'] = combined_dataframes['url'].astype(str).str.strip() #Collecting Game URLS
    combined_dataframes['game_id'] = combined_dataframes['game_id'].str.lstrip('https://www.nba.com/game/') #Gettting game_ids

    num_columns = ['FGM', 'FGA', 'FG%', '3PM', '3PA', '3P%', 'FTM', 'FTA', 'FT%', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TO', 'PF', 'PTS', 'plus_mins'] #Selecting numeric columnc to force to numeric
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

def convert_date(date_str):
    try:
        # Parse the given date string (without a year)
        date_obj = date.strptime(date_str, "%a, %b %d")

        # Determine the correct year
        assumed_year = 2024 if date_obj.month >= 10 else 2025  # ESeason starts in october meaning everything greater than 10 is 2024

        # Assign the determined year
        date_obj = date_obj.replace(year=assumed_year)

        #Return as date type
        return date_obj.date()
    except ValueError as e:
        print(f"Skipping invalid date: {date_str} - {e}")
        return None

def process_all_pages(pages_info,max_threads):
    """
    Processes multiple pages concurrently using ThreadPoolExecutor with tqdm progress tracking.
    :param pages_info: List of tuples (page_url, game_id, game_date, home, away)
    """
    all_dataframes = []
    retries = {}  # Dictionary to track retries per page
    remaining_pages = list(pages_info)  # Start with the full list of pages

    while remaining_pages:
        total_pages = len(remaining_pages)
        failed_pages = []  # Reset failed pages for this iteration

        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            future_to_page = {executor.submit(process_page, *args): args for args in remaining_pages}


            for future in as_completed(future_to_page):
                args = future_to_page[future]  # Get page details
                start_time = time.time()
                try:
                    result = future.result()  # Attempt processing
                    process_time = time.time() - start_time
                    if isinstance(result, pd.DataFrame):
                        all_dataframes.append(result)  # Successful page
                        print(f'Processed at time: {process_time}')
                    else:
                        raise Exception("Processing failed")  # Handle failure
                except Exception:
                    game_id, game_date, home, away = args[1:5]  # Extract identifiers
                    key = (game_id, game_date, home, away)

                    # Track retries
                    if key in retries:
                        retries[key] += 1
                    else:
                        retries[key] = 1
                    failed_pages.append(args)


        remaining_pages = failed_pages  # Reattempt only failed pages
        executor.shutdown(wait=True)
    return pd.concat(all_dataframes, ignore_index=True) if all_dataframes else None




#Makes it so we are not connecting to driver on import
if __name__ == "__main__":
    driver = establish_driver()
    select_all_option(driver)
