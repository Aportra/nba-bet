"""Selenium Utility Module for Web Scraping and Automation."""

import os
import time
import smtplib
import signal
import psutil
import pandas as pd
from datetime import datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from webdriver_manager.firefox import GeckoDriverManager
from bs4 import BeautifulSoup
from tqdm import tqdm



def establish_driver(local=False):
    """Establishes a Selenium WebDriver for Chrome.

    Args:
        local (bool): If True, runs WebDriver locally. Otherwise, uses a remote setup.

    Returns:
        webdriver.Chrome: A configured instance of the Chrome WebDriver.
    """
    options = Options()
    options.add_argument("--headless")  # Run without UI
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")  # Prevents GPU-related crashes
    service = Service(GeckoDriverManager.install())
    

    if local:
        chrome_path = "/opt/homebrew/bin/chromedriver"  
        service = Service(executable_path=chrome_path, log_path="chromedriver.log")
        driver = webdriver.Firefox(service=service, options=options)
    else:
        chrome_path = "/opt/homebrew/bin/chromedriver"  
        service = Service(executable_path=chrome_path, log_path="chromedriver.log")
        driver = webdriver.Firefox(service=service,options=options)

    driver.set_window_size(2560, 1440)
    return driver

def terminate_firefox_processes():
    """Forcefully terminates lingering Firefox & Geckodriver processes."""
    for process in psutil.process_iter(attrs=["pid", "name"]):
        try:
            if process.info["name"].lower() in ("firefox-bin", "geckodriver", "firefox.exe"):
                os.kill(process.info["pid"], signal.SIGTERM)  # Terminate process
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass


def select_all_option(driver):
    """Selects the 'All' option in a dropdown menu on a webpage."""
    try:
        dropdown = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH, "/html/body/div[1]/div[2]/div[2]/div[3]/section[2]/div/div[2]/div[2]/div[1]/div[3]/div/label")
            )
        )
        driver.execute_script("arguments[0].click();", dropdown)

        all_option = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH, "/html/body/div[1]/div[2]/div[2]/div[3]/section[2]/div/div[2]/div[2]/div[1]/div[3]/div/label/div/select/option[1]")
            )
        )
        all_option.click()
        print("Successfully selected the 'All' option.")
    except Exception as e:
        print(f"Error selecting the 'All' option: {e}")


def process_page(page, game_id, game_date, home, away):
    """Processes an NBA game page and extracts statistical data.

    Args:
        page (str): The game URL.
        game_id (str): Unique game identifier.
        game_date (datetime.date): Date of the game.
        home (str): Home team.
        away (str): Away team.

    Returns:
        pd.DataFrame: Extracted game data or None if processing fails.
    """
    driver = establish_driver()
    try:
        driver.get(page)
        driver.set_page_load_timeout(120)

        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "StatsTable_st__g2iuW")))

        soup = BeautifulSoup(driver.page_source, "html5lib")
        tables = soup.find_all("div", class_="StatsTable_st__g2iuW")
        last_updated = dt.today()

        df_data = []
        if tables:
            for table_index, table in enumerate(tables):
                rows = table.find_all("tr")
                t1, t2 = (home, away) if table_index == 1 else (away, home)

                headers = [th.get_text(strip=True) for th in rows[0].find_all("th")] if rows else []
                data = []

                for row in rows[1:-1]:
                    cols = row.find_all("td")
                    name_span = row.find("td").find("span", class_="GameBoxscoreTablePlayer_gbpNameFull__cf_sn")
                    player_name = name_span.get_text(strip=True).replace(".", "") if name_span else "Unknown"

                    row_data = [player_name] + [col.get_text(strip=True) for col in cols[1:]]
                    row_data.extend([t1, game_id, game_date, t2, page, last_updated])
                    data.append(row_data)

                if headers and data:
                    headers = ["player"] + headers[1:] + ["team", "game_id", "game_date", "matchup", "url", "last_updated"]
                    df_data.append(pd.DataFrame(data, columns=headers))
                else:
                    print(f"Could not process: {page}")
                    return None

            return pd.concat(df_data, ignore_index=True)
        else:
            print(f"Could not process: {page}")
            return None

    finally:
        driver.quit()

def gather_data(rows, current=True, scrape_date=dt.today()):
    """Extracts game data from table rows.

    Args:
        rows (list): List of Selenium WebElements representing table rows.
        current (bool, optional): If True, filters only current games. Defaults to True.
        scrape_date (datetime.date, optional): The date to compare games against. Defaults to today.

    Returns:
        list: A list of tuples containing (game_id, game_date, home_team, away_team).
    """
    game_data = []
    unique_game_ids = set()

    for row in rows:
        try:
            # Extract game date
            date_element = row.find_element(By.XPATH, ".//td[3]/a")
            game_date_text = date_element.text.strip()
            game_date = dt.strptime(game_date_text, "%m/%d/%Y").date()

            # Filter only future games if current=True
            if current and game_date < scrape_date.date():
                break

            # Extract game ID and matchup data
            matchup_element = row.find_element(By.XPATH, ".//td[2]/a")
            game_id = matchup_element.get_attribute("href")

            # Skip duplicate game IDs
            if game_id in unique_game_ids:
                continue
            unique_game_ids.add(game_id)

            matchup_text = matchup_element.text.strip()

            # Determine home and away teams
            if "@" in matchup_text:
                away, home = matchup_text.split(" @ ")
            elif "vs." in matchup_text:
                home, away = matchup_text.split(" vs. ")
            else:
                print(f"Unexpected matchup format: {matchup_text}")
                continue  # Skip malformed rows

            game_data.append((game_id, game_date, home, away))

        except Exception as e:
            print(f"Error processing row: {e}")
            continue  # Skip problematic rows

    return game_data


def process_all_pages(pages_info, max_threads):
    """Processes multiple pages concurrently using threading.

    Args:
        pages_info (list): List of tuples (page_url, game_id, game_date, home, away).
        max_threads (int): Number of concurrent threads.

    Returns:
        pd.DataFrame: Combined data from all processed pages.
    """
    all_dataframes = []
    retries = {}
    remaining_pages = list(pages_info)

    while remaining_pages:
        failed_pages = []

        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            future_to_page = {executor.submit(process_page, *args): args for args in remaining_pages}

            for future in as_completed(future_to_page):
                args = future_to_page[future]
                try:
                    result = future.result()
                    if isinstance(result, pd.DataFrame):
                        all_dataframes.append(result)
                    else:
                        raise Exception("Processing failed")
                except Exception:
                    failed_pages.append(args)
                    retries[args] = retries.get(args, 0) + 1

        terminate_firefox_processes()
        remaining_pages = failed_pages

    return pd.concat(all_dataframes, ignore_index=True) if all_dataframes else None

def prepare_for_gbq(combined_dataframes):
    """Prepares the dataframe for Google BigQuery by cleaning and formatting data.

    Args:
        combined_dataframes (pd.DataFrame): The dataframe containing game data.

    Returns:
        pd.DataFrame: The cleaned and formatted dataframe ready for BigQuery upload.
    """
    valid_time_pattern = r"^\d{1,2}:\d{1,2}$"
    
    # Standardizing column names
    combined_dataframes.rename(columns={'+/-': 'plus_mins'}, inplace=True)

    # Identify invalid 'MIN' rows
    invalid_rows = ~combined_dataframes['MIN'].str.match(valid_time_pattern)

    # Swap misplaced values due to NA columns
    columns_to_swap = ['FGM', 'FGA', 'FG%', '3PM', '3PA', '3P%']
    valid_columns = ['team', 'game_id', 'game_date', 'matchup', 'url', 'last_updated']
    
    combined_dataframes.loc[invalid_rows, valid_columns] = combined_dataframes.loc[invalid_rows, columns_to_swap].values
    combined_dataframes.loc[invalid_rows, columns_to_swap] = None  # Filling with NA values

    # Standardize date and timezone
    combined_dataframes['last_updated'] = (
        pd.to_datetime(combined_dataframes['last_updated'], errors='coerce')
        .dt.tz_localize('UTC')
        .dt.tz_convert('America/Los_Angeles')
    )

    # Clean string fields
    combined_dataframes['url'] = combined_dataframes['url'].astype(str).str.strip()
    combined_dataframes['game_id'] = combined_dataframes['game_id'].str.lstrip('https://www.nba.com/game/')

    # Convert numerical columns to appropriate types
    num_columns = [
        'FGM', 'FGA', 'FG%', '3PM', '3PA', '3P%', 'FTM', 'FTA', 'FT%', 
        'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TO', 'PF', 'PTS', 'plus_mins'
    ]
    combined_dataframes[num_columns] = combined_dataframes[num_columns].apply(pd.to_numeric, errors='coerce')
    
    # Convert date field to datetime format
    combined_dataframes['game_date'] = pd.to_datetime(
        combined_dataframes['game_date'], format='%m/%d/%Y', errors='coerce'
    )

    # Convert all column names to lowercase
    combined_dataframes.rename(columns=str.lower, inplace=True)

    return combined_dataframes


def send_email(subject, body):
    """Sends an email notification.

    Args:
        subject (str): The email subject.
        body (str): The email body content.
    """
    try:
        load_dotenv('/home/aportra99/Capstone/.env')
        print('Loaded the .env file.')
    except FileNotFoundError:
        print('Could not load .env file.')

    sender_email = os.getenv('SERVER_EMAIL')
    receiver_email = os.getenv('EMAIL_USERNAME')
    password = os.getenv('EMAIL_PASSWORD')

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, password)
        server.send_message(msg)
    except Exception as e:
        print(f"Failed to send email: {e}")
    finally:
        server.quit()


def convert_date(date_str):
    """Converts a date string to a datetime object with the correct year.

    Args:
        date_str (str): The date string in the format "Weekday, Month Day" (e.g., "Tue, Oct 10").

    Returns:
        datetime.date or None: The converted date object or None if invalid.
    """
    try:
        date_obj = dt.strptime(date_str, "%a, %b %d")

        # Assign correct year based on NBA season start (October)
        assumed_year = 2024 if date_obj.month >= 10 else 2025
        date_obj = date_obj.replace(year=assumed_year)

        return date_obj.date()
    except ValueError as e:
        print(f"Skipping invalid date: {date_str} - {e}")
        return None

if __name__ == "__main__":
    driver = establish_driver()
    select_all_option(driver)
