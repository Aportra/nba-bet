"""Selenium Utility Module for Web Scraping and Automation."""

import os
import time
import random
import smtplib
import signal
import psutil
import pandas as pd
import chromedriver_autoinstaller
import requests
from datetime import datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from tqdm import tqdm


def establish_driver(local=False):
    """Establishes a Selenium WebDriver for Chrome.

    Args:
        local (bool): If True, runs WebDriver locally. Otherwise, uses a remote setup.

    Returns:
        webdriver.Chrome: A configured instance of the Chrome WebDriver.
    """
    chrome_options = webdriver.ChromeOptions()  # Correct usage of ChromeOptions
    chrome_options.add_argument("--headless")  # Run without UI (remove if you need the UI)
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")  # Prevents GPU-related crashes
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36")

    if not local:
        # If not local, install chromedriver and set up the service for remote use
        chrome_path = chromedriver_autoinstaller.install()
        service = Service(chrome_path)
        chrome_options.binary_location = "/usr/bin/google-chrome-stable"  # Path to Chrome binary if using a custom installation
        driver = webdriver.Chrome(service=service, options=chrome_options)
    else:
        # If local, use the automatically installed chromedriver
        chrome_path = chromedriver_autoinstaller.install()
        service = Service(chrome_path)
        driver = webdriver.Chrome(service=service, options=chrome_options)
    
    # Set the window size for the Chrome browser (optional)
    driver.set_window_size(2560, 1440)

    return driver

def establish_requests(url):
    # Headers to mimic a real browser request (prevents bot blocking)

    USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:90.0) Gecko/20100101 Firefox/90.0",
        ]
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": "https://www.nba.com/",
        "Origin": "https://www.nba.com",
        "Accept": "application/json, text/plain, */*",
        "Host": "stats.nba.com",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9"
    }

    # Send request
    response = requests.get(url, headers=headers)
    
    return response


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



def process_game(game_id):
    """Fetch and process individual game data from the NBA API."""
    try:
        url = f"https://stats.nba.com/stats/boxscoretraditionalv2?GameID={game_id}&StartPeriod=0&EndPeriod=10"
        game_response = establish_requests(url)

        # Check if request is successful
        if game_response.status_code != 200:
            raise Exception(f"API Error: Status Code {game_response.status_code}")

        game_response = game_response.json()

        # Extract headers and data
        column = [header.lower() for header in game_response['resultSets'][0]['headers']]
        row_data = game_response['resultSets'][0]['rowSet']

        # Create DataFrame
        game_data = pd.DataFrame(row_data, columns=column)

        # Drop unnecessary columns
        game_data.drop(columns=['comment', 'start_position', 'nickname'], inplace=True, errors='ignore')

        # Fix minutes column formatting
        game_data['min'] = game_data['min'].apply(
            lambda x: ''.join(x.split('.000000')) if isinstance(x, str) and '.000000' in x else x
        )

        return game_data  # Return processed game DataFrame

    except Exception as e:
        print(f"Failed to process game {game_id}: {e}")
        return None  # Return None if failure occurs

def process_all_games(game_ids, max_threads=5):
    """Processes multiple NBA games concurrently using threading.

    Args:
        game_ids (list): List of game IDs to process.
        max_threads (int): Number of concurrent threads.

    Returns:
        pd.DataFrame: Combined data from all processed games.
    """
    all_dataframes = []
    retries = {}
    remaining_games = list(game_ids)

    while remaining_games:
        failed_games = []

        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            future_to_game = {executor.submit(process_game, game): game for game in remaining_games}

            # Using tqdm for progress tracking
            for future in tqdm(as_completed(future_to_game), total=len(remaining_games), desc="Processing Games"):
                game = future_to_game[future]
                try:
                    result = future.result()
                    if isinstance(result, pd.DataFrame):
                        all_dataframes.append(result)
                    else:
                        raise Exception("Game processing failed")
                except Exception as e:
                    print(f"Retrying game {game}: {e}")
                    failed_games.append(game)
                    retries[game] = retries.get(game, 0) + 1

        remaining_games = [g for g in failed_games if retries[g] < 3]  # Retry failed games up to 3 times
        time.sleep(2)  # Brief pause between retry loops

    if all_dataframes:
        return pd.concat(all_dataframes, ignore_index=True)
    else:
        return pd.DataFrame()  # Return empty D

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
