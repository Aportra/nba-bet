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
from bs4 import BeautifulSoup
from tqdm import tqdm


def establish_driver(local=False):
    """Establishes a Selenium WebDriver for Firefox.

    Args:
        local (bool): If True, runs WebDriver locally. Otherwise, uses a remote setup.

    Returns:
        webdriver.Firefox: A configured instance of the Firefox WebDriver.
    """
    try:
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--window-size=1920,1080")

        if not local:
            options.binary_location = "/usr/bin/firefox"
            geckodriver_path = "/usr/local/bin/geckodriver"
            service = Service(executable_path=geckodriver_path, log_path="geckodriver.log")

            driver = webdriver.Firefox(service=service, options=options)
            print("Remote WebDriver initialized successfully.")
        else:
            driver = webdriver.Firefox(options=options)
            print("Local WebDriver initialized successfully.")

        return driver

    except Exception as e:
        print(f"Error initializing WebDriver: {e}")
        return None


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


if __name__ == "__main__":
    driver = establish_driver()
    select_all_option(driver)
