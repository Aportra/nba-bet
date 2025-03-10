"""Module for scraping NBA team ratings from NBA Stats and uploading to BigQuery."""

import time
import traceback
from datetime import datetime as dt

import pandas as pd
import pandas_gbq
import scraping_data.utils as utils
from google.cloud import bigquery
from google.oauth2 import service_account
from selenium.webdriver.common.by import By


def scrape_current_team_data(length):
    """Scrapes current NBA team ratings and uploads data to BigQuery.
    
    Args:
        length (int): Expected number of teams per game (usually 2).
        
    Returns:
        pd.DataFrame: The scraped data.
    """

    try:
        credentials = service_account.Credentials.from_service_account_file(
            "/home/aportra99/scraping_key.json"
        )
        local = False
        print("Credentials file loaded.")
    except FileNotFoundError:
        local = True
        credentials = None
        print("Running with default credentials.")

    urls = {
        "2024-2025_team_ratings": "https://www.nba.com/stats/teams/boxscores-advanced?Season=2024-25"
    }

    driver = utils.establish_driver()
    scrape_date = dt.today()

    try:
        for season, url in urls.items():
            driver.get(url)
            time.sleep(5)

            table_element = driver.find_element(By.XPATH, "//table[contains(@class, 'Crom_table__p1iZz')]")
            rows = table_element.find_elements(By.XPATH, ".//tr")

            if not rows:
                print(f"No rows found for {season}. Skipping...")
                continue

            headers = [th.text.strip().lower() for th in rows[0].find_elements(By.XPATH, ".//th")]
            headers.extend(["game_id", "home", "away", "last_updated"])
            game_data = []

            for idx, row in enumerate(rows[1:-1]):
                if (idx + 1) % 10 == 0:
                    print(f"{round((idx + 1) / len(rows) * 100, 2)}% gathered")

                cols = row.find_elements(By.XPATH, ".//td")

                # Extract game date
                date_element = row.find_element(By.XPATH, ".//td[3]/a")
                game_date_text = date_element.text.strip()
                game_date = dt.strptime(game_date_text, "%m/%d/%Y").date()

                if game_date < scrape_date.date():
                    break  # Stop if data is outdated

                # Extract game ID and matchup
                matchup_element = row.find_element(By.XPATH, ".//td[2]/a")
                game_id = matchup_element.get_attribute("href")
                matchup_text = matchup_element.text.strip()

                if "@" in matchup_text:
                    home_binary, away_binary = 0, 1
                elif "vs." in matchup_text:
                    home_binary, away_binary = 1, 0
                else:
                    home_binary, away_binary = None, None  # Handle unexpected cases

                row_data = [col.text.strip() for col in cols]
                row_data.extend([game_id, home_binary, away_binary, scrape_date])

                game_data.append(row_data)

            if len(game_data) < length * 2 and game_data:
                driver.quit()
                utils.send_email(
                    subject="NEEDED TO RESTART TEAM DATA SCRIPT",
                    body=f"Restarting script as of {scrape_date.date()}",
                )
                return scrape_current_team_data(length)

            # Convert data into a DataFrame
            data = pd.DataFrame(game_data, columns=headers)

            # Rename columns for clarity
            data.rename(columns={"w/l": "win_loss", "ast/to": "ast_to", "ast\nratio": "ast_ratio"}, inplace=True)

            # Convert date column to datetime format
            data["game date"] = pd.to_datetime(data["game date"]).dt.date

            # Convert numerical columns to float
            string_columns = ["home", "away", "game_id", "win_loss", "team", "match up", "game date", "last_updated"]
            for column in data.columns:
                if column not in string_columns:
                    data[column] = data[column].astype("float64")

            # Upload data to BigQuery
            table_id = f"miscellaneous-projects-444203.capstone_data.{season}"
            table_schema = [{"name": "game date", "type": "DATE"}]

            pandas_gbq.to_gbq(
                data,
                project_id="miscellaneous-projects-444203",
                destination_table=table_id,
                if_exists="append",
                credentials=credentials if not local else None,
                table_schema=table_schema,
            )

        # Send a summary email
        if game_data:
            utils.send_email(
                subject=f"TEAM RATINGS SCRAPING: COMPLETED # OF GAMES {len(game_data)}",
                body=f"{len(game_data)} games scraped as of {scrape_date.date()}",
            )
        else:
            utils.send_email(
                subject="TEAM RATINGS SCRAPING: NO GAMES",
                body=f"No games as of {scrape_date.date()}",
            )

    except Exception as e:
        error_traceback = traceback.format_exc()
        error_message = f"""
        NBA SCRAPING: SCRIPT CRASHED

        The script encountered an error:
        Type: {type(e).__name__}
        Message: {str(e)}

        Full Traceback:
        {error_traceback}
        """
        print(error_message)

        utils.send_email(
            subject="NBA SCRAPING: SCRIPT CRASHED",
            body=error_message,
        )

    driver.quit()
    return data


def scrape_past_team_data():
    """Scrapes past NBA team ratings and uploads data to BigQuery."""

    try:
        credentials = service_account.Credentials.from_service_account_file(
            "/home/aportra99/scraping_key.json"
        )
        local = False
        print("Credentials file loaded.")
    except FileNotFoundError:
        local = True
        credentials = None
        print("Running with default credentials.")

    urls = {
        f"{i}-{i+1}_team_ratings": f"https://www.nba.com/stats/teams/boxscores-advanced?Season={i}-{str(i-2000+1)}"
        for i in range(2015, 2025)
    }

    driver = utils.establish_driver(local=True)
    scrape_date = dt.today()

    try:
        for season, url in urls.items():
            driver.get(url)
            time.sleep(5)
            utils.select_all_option(driver)

            table_element = driver.find_element(By.XPATH, "//table[contains(@class, 'Crom_table__p1iZz')]")
            rows = table_element.find_elements(By.XPATH, ".//tr")

            if not rows:
                print(f"No rows found for {season}. Skipping...")
                continue

            headers = [th.text.strip().lower() for th in rows[0].find_elements(By.XPATH, ".//th")]
            headers.extend(["game_id", "home", "away", "last_updated"])
            game_data = []

            for idx, row in enumerate(rows[1:-1]):
                if (idx + 1) % 10 == 0:
                    print(f"{round((idx + 1) / len(rows) * 100, 2)}% gathered")

                cols = row.find_elements(By.XPATH, ".//td")

                # Extract game ID and matchup
                matchup_element = row.find_element(By.XPATH, ".//td[2]/a")
                game_id = matchup_element.get_attribute("href").lstrip("https://www.nba.com/game/")
                matchup_text = matchup_element.text.strip()

                if "@" in matchup_text:
                    home_binary, away_binary = 0, 1
                elif "vs." in matchup_text:
                    home_binary, away_binary = 1, 0
                else:
                    home_binary, away_binary = None, None  # Handle unexpected cases

                row_data = [col.text.strip() for col in cols]
                row_data.extend([game_id, home_binary, away_binary, scrape_date])

                game_data.append(row_data)

            # Convert data into a DataFrame
            data = pd.DataFrame(game_data, columns=headers)

            # Rename columns for consistency
            data.rename(columns={"w/l": "win_loss", "ast/to": "ast_to", "ast\nratio": "ast_ratio"}, inplace=True)

            # Convert date column to datetime format
            data["game date"] = pd.to_datetime(data["game date"]).dt.date

            # Convert numerical columns to float
            string_columns = ["home", "away", "game_id", "win_loss", "team", "match up", "game date", "last_updated"]
            for column in data.columns:
                if column not in string_columns:
                    data[column] = data[column].astype("float64")

            # Upload data to BigQuery
            table_id = f"miscellaneous-projects-444203.capstone_data.{season}"
            table_schema = [{"name": "game date", "type": "DATE"}]

            pandas_gbq.to_gbq(
                data,
                project_id="miscellaneous-projects-444203",
                destination_table=table_id,
                if_exists="replace",
                credentials=credentials if not local else None,
                table_schema=table_schema,
            )

        print("Scraping completed successfully.")

    except Exception as e:
        error_traceback = traceback.format_exc()
        error_message = f"""
        NBA SCRAPING: SCRIPT CRASHED

        The script encountered an error:
        Type: {type(e).__name__}
        Message: {str(e)}

        Full Traceback:
        {error_traceback}
        """
        print(error_message)

        utils.send_email(
            subject="NBA SCRAPING: SCRIPT CRASHED",
            body=error_message,
        )

    driver.quit()