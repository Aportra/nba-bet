"""Module for scraping NBA game data and uploading to BigQuery."""

import time
import gc
import traceback
from datetime import datetime as dt

import pandas as pd
import pandas_gbq
import scraping_data.utils as utils
from google.cloud import bigquery
from google.oauth2 import service_account
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from tqdm import tqdm


def scrape_current_games():
    """Scrapes NBA game data and uploads it to BigQuery."""
    
    try:
        credentials = service_account.Credentials.from_service_account_file(
            "/home/aportra99/scraping_key.json"
        )
        local = False
    except FileNotFoundError:
        print("File not found. Continuing as if on local.")
        local = True
        credentials = None

    driver = utils.establish_driver()
    scrape_date = dt.today()
    url = {
        "NBA_Season_2024-2025_uncleaned": "https://www.nba.com/stats/teams/boxscores?Season=2024-25"
    }

    driver.get(url["NBA_Season_2024-2025_uncleaned"])
    time.sleep(5)

    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located(
                (By.XPATH, "/html/body/div[1]/div[2]/div[2]/div[3]/section[2]/div/div[2]/div[3]/table")
            )
        )
        print("The section has loaded!")
    except TimeoutException:
        utils.send_email(
            subject="NBA SCRAPING: DATE ERRORS",
            body="The section did not load in time.",
        )

    rows = driver.find_elements(By.XPATH, "//tbody[@class='Crom_body__UYOcU']/tr")
    game_data = utils.gather_data(rows)
    driver.quit()

    data = []
    failed_pages = []

    try:
        if game_data:
            for game_id, game_date, home, away in game_data:
                page = f"{game_id}/box-score"
                result = utils.process_page(page, game_id, game_date, home, away)
                
                if isinstance(result, pd.DataFrame):
                    data.append(result)
                else:
                    failed_pages.append(result)
                    print(f"Failed pages count: {len(failed_pages)}")

            retries = {}

            while failed_pages:
                game_id, game_date, home, away = failed_pages.pop(0)
                key = (game_id, game_date, home, away)
                retries[key] = retries.get(key, 0) + 1

                print(f"Retry count for {game_id}: {retries[key]}")
                print(f"Processing failed page: {game_id}")

                page = f"{game_id}/box-score"
                result = utils.process_page(page, game_id, game_date, home, away)

                if isinstance(result, pd.DataFrame):
                    data.append(result)
                    print(f"Successfully processed failed page: {game_id}")
                else:
                    failed_pages.append((game_id, game_date, home, away))
                    print(f"Retry failed: {game_id}. Re-adding to failed pages.")

            combined_dataframes = pd.concat(data, ignore_index=True)
            combined_dataframes = utils.prepare_for_gbq(combined_dataframes)

            table_id = "miscellaneous-projects-444203.capstone_data.2024-2025_uncleaned"
            table_schema = [{"name": "game_date", "type": "DATE"}]

            if not local:
                pandas_gbq.to_gbq(
                    combined_dataframes,
                    project_id="miscellaneous-projects-444203",
                    destination_table=table_id,
                    if_exists="append",
                    credentials=credentials,
                    table_schema=table_schema,
                )
            else:
                pandas_gbq.to_gbq(
                    combined_dataframes,
                    project_id="miscellaneous-projects-444203",
                    destination_table=table_id,
                    if_exists="append",
                    table_schema=table_schema,
                )

            utils.send_email(
                subject=f"NBA SCRAPING: COMPLETED # OF GAMES {len(game_data)}",
                body=f"{len(game_data)} games scraped as of {scrape_date.date()}",
            )
            print("Scraping successful.")

            return combined_dataframes, len(game_data)

        utils.send_email(
            subject="NBA SCRAPING: NO GAMES",
            body=f"No games found as of {scrape_date.date()}",
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

    finally:
        driver.quit()



def scrape_past_games(multi_threading=True, max_workers=0):
    """Scrapes past NBA game data from 2015 to 2025 and uploads to BigQuery.
    
    Args:
        multi_threading (bool): Whether to use multi-threading for scraping.
        max_workers (int): Number of workers to use for multi-threading.
    """

    urls = {
        f"{i}-{i+1}_uncleaned": f"https://www.nba.com/stats/teams/boxscores-advanced?Season={i}-{str(i-2000+1)}"
        for i in range(2015, 2025)
    }

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

    for season, url in urls.items():
        driver = utils.establish_driver(local=True)
        driver.get(url)
        utils.select_all_option(driver)
        time.sleep(5)

        rows = driver.find_elements(By.XPATH, "//tbody[@class='Crom_body__UYOcU']/tr")
        game_data = utils.gather_data(rows, current=False)
        driver.quit()

        if multi_threading:
            print(f"Using {max_workers} threads.")
            pages_info = [
                (f"{game_id}/box-score", game_id, game_date, home, away)
                for game_id, game_date, home, away in game_data
            ]
            combined_data = utils.process_all_pages(pages_info, max_threads=max_workers)
        else:
            data = []
            failed_pages = []

            with tqdm(total=len(game_data), desc="Processing Games", ncols=80) as pbar:
                for game_id, game_date, home, away in game_data:
                    page = f"{game_id}/box-score"
                    result = utils.process_page(page, game_id, game_date, home, away)

                    if isinstance(result, pd.DataFrame):
                        data.append(result)
                    else:
                        failed_pages.append(result)
                        print(f"Failed pages count: {len(failed_pages)}")

                    pbar.update(1)

            retries = {}

            while failed_pages:
                game_id, game_date, home, away = failed_pages.pop(0)
                key = (game_id, game_date, home, away)
                retries[key] = retries.get(key, 0) + 1

                print(f"Retry count for {game_id}: {retries[key]}")
                print(f"Processing failed page: {game_id}")

                page = f"{game_id}/box-score"
                result = utils.process_page(page, game_id, game_date, home, away)

                if isinstance(result, pd.DataFrame):
                    data.append(result)
                    print(f"Successfully processed failed page: {game_id}")
                else:
                    failed_pages.append((game_id, game_date, home, away))
                    print(f"Retry failed: {game_id}. Re-adding to failed pages.")

            combined_data = pd.concat(data, ignore_index=True)

        combined_data = utils.prepare_for_gbq(combined_data)
        table_id = f"miscellaneous-projects-444203.capstone_data.{season}"
        table_schema = [{"name": "game_date", "type": "DATE"}]

        if local:
            pandas_gbq.to_gbq(
                combined_data,
                project_id="miscellaneous-projects-444203",
                destination_table=table_id,
                if_exists="replace",
                table_schema=table_schema,
            )
        else:
            pandas_gbq.to_gbq(
                combined_data,
                project_id="miscellaneous-projects-444203",
                destination_table=table_id,
                if_exists="replace",
                credentials=credentials,
                table_schema=table_schema,
            )

        del combined_data
        gc.collect()