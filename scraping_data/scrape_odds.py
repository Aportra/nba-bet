"""Module for scraping NBA player prop odds from DraftKings and uploading to BigQuery."""

import time
import traceback
from datetime import datetime as dt

import pandas as pd
import pandas_gbq
import scraping_data.utils as utils
from google.cloud import bigquery
from google.oauth2 import service_account
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def process_categories():
    """Scrapes NBA player prop odds from DraftKings and uploads data to BigQuery."""
    driver = utils.establish_driver()
    scrape_date = dt.today()

    urls = {
        "points": "https://sportsbook.draftkings.com/nba-player-props?category=player-points&subcategory=points-o%2Fu",
        "rebounds": "https://sportsbook.draftkings.com/nba-player-props?category=player-rebounds&subcategory=rebounds-o%2Fu",
        "assists": "https://sportsbook.draftkings.com/nba-player-props?category=player-assists&subcategory=assists-o%2Fu",
        "steals": "https://sportsbook.draftkings.com/nba-player-props?category=player-defense&subcategory=steals-o%2Fu",
        "blocks": "https://sportsbook.draftkings.com/nba-player-props?category=player-defense&subcategory=blocks-o%2Fu",
        "threes_made": "https://sportsbook.draftkings.com/nba-player-props?category=player-threes&subcategory=threes-o%2Fu",
    }

    try:
        credentials = service_account.Credentials.from_service_account_file(
            "/home/aportra99/scraping_key.json"
        )
        default_creds = False
        print("Credentials Loaded.")
    except FileNotFoundError:
        default_creds = True
        credentials = None
        print("No credentials file found. Using default credentials.")

    for category, url in urls.items():
        try:
            driver.get(url)
            driver.implicitly_wait(10)
            WebDriverWait(driver, 300).until(
                EC.presence_of_all_elements_located(
                    (By.XPATH, "//tbody[@class='sportsbook-table__body']/tr")
                )
            )

            rows = driver.find_elements(By.XPATH, "//tbody[@class='sportsbook-table__body']/tr")
            data = []

            for row in rows:
                try:
                    name_element = row.find_element(By.XPATH, ".//a/span")
                    name = name_element.text

                    over_element = row.find_element(By.XPATH, "./td[1]")
                    under_element = row.find_element(By.XPATH, "./td[2]")

                    over = over_element.text.split()
                    under = under_element.text.split()

                    over_value = over[2]
                    under_value = under[2]
                    points_value = over[1]

                    data.append({
                        "Player": name,
                        f"{category}": points_value,
                        "Over": over_value,
                        "Under": under_value,
                        "Date_Updated": scrape_date,
                    })
                except Exception as e:
                    error_traceback = traceback.format_exc()
                    error_message = f"""
                    NBA SCRAPING: ERROR PROCESSING PLAYER DATA

                    The script encountered an error:
                    Type: {type(e).__name__}
                    Message: {str(e)}

                    Full Traceback:
                    {error_traceback}
                    """
                    print(error_message)
                    utils.send_email(
                        subject="NBA SCRAPING: ERROR PROCESSING PLAYER DATA",
                        body=error_message,
                    )
                    process_categories()
            combined_data = pd.DataFrame(data)

            table_id = f"miscellaneous-projects-444203.capstone_data.player_{category}_odds"
            table_schema = [{"name": "game_date", "type": "DATE"}]

            pandas_gbq.to_gbq(
                combined_data,
                project_id="miscellaneous-projects-444203",
                destination_table=table_id,
                if_exists="append",
                credentials=credentials if not default_creds else None,
                table_schema=table_schema,
            )

            utils.send_email(
                subject=f"{category} ODDS SCRAPING: COMPLETED # OF PLAYERS {len(data)}",
                body=f"{len(data)} players' odds scraped as of {scrape_date.date()}",
            )
            print(f"Scraping completed for {category}.")

        except Exception as e:
            error_traceback = traceback.format_exc()
            error_message = f"""
            NBA SCRAPING: ERROR PROCESSING CATEGORY {category}

            The script encountered an error:
            Type: {type(e).__name__}
            Message: {str(e)}

            Full Traceback:
            {error_traceback}
            """
            print(error_message)
            utils.send_email(
                subject=f"{category} ODDS SCRAPING: ERROR",
                body=error_message,
            )
            process_categories()
    driver.quit()
