"""Module for scraping NBA team schedules from ESPN and uploading to BigQuery."""

import time
from datetime import datetime as dt
from datetime import timedelta
import pandas as pd
import pandas_gbq
import utils
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from tqdm import tqdm


NBA_TEAMS = [
    "ATL", "BOS", "BKN", "CHA", "CHI", "CLE", "DAL", "DEN", "DET", "GSW",
    "HOU", "IND", "LAC", "LAL", "MEM", "MIA", "MIL", "MIN", "NO", "NYK",
    "OKC", "ORL", "PHI", "PHX", "POR", "SAC", "SAS", "TOR", "UTAH", "WAS"
]


def scrape_team_schedule(nba_teams):
    """Scrapes NBA team schedules from ESPN and uploads data to BigQuery.

    Args:
        nba_teams (list): List of NBA team abbreviations.
    """

    driver = utils.establish_driver(local=True)
    url_base = "https://www.espn.com/nba/team/schedule/_/name/"
    scrape_date = dt.today()-timedelta(1)
    all_data = []

    with tqdm(total=len(nba_teams), desc="Processing Teams", ncols=80) as pbar:
        for team in nba_teams:
            team_url = f"{url_base}{team}"
            driver.get(team_url)

            # Wait for webpage to load
            time.sleep(10)
            WebDriverWait(driver, 30).until(
                EC.presence_of_all_elements_located((By.XPATH, "//tbody[@class='Table__TBODY']"))
            )

            # Gather rows in the table
            rows = driver.find_elements(By.XPATH, "//tbody[@class='Table__TBODY']/tr")
            data = []

            for row in rows[2:]:  # Skipping headers
                try:
                    date_element = row.find_element(By.XPATH, "./td[1]/span")
                    date_text = date_element.text.strip()

                    if date_text.upper() == "DATE":  # Skip unexpected headers
                        continue

                    converted_date = utils.convert_date(date_text)

                    game_played = 0 if converted_date > dt.today().date() else 1
                    divider = row.find_element(By.XPATH,'./td[2]/div/span[1]')
                    divide = divider.text
                    opponent_element = row.find_element(By.XPATH, "./td[2]/div/span[3]/a")
                    opponent_text = opponent_element.text
   
                    # Determine home/away status
                    if "@" in divide:
                        home, away = 0, 1
                    else:
                        home, away = 1, 0

                    opponent_text = opponent_text.split(r"@|vs")[0].strip()

                    data.append({
                        "team": team,
                        "date": converted_date,
                        "opponent": opponent_text,
                        "scrape_date": scrape_date,
                        "home": home,
                        "away": away,
                        "game_played": game_played,
                    })
                except Exception as e:
                    print(f"Error processing {team}: {e}")

            pbar.update(1)
            all_data.extend(data)

    driver.quit()

#     # Convert collected data to a DataFrame
    combined_data = pd.DataFrame(all_data)
    combined_data["date"] = combined_data["date"].astype(str)  # Ensure date column is string for BigQuery

    # Mapping of full team names to abbreviations
    team_abbreviations = {
        "ATL": "ATL", "BOS": "BOS", "BKN": "BKN", "CHA": "CHA", "CHI": "CHI",
        "CLE": "CLE", "DAL": "DAL", "DEN": "DEN", "DET": "DET", "GS": "GSW",
        "HOU": "HOU", "IND": "IND", "LAL": "LAL",  # Lakers
        "LA": "LAC", "MEM": "MEM", "MIA": "MIA", "MIL": "MIL", "MIN": "MIN",
        "NOP": "NOP", "NY": "NYK", "OKC": "OKC", "ORL": "ORL",
        "PHI": "PHI", "PHX": "PHX", "POR": "POR", "SA": "SAS",
        "SAC": "SAC", "TOR": "TOR", "UTAH": "UTA", "WSH": "WAS"
    }

    # Replace opponent names with correct abbreviations
    combined_data["opponent"] = combined_data["opponent"].replace(team_abbreviations)

    # Normalize team abbreviations
    combined_data["team"] = combined_data["team"].replace({"NO": "NOP", "UTAH": "UTA", "WSH": "WAS","NY":"NYK","GS":"GSW","SA":"SAC","LA":"LAC"})

    # Upload to BigQuery
    pandas_gbq.to_gbq(
        combined_data,
        project_id="miscellaneous-projects-444203",
        destination_table="miscellaneous-projects-444203.capstone_data.schedule",
        if_exists="replace",
        table_schema=[{"name": "date", "type": "DATE"}]
    )

    print("Scraping completed successfully.")


# Run the scraping function
scrape_team_schedule(NBA_TEAMS)
