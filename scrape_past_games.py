import pandas as pd
import main
from datetime import datetime as dt
from selenium.webdriver.common.by import By
from selenium import webdriver
import time
import pandas_gbq
from google.cloud import bigquery

# Basketball Reference URLs
urls = {
    'NBA_Season_2021-2022_': 'https://www.basketball-reference.com/leagues/NBA_2022_games-',
    'NBA_Season_2022-2023_': 'https://www.basketball-reference.com/leagues/NBA_2023_games-',
    'NBA_Season_2023-2024_': 'https://www.basketball-reference.com/leagues/NBA_2024_games-',
    'NBA_Season_2024-2025_': 'https://www.basketball-reference.com/leagues/NBA_2025_games-',
}

months = ['October', 'November', 'December', 'January', 'February', 'March', 'April']

valid_time_pattern = r"^\d{1,2}:\d{1,2}$"

driver = webdriver.Firefox()

for url_key, base_url in urls.items():
    game_data = []
    unique_game_ids = set()

    for month in months[:1]:
        driver.get(f"{base_url}{month.lower()}.html")
        time.sleep(2)  # Allow page to load

        rows = driver.find_elements(By.XPATH, "//table[@id='schedule']/tbody/tr")

        for row in rows[:1]:
            # Skip header rows
            if "thead" in row.get_attribute("class"):
                continue

            # Extract game date
            date_element = row.find_element(By.XPATH, "./th[@data-stat='date_game']")
            game_date_text = date_element.text.strip()
            print(game_date_text)

            try:
                # Parse the date using the existing format
                game_date = dt.strptime(game_date_text, "%a, %b %d, %Y").date()
            except ValueError:
                print(f"Invalid date format: {game_date_text}")
                continue  # Skip invalid dates

            # Stop if the game is in the future
            if game_date > dt.today().date():
                break

            # Extract matchup data
            try:
                visitor_team = row.find_element(By.XPATH, "./td[@data-stat='visitor_team_name']").text.strip()
                home_team = row.find_element(By.XPATH, "./td[@data-stat='home_team_name']").text.strip()
                game_id = row.find_element(By.XPATH, "./td[@data-stat='box_score_text']/a").get_attribute("href")
            except Exception:
                continue  # Skip rows with missing data

            if game_id in unique_game_ids:
                continue

            unique_game_ids.add(game_id)
            game_data.append((game_id, game_date, home_team, visitor_team))

    # Process game data
    data = []
    failed_pages = []
    retries = {}

    for i, (game_id, game_date, home, away) in enumerate(game_data):
        print(game_id)
        page = f"{game_id}"
        if len(data) % 100:
            print(f"Processing {i} requests: {round(len(data)/len(game_data)*100, 2)}% complete")

        result = main.process_page(page, game_id, game_date, home, away, driver)
        if isinstance(result, pd.DataFrame):
            data.append(result)
        else:
            failed_pages.append((game_id, game_date, home, away))
            print(f"Failed Pages length: {len(failed_pages)}")

    # Retry failed pages
    while failed_pages:
        game_id, game_date, home, away = failed_pages.pop(0)
        key = (game_id, game_date, home, away)

        retries[key] = retries.get(key, 0) + 1
        print(f"Retry Count: {retries[key]}")

        page = f"{game_id}"
        result = main.process_page(page, game_id, game_date, home, away, driver)

        if isinstance(result, pd.DataFrame):
            data.append(result)
            print(f"Processed {game_id} from failed pages")
        else:
            failed_pages.append((game_id, game_date, home, away))
            print(f"Failed {game_id} again, re-added to retry list")

    # Combine dataframes for upload
    combined_dataframes = pd.concat(data, ignore_index=True)

    # BigQuery client setup
    client = bigquery.Client(project="miscellaneous-projects-444203")

    combined_dataframes.rename(columns={'+/-': 'plus_minus'}, inplace=True)

    # Correct invalid rows
    # invalid_rows = ~combined_dataframes['MIN'].str.match(valid_time_pattern, na=False)
    # columns_to_swap = ['FGM', 'FGA', 'FG%', '3PM', '3PA', '3P%']
    # valid_columns = ['team', 'game_id', 'game_date', 'matchup', 'url', 'last_updated']

    # combined_dataframes.loc[invalid_rows, valid_columns] = combined_dataframes.loc[invalid_rows, columns_to_swap].values
    # combined_dataframes.loc[invalid_rows, columns_to_swap] = None

    # # Fix data types
    # combined_dataframes['game_date'] = pd.to_datetime(combined_dataframes['game_date'], errors='coerce')
    # combined_dataframes['last_updated'] = pd.to_datetime(combined_dataframes['last_updated'], errors='coerce')
    # combined_dataframes['url'] = combined_dataframes['url'].astype(str).str.strip()
    # combined_dataframes['game_id'] = combined_dataframes['game_id'].str.replace('https://www.basketball-reference.com/', '')

    # num_columns = ['FGM', 'FGA', 'FG%', '3PM', '3PA', '3P%', 'FTM', 'FTA', 'FT%', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TO', 'PF', 'PTS', 'plus_mins']
    # combined_dataframes[num_columns] = combined_dataframes[num_columns].apply(pd.to_numeric, errors='coerce')

    # Upload to BigQuery
    pandas_gbq.to_gbq(combined_dataframes, project_id="miscellaneous-projects-444203", destination_table=f'basketball_reference.{url_key}_basketball_ref', if_exists='replace')
