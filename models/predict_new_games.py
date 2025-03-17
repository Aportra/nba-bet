from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.neural_network._multilayer_perceptron import MLPRegressor
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium import webdriver
from google.oauth2 import service_account
from datetime import datetime as date
from selenium.webdriver.firefox.options import Options
from sklearn.preprocessing import StandardScaler
from models import model_utils

import joblib
import pandas as pd
import pandas_gbq
import time


def clean_player_name(name):
    """Standardizes player names by removing special characters and handling known name variations."""
    name = name.lower().strip()  # Convert to lowercase & remove extra spaces
    name = name.replace(".", "")  # Remove periods

    # Known name changes (add more as needed)
    name_corrections = {
        "alexandre sarr": "alex sarr",
        "jimmy butler": "jimmy butler iii",
        "nicolas claxton": "nic claxton",
        "kenyon martin jr": "kj martin",
        "carlton carrington": "bub carrington",
        "ron holland ii": "ronald holland ii",
        'cameron thomas':'cam thomas'
    }

    # Apply corrections if the name exists in the dictionary
    return name_corrections.get(name, name)  # Default to original name if no correction found


def fetch_bigquery_data(query,credentials):
    """Fetches data from BigQuery."""
    print(f"Fetching data from BigQuery: {query[:50]}...") 
    try:
        return pd.DataFrame(
            pandas_gbq.read_gbq(
                query, 
                project_id="miscellaneous-projects-444203",
                credentials=credentials
            )
        )
    except Exception as e:
        print(f"BigQuery Error: {e}")
        return pd.DataFrame()


def gather_data_to_model():
    """Fetches today's NBA schedule from BigQuery."""
    query = """
    SELECT team, opponent, date
    FROM `capstone_data.schedule`
    WHERE date = CURRENT_DATE('America/Los_Angeles')
    """

    team_mapping = {"WAS": "WSH", "UTA": "UTAH", "NOP": "NO"}

    try:
        credentials = service_account.Credentials.from_service_account_file("/home/aportra99/scraping_key.json")
        local = False
    except FileNotFoundError:
        print("File not found, continuing as if on local.")
        local = True
        credentials = None

    teams_data = fetch_bigquery_data(query,credentials=credentials)
    if teams_data.empty:
        print("No game data found for today.")
        return pd.DataFrame()

    teams_data["team"] = teams_data["team"].replace(team_mapping)
    teams_data["opponent"] = teams_data["opponent"].replace(team_mapping)

    return teams_data


def scrape_roster(data):
    """Scrapes team rosters for today's games from ESPN."""
    print("Fetching team rosters...")

    driver = model_utils.establish_driver(local=True)

    teams, players, opponents = [], [], []

    for team, opp in zip(data["team"], data["opponent"]):
        url = f"https://www.espn.com/nba/team/roster/_/name/{team}/"
        driver.get(url)
        time.sleep(5)
        driver.implicitly_wait(10)
        WebDriverWait(driver, 300).until(
            EC.presence_of_all_elements_located(
                (By.XPATH, '//tbody[@class="Table__TBODY"]/tr')
            )
        )
        try:
            rows = driver.find_elements(By.XPATH, "//tbody[@class='Table__TBODY']/tr")
            for row in rows:
                name_element = row.find_element(By.XPATH, ".//td[2]/div/a")
                players.append(name_element.text)
                teams.append(team)
                opponents.append(opp)
        except Exception as e:
            print(f"Error scraping {team} roster: {e}")

    print("Quitting WebDriver...")  # Debugging print
    driver.quit()
    print("WebDriver successfully quit.")

    # Standardize team names
    team_mapping = {"WSH": "WAS", "UTAH": "UTA", "NO": "NOP"}
    teams = [team_mapping.get(team, team) for team in teams]
    opponents = [team_mapping.get(team, team) for team in opponents]

    return pd.DataFrame({"player": players, "team": teams, "opponent": opponents})


def pull_odds():
    """Fetches the latest player odds from BigQuery."""
    tables = ["points", "rebounds", "assists", "threes_made"]
    odds_data = {}

    try:
        credentials = service_account.Credentials.from_service_account_file("/home/aportra99/scraping_key.json")
        local = False
    except FileNotFoundError:
        print("File not found, continuing as if on local.")
        local = True
        credentials = None

    for table in tables:
        odds_query = f"""
        SELECT * 
        FROM `capstone_data.player_{table}_odds`
        WHERE DATE(Date_Updated) = CURRENT_DATE('America/Los_Angeles')
        """
        odds_data[table] = fetch_bigquery_data(odds_query,credentials=credentials)
        odds_data[table]["Player"] = odds_data[table]["Player"].apply(clean_player_name)

    return odds_data


def recent_player_data(games):
    """Fetches recent player, team, and opponent data from BigQuery."""
    print("Fetching recent player, team, and opponent data...")

    games["player"] = games["player"].apply(clean_player_name)
    games.rename(columns={"opponent": "matchup"}, inplace=True)

    today = date.today()
    season = today.year if today.month >= 10 else today.year - 1

    existing_players_query = """
    SELECT DISTINCT player 
    FROM `capstone_data.player_prediction_data_partitioned`
    WHERE season_start_year = 2024
    """
    try:
        credentials = service_account.Credentials.from_service_account_file("/home/aportra99/scraping_key.json")
    except FileNotFoundError:
        credentials=None
    existing_players = fetch_bigquery_data(existing_players_query,credentials=credentials)
    existing_players_set = set(existing_players["player"].apply(clean_player_name))
    filtered_players = [player for player in games["player"].unique() if player in existing_players_set]

    if not filtered_players:
        print("No valid players found.")
        return None, None

    queries = {
    "player_data": f"""
        WITH RankedGames AS (
            SELECT *, 
                ROW_NUMBER() OVER (PARTITION BY player ORDER BY game_date DESC) AS game_rank
            FROM `capstone_data.player_prediction_data_partitioned`
            WHERE LOWER(player) IN ({','.join([f'"{player}"' for player in filtered_players])})
            AND season_start_year = {season}
        )
        SELECT *
        FROM RankedGames
        WHERE game_rank = 1;
    """,
    "team_data": f"""
        WITH RankedGames AS (
            SELECT *, 
                ROW_NUMBER() OVER (PARTITION BY team ORDER BY game_date DESC) AS game_rank
            FROM `capstone_data.team_prediction_data_partitioned`
            WHERE team IN ({','.join([f'"{team}"' for team in games["team"].unique()])}) 
            AND season_start_year = {season}
        )
        SELECT *
        FROM RankedGames
        WHERE game_rank = 1;
    """
}


    # Fetch player, opponent, and team data
    player_data, team_data = [fetch_bigquery_data(queries[q],credentials=credentials) for q in queries]
    print('queries complete')
    # Standardize player names in player_data
    print('cleaning names')
    player_data["player"] = player_data["player"].apply(clean_player_name)

    team_data  = team_data.merge(team_data,on='game_id',suffixes=("_team","_opponent"))
    team_data_merged = team_data_merged[team_data_merged["team_id_team"] != team_data_merged["team_id_opponent"]]

    # Merge datasets while keeping only necessary columns
    print('merging data')
    full_data = (
        games
        .merge(player_data, on="player", how="inner", suffixes=("", "_remove"))
        .merge(team_data, on="team_abbreviation", how="inner", suffixes=("", "_remove"))
    )

    # Drop duplicate or unnecessary columns
    full_data.drop([col for col in full_data.columns if "_remove" in col], axis=1, inplace=True)
    full_data.drop([col for col in full_data.columns if "_1" in col], axis=1, inplace=True)

    print('data merged')
    odds_data = pull_odds()

    return full_data, odds_data




def predict_games(full_data, odds_data):
    """Predicts NBA player stats using pre-trained models and compares with betting odds."""
    print('loading models...')
    # Load models
    # models = joblib.load('/home/aportra99/Capstone/models/models.pkl')
    
    models = joblib.load('models/models.pkl')
    for key, odds_df in odds_data.items():
        print(f"Processing predictions for {key}...")

        # Filter relevant players
        data_ordered = full_data[full_data['player'].isin(odds_df['Player'])].copy()
        print(f"Filtered {len(data_ordered)} players for {key} predictions.")

        # Calculate per-minute stats for momentum tracking
        data_ordered['pts_per_min_3gm'] = data_ordered['pts_3gm_avg'] / data_ordered['min_3gm_avg']
        data_ordered['pts_per_min_season'] = data_ordered['pts_season'] / data_ordered['min_season']
        data_ordered['pts_per_min_momentum'] = data_ordered['pts_per_min_3gm'] - data_ordered['pts_per_min_season']

        data_ordered['3pm_per_min_3gm'] = data_ordered['3pm_3gm_avg'] / data_ordered['min_3gm_avg']
        data_ordered['3pm_per_min_season'] = data_ordered['3pm_season'] / data_ordered['min_season']
        data_ordered['3pm_per_min_momentum'] = data_ordered['3pm_per_min_3gm'] - data_ordered['3pm_per_min_season']

        data_ordered['reb_per_min_3gm'] = data_ordered['reb_3gm_avg'] / data_ordered['min_3gm_avg']
        data_ordered['reb_per_min_season'] = data_ordered['reb_season'] / data_ordered['min_season']
        data_ordered['reb_per_min_momentum'] = data_ordered['reb_per_min_3gm'] - data_ordered['reb_per_min_season']

        # Ensure chronological order for calculations
        data_ordered.sort_values(by=['player', 'season', 'game_date'], inplace=True)

        # Load Google Cloud credentials
        try:
            credentials = service_account.Credentials.from_service_account_file('/home/aportra99/scraping_key.json')
            local = False
        except FileNotFoundError:
            print("File not found, running in local mode.")
            local = True
            credentials = None

        # Determine category for prediction
        category_mapping = {
            "points": "pts",
            "rebounds": "reb",
            "assists": "ast",
            "threes_made": "3pm"
        }
        category = category_mapping.get(key, "pts")

        # Run predictions using trained models
        for model_name, model in models[category].items():
            if model_name.lower() not in ["xgboost", "sarimax", "mlp", "random_forest"]:
                features = [f.strip() for f in model.feature_names_in_]

                if set(features).issubset(data_ordered.columns):
                    data_ordered[f'{category}_{model_name}'] = model.predict(data_ordered[features])
                else:
                    print(f"Skipping {model_name} for {category}: Missing features")

        # Standardize player names
        data_ordered['player'] = data_ordered['player'].apply(clean_player_name)
        odds_df['Player'] = odds_df['Player'].apply(clean_player_name)

        # Convert betting odds to numeric values
        for col in ['Over', 'Under']:
            odds_df[col] = pd.to_numeric(
                odds_df[col].astype(str).str.replace('−', '-', regex=False).str.replace('+', '', regex=False),
                errors='coerce'
            ).fillna(0).astype(int)

        # Merge predictions with odds data
        for idx, row in odds_df.iterrows():
            player_name = row['Player']
            matching_rows = data_ordered[data_ordered['player'] == player_name]

            if not matching_rows.empty:
                for model_name in models[category]:
                    if model_name not in ['lightgbm', 'linear_model']:
                        continue

                    col_name = f'{category}_{model_name}'
                    if col_name in matching_rows.columns:
                        prediction_value = matching_rows[col_name].values[0]
                        odds_df.at[idx, col_name] = prediction_value

                        # Determine betting recommendation
                        actual_value = float(row[key])
                        recommendation = 'Over' if prediction_value > actual_value else 'Under'
                        odds_df.at[idx, f'recommendation_{category}_{model_name}'] = recommendation
                    else:
                        print(f"Missing column {col_name} for {player_name} in {category}")
            else:
                print(f"Warning: No match found for {player_name} in category {category}")

        # Upload predictions to BigQuery
        table_name = f'miscellaneous-projects-444203.capstone_data.{key}_predictions'
        pandas_gbq.to_gbq(odds_df, table_name, project_id='miscellaneous-projects-444203', credentials=credentials if not local else None, if_exists='append')
        print(f"Successfully uploaded {key} predictions.")


def run_predictions():
    """Runs the full prediction pipeline from data gathering to model inference."""
    print("Running game predictions...")

    data = gather_data_to_model()
    games = scrape_roster(data)

    if games.empty:
        print("No games found today. Exiting...")
        return

    full_data, odds_data = recent_player_data(games)

    if full_data is None or odds_data is None:
        print("Failed to retrieve necessary data. Exiting...")
        return

    predict_games(full_data, odds_data)


