from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from google.oauth2 import service_account
from datetime import datetime as date
from models import model_utils

import joblib
import pandas as pd
import pandas_gbq
import time
import unicodedata
import os

current_wd = os.getcwd()
print(current_wd)

def clean_player_name(name):
    """Standardizes player names by removing special characters and handling known name variations."""
    name = name.lower().strip()  # Convert to lowercase & remove extra spaces
    name = name.replace(".", "")  # Remove periods
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('utf-8')

    # Remove special characters (apostrophes, dashes, etc.) 
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


def scrape_roster(input_data):
    """Scrapes team rosters for today's games from ESPN."""

    teams = tuple(input_data['team'])

    query = f"""Select
    distinct
        team_id,
        team
    from `capstone_data.team_prediction_data_partitioned`
    where team in {teams}"""
    
    print(query)

    try:
        credentials = service_account.Credentials.from_service_account_file("/home/aportra99/scraping_key.json")
        local = False
    except FileNotFoundError:
        print("File not found, continuing as if on local.")
        local = True
        credentials = None

    teams_data = fetch_bigquery_data(query,credentials=credentials)
    print(teams_data.columns)
    team_dict = {teams_data['team_id'][i]: teams_data['team'][i] for i in range(len(teams_data['team_id']))}
    opponent_dict = {input_data['team'][i]:input_data['opponent'][i] for i in range(len(input_data['team']))}
    full_dicts = []

    print(opponent_dict.keys())
    for i in range(len(teams_data['team_id'])):
        url = f"https://stats.nba.com/stats/commonteamroster?LeagueID=&Season=2025-26&TeamID={teams_data['team_id'][i]}"
        req = model_utils.establish_requests(url)
        result = req.json()['resultSets'][0]
        headers = [result['headers'][i].lower() for i in range(len(result['headers']))]
        cols = {result['headers'][x].lower():[] for x in range(len(result['headers']))}
        data = req.json()['resultSets'][0]['rowSet']
        cols['team'] = []
        cols['opponent'] = []

        for i in range(len(data)):
            for y in range(len(data[i])):
                print(headers[y])
                if headers[y] == 'teamid':
                    cols['team'].append(team_dict[data[i][y]])
                    cols['opponent'].append(opponent_dict[team_dict[data[i][y]]])
                    cols[headers[y]].append(data[i][y])
                else:
                    cols[headers[y]].append(data[i][y])
        full_dicts.append(cols)

    final_dict = {key: [] for key in full_dicts[0].keys()}

    for i in range(len(full_dicts)):
        final_dict.update(full_dicts[i])
    df = pd.DataFrame(final_dict)

    return df


def pull_odds():
    """Fetches the latest player odds from BigQuery."""
    table = "points"
    odds_data = {}

    try:
        credentials = service_account.Credentials.from_service_account_file("/home/aportra99/scraping_key.json")
        local = False
    except FileNotFoundError:
        print("File not found, continuing as if on local.")
        local = True
        credentials = None

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

    today = date.today()
    season =int(today.year if today.month >= 10 else today.year - 1)
    print(season)
    existing_players_query = """
    SELECT DISTINCT player 
    FROM `capstone_data.player_prediction_data_partitioned`
    WHERE season_start_year = 2024
    """
    try:
        credentials = service_account.Credentials.from_service_account_file("/home/aportra99/scraping_key.json")
    except FileNotFoundError:
        credentials=None
    odds_data = pull_odds()

    existing_players = fetch_bigquery_data(existing_players_query,credentials=credentials)
    existing_players_set = set(existing_players["player"].apply(clean_player_name))
    # filtered_players = [player for player in existing_players_set]
    filtered_players = set()
    for table in odds_data:
        filtered_players.update(odds_data[table]['Player'].unique())

    print(f'player length:{len(filtered_players)}')
    print(f'team length: {len(games['team'].unique())}')
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
            AND season_start_year = 2024
        )
        SELECT * except(game_rank)
        FROM RankedGames
        where game_rank = 1;
    """,
    "team_data": f"""
        WITH RankedGames AS (
            SELECT *, 
                ROW_NUMBER() OVER (PARTITION BY team ORDER BY game_date DESC) AS game_rank
            FROM `capstone_data.team_prediction_data_partitioned`
            WHERE team IN ({','.join([f'"{team}"' for team in games["team"].unique()])}) 
            AND season_start_year = 2024
        )
        SELECT * except(game_rank)
        FROM RankedGames
        where game_rank = 1;
    """
}
    print(queries['player_data'])

    
    # Fetch player, opponent, and team data
    player_data, team_data = [fetch_bigquery_data(queries[q],credentials=credentials) for q in queries]

    print('queries complete')
    # Standardize player names in player_data
    print('cleaning names')
    player_data["player"] = player_data["player"].apply(clean_player_name)
    len(f'player_data length: {player_data}')
    len(f'team_data length:{team_data}')

    team_data_1 = team_data.copy()

    team_data_1.rename(columns={'team':'opponent'},inplace=True)
    pd.set_option('display.max_rows', None)  # Show all rows
    pd.set_option('display.max_columns', None)  # Show all columns
    pd.set_option('display.expand_frame_repr', False)
    # Merge datasets while keeping only necessary columns
    print('merging data')
    full_data = (player_data.merge(games, on="player", how="inner", suffixes=("", "_remove")))
    full_data = full_data.merge(team_data, on = 'team',how = 'left', suffixes = ("","_remove"))
    full_data = full_data.merge(team_data_1, on= 'opponent', how = 'left',suffixes = ("","_opponent"))



    print('print full data post merge',len(full_data['player'].unique()))
    print("Columns with NaN after merge:", full_data.isna().sum())
    # Drop duplicate or unnecessary columns

    print("Columns with NaN after merge after drop:", full_data.isna().sum())
  

    # players_to_drop = full_data[full_data.isnull().any(axis=1)]
    # print("🚨 Dropping these players due to NaN values:")
    # print(players_to_drop[['player', 'team']])
# Save the players being dropped for debugging

    # Drop NaNs
    print(full_data[['to_season','to_3gm_avg']])
    full_data.dropna(axis=1,inplace=True)
    
    nan_players = full_data[full_data.isnull().any(axis=1)]


    return full_data, odds_data




def predict_games(full_data, odds_data):
    """Predicts NBA player stats using pre-trained models and compares with betting odds."""
    print('loading models...')
    # Load models
    # models = joblib.load('/home/aportra99/nba-bet/models/models.pkl')
    full_data = full_data
    odds = {}
    lowest_data = {}
    models = joblib.load(f'{current_wd}/models/models.pkl')
    for key, odds_df in odds_data.items():
  
        # Filter relevant players
        data_ordered = full_data[full_data['player'].isin(odds_df['Player'])].copy()

        print(f"Filtered {len(data_ordered)} players for {key} predictions.")
        # Players in odds_df that are not in full_data
        players_not_in_full_data = set(odds_df['Player']) - set(full_data['player'])

        # Players in odds_df that are not in data_ordered
        players_not_in_data_ordered = set(odds_df['Player']) - set(data_ordered['player'])

        # Display results
        print("Players in odds_df but not in full_data:", players_not_in_full_data)
        print("Players in odds_df but not in data_ordered:", players_not_in_data_ordered)


        # Ensure chronological order for calculations
        data_ordered.sort_values(by=['player', 'game_date'], inplace=True)

        latest_rows = data_ordered.groupby('player', as_index=False).tail(1)
        # print(latest_rows)
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
            "points": "pts"
            # "rebounds": "reb",
            # "assists": "ast",
            # "threes_made": "3pm"
        }
        category = category_mapping[key]

        # Run predictions using trained models
        for model_name, model in models[category].items():
            print(category)
            print(model_name)
            if model_name.lower() not in ["xgboost", "sarimax", "mlp", "random_forest"]:
                features = [f.strip() for f in model.feature_names_in_]

                if set(features).issubset(latest_rows.columns):
                    latest_rows[f'{category}_{model_name}'] = model.predict(latest_rows[features])
                else:
                    missing = set(features) - set(latest_rows.columns)
                    print(f"Skipping {model_name} for {category}: Missing features: {missing}")


        # Convert betting odds to numeric values
        for col in ['Over', 'Under']:
            odds_df[col] = pd.to_numeric(
                odds_df[col].astype(str).str.replace('−', '-', regex=False).str.replace('+', '', regex=False),
                errors='coerce'
            ).fillna(0).astype(int)

        # Merge predictions with odds data
        for idx, row in odds_df.iterrows():
            player_name = row['Player']
            matching_rows = latest_rows[latest_rows['player'] == player_name]

            if not matching_rows.empty:
                for model_name in models[category]:
                    if model_name not in ['lightgbm', 'linear_model']:
                        continue

                    col_name = f'{category}_{model_name}'
                    if col_name in matching_rows.columns:
                        print('0',matching_rows[col_name].values[0],matching_rows['game_date'].values[0],matching_rows['player'].values[0])
                        prediction_value = matching_rows[col_name].values[0]
                        print(col_name)
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
        odds_df.dropna(axis=0,inplace = True)
        odds_df.drop_duplicates(keep='first',inplace=True)
        pandas_gbq.to_gbq(odds_df, table_name, project_id='miscellaneous-projects-444203', credentials=credentials if not local else None, if_exists='append')
        odds[category] = odds_df
        lowest_data[category] = latest_rows
        print(f"Successfully uploaded {key} predictions.")

    return lowest_data,odds

#best bets tab added in

def classification(lowest_data,odds):
        
    ensemble = joblib.load(f'{current_wd}/models/meta_model.pkl')
    models = joblib.load(f'{current_wd}/models/classification_models.pkl')

    # Display settings
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.max_rows', None)

    # Auth
    try:
        credentials = service_account.Credentials.from_service_account_file('/home/aportra99/scraping_key.json')
        local = False
    except FileNotFoundError:
        print("File not found, running in local mode.")
        local = True
        credentials = None

    # categories = ['pts', 'reb', 'ast', '3pm']
    categories = ['pts']
    # lines = ['points', 'rebounds', 'assists', 'threes_made']
    lines = ['points']

    # Backup
    odds_raw = {cat: df.copy() for cat, df in odds.items()}

    for cat, line in zip(categories, lines):
        print(f"\n=== Category: {cat.upper()} ===")

        # Format odds
        for col in ['Over', 'Under']:
            odds[cat][col] = pd.to_numeric(
                odds[cat][col].astype(str).str.replace('−', '-', regex=False).str.replace('+', '', regex=False),
                errors='coerce'
            ).fillna(0).astype(int)

        odds[cat].rename(columns={'Player': 'player'}, inplace=True)

        # Check required model columns exist
        linear_col = f'{cat}_linear_model'
        lightgbm_col = f'{cat}_lightgbm'


        print(f"Columns available for {cat}: {list(odds[cat].columns)}")
        print(f"Looking for: {linear_col}, {lightgbm_col}")

        if linear_col not in odds[cat].columns or lightgbm_col not in odds[cat].columns:
            print(f"Skipping {cat} due to missing model cols.")
            continue
        # Ensemble score
        coef_linear, coef_lightgbm = ensemble[cat].coef_
        odds[cat][f'{cat}_ensemble'] = (
            odds[cat][linear_col] * coef_linear +
            odds[cat][lightgbm_col] * coef_lightgbm
        )

        # Merge with features
        all_data = odds[cat].merge(lowest_data[cat], on='player', how='inner')
        all_data[line] = pd.to_numeric(all_data[line], errors='coerce')

        # Recalc delta
        all_data[f'{cat}_delta'] = all_data[f'{cat}_ensemble'] - all_data[line]

        #Load model + thresholds
        model_dict = models[cat]
        clf = model_dict['Fitted_Model']
        threshold_over = model_dict['Over_Threshold']
        threshold_under = model_dict['Under_Threshold']

        # Ensure all expected features exist
        expected_features = list(clf.feature_names_in_)
        
        for col in expected_features:
            if col not in all_data.columns:
                all_data[col] = 0.0

        # Slice and match expected order
        filtered_data = all_data[expected_features].astype(float)
        assert list(filtered_data.columns) == list(clf.feature_names_in_)

        #Predict
        try:
            proba = clf.predict_proba(filtered_data.to_numpy())[:, 1]
        except Exception as e:
            print(f"Error predicting for {cat}: {e}")
            continue

        def classify(p): return 'Over' if p > threshold_over else 'Under' if p < threshold_under else 'No Bet Recommendation'

        all_data['proba'] = proba
        all_data['recommendation'] = all_data['proba'].apply(classify)

        for col in ['recommendation', 'proba']:
            if col in odds[cat].columns:
                odds[cat].drop(columns=col, inplace=True)

        # Clean up from all_data
        merged_predictions = all_data[['player', 'recommendation', 'proba']].drop_duplicates(subset='player')

        # Merge cleanly
        odds[cat] = odds[cat].merge(merged_predictions, on='player', how='left')


        #Optional sanity check
        if odds[cat].duplicated(subset='player').any():
            print(f"Duplicates found in {cat} after merge!")
        odds[cat].drop(columns =[f'{cat}_linear_model',f'{cat}_lightgbm',f'recommendation_{cat}_linear_model',f'recommendation_{cat}_lightgbm',f'{cat}_ensemble'],inplace= True)
        
        odds[cat].dropna(axis=0,inplace=True)
        
        odds[cat].drop_duplicates(keep='first',inplace=True)
        table_name = f'miscellaneous-projects-444203.capstone_data.{cat}_classifications'
        pandas_gbq.to_gbq(odds[cat], table_name, project_id='miscellaneous-projects-444203', credentials=credentials if not local else None, if_exists='append')
        

def run_predictions(matchups):
    """Runs the full prediction pipeline from data gathering to model inference."""
    print("Running game predictions...")

    try:
        games = scrape_roster(matchups)

        full_data, odds_data = recent_player_data(games)

        lowest_data, odds = predict_games(full_data, odds_data)

        classification(lowest_data, odds)
        if full_data is None or odds_data is None:
            print("Failed to retrieve necessary data. Exiting...")
            return
        model_utils.send_email(
            subject="Predictions Ran Sucessfully",
            body=" "
        )
    except Exception as e:
        model_utils.send_email(        
        subject="Predictions Error",
        body=f"Error {e}",
            ) 



