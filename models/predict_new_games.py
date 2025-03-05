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
    name = name.replace(".", "")# Remove periods

    # Known name changes (add more as needed)
    name_corrections = {
        "alexandre sarr":"alex sarr",
        "jimmy butler": "jimmy butler iii",
        'nicolas claxton':'nic claxton',
          'kenyon martin jr':'kj martin',
          'carlton carrington':'bub carrington', # Example name change
          'ron holland ii':'ronald holland ii'
    }

    # Apply corrections if the name exists in the dictionary
    return name_corrections.get(name, name)  # Default to original name if no correction found


def gather_data_to_model():
    query = """
    select team,opponent,date
    from `capstone_data.schedule`
    where date = current_date('America/Los_Angeles')
    """
    team_mapping = {'WAS':'WSH',
                    'UTA':'UTAH',
                    'NOP':'NO'}

    try:
        credentials = service_account.Credentials.from_service_account_file('/home/aportra99/scraping_key.json') #For Google VM
        local = False
    except FileNotFoundError:
        print("File not found continuing as if on local")
        local = True
        credentials = False

    if not local:
        teams_playing = [team_mapping.get(team,team) for team in pd.DataFrame(pandas_gbq.read_gbq(query,project_id='miscellaneous-projects-444203',credentials=credentials))['team']]
        opponents = [team_mapping.get(opponent,opponent) for opponent in pd.DataFrame(pandas_gbq.read_gbq(query,project_id='miscellaneous-projects-444203',credentials=credentials))['opponent']]
    else:
        teams_playing = [team_mapping.get(team,team) for team in pd.DataFrame(pandas_gbq.read_gbq(query,project_id='miscellaneous-projects-444203'))['team']]
        opponents = [team_mapping.get(opponent,opponent) for opponent in pd.DataFrame(pandas_gbq.read_gbq(query,project_id='miscellaneous-projects-444203'))['opponent']]

    data = pd.DataFrame(data={'team':teams_playing,'opponent':opponents})

    return data



def scrape_roster(data):

    print('pulling rosters')
    driver = model_utils.establish_driver()

    teams_playing = data['team']
    opponents = data['opponent']
    teams = []
    players = []
    opponent = []

    for team,opp in zip(teams_playing,opponents):

        url = f'https://www.espn.com/nba/team/roster/_/name/{team}/'
        
        driver.get(url)
        time.sleep(5)
        rows = driver.find_elements(By.XPATH, "//tbody[@class = 'Table__TBODY']/tr")

        for row in rows:

            name_element = row.find_element(By.XPATH,'.//td[2]/div/a')
            name_text = name_element.text

            teams.append(team)
            opponent.append(opp)
            players.append(name_text)
    
    driver.quit()
    
    team_mapping = {'WSH':'WAS',
                        'UTAH':'UTA',
                        'NO':'NOP'} 

    teams = [team_mapping.get(team,team) for team in teams]
    opponent = [team_mapping.get(team,team) for team in opponent]

    games = pd.DataFrame(data = {'player':players,'team':teams,'opponent':opponent})

    return games

def pull_odds():

    tables = ['points','rebounds','assists','threes_made']

    odds_data = {}

    try:
        credentials = service_account.Credentials.from_service_account_file('/home/aportra99/scraping_key.json') #For Google VM
        local = False
    except FileNotFoundError:
        print("File not found continuing as if on local")
        local = True
        credentials = False

    for table in tables:
        odds_query=(
        f"""
        select * 
        from `capstone_data.player_{table}_odds`
        where date(Date_Updated) = current_date('America/Los_Angeles')
        """)
        if local:    
            odds_data[table] = pd.DataFrame(pandas_gbq.read_gbq(odds_query,project_id='miscellaneous-projects-444203'))
        else: 
            odds_data[table] = pd.DataFrame(pandas_gbq.read_gbq(odds_query,project_id='miscellaneous-projects-444203',credentials=credentials))
        odds_data[table]['Player'] = odds_data[table]['Player'].apply(clean_player_name)
        
    return odds_data 


def recent_player_data(games):

    players = games['player'].unique()
    teams = games['team'].unique()
    opponents = games['opponent'].unique()
    
    players = [clean_player_name(player) for player in players]
    games = games.rename(columns={'opponent':'matchup'})

    today = date.today()

    if today.month >= 10:
        season = today.year
    else:
        season = today.year - 1

    existing_players_query = f"""
    SELECT DISTINCT player 
    FROM `capstone_data.player_prediction_data_partitioned`
    where season_start_year = 2024"""

    try:
        credentials = service_account.Credentials.from_service_account_file('/home/aportra99/scraping_key.json') #For Google VM
        local = False
    except FileNotFoundError:
        print("File not found continuing as if on local")
        local = True
        credentials = False

    if local:
        existing_players_df = pandas_gbq.read_gbq(existing_players_query, project_id="miscellaneous-projects-444203")
    else:
        existing_players_df = pandas_gbq.read_gbq(existing_players_query, project_id="miscellaneous-projects-444203",credentials=credentials)
        
    # Convert to a set for fast lookup
    existing_players_set = set(existing_players_df['player'])
    existing_players_set = [clean_player_name(player) for player in existing_players_set]
    # Filter players list to only include those in BigQuery
    filtered_players = [player for player in players if player in existing_players_set]

    if not filtered_players:
        print("No valid players found in the dataset.")
    else:
        # Now run the query with only valid players
        player_query = f"""
        WITH RankedGames AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY player ORDER BY game_date DESC) AS game_rank
            FROM `capstone_data.player_prediction_data_partitioned`
            WHERE lower(player) IN ({','.join([f'"{player}"' for player in filtered_players])})
        )
        SELECT *
        FROM RankedGames
        where game_rank <= 1
        ORDER BY player, game_date DESC;
        """
        
        opponent_query = f"""
        WITH RankedGames AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY team ORDER BY game_date DESC) AS game_rank
            FROM `capstone_data.team_prediction_data_partitioned`
            WHERE team IN ({','.join([f'"{opponent}"' for opponent in opponents])}) and season_start_year = {season}
        )
        SELECT *
        FROM RankedGames
        where game_rank <= 1
        ORDER BY team, game_date DESC;
        """

        team_query = f"""
        WITH RankedGames AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY team ORDER BY game_date DESC) AS game_rank
            FROM `capstone_data.team_prediction_data_partitioned`
            WHERE team IN ({','.join([f'"{team}"' for team in teams])}) and season_start_year = {season}
        )
        SELECT *
        FROM RankedGames
        where game_rank <= 1
        ORDER BY team, game_date DESc;
        """

        games['player'] = games['player'].apply(clean_player_name)

        if local:
            player_data = pd.DataFrame(pandas_gbq.read_gbq(player_query,project_id='miscellaneous-projects-444203'))
            opponent_data = pd.DataFrame(pandas_gbq.read_gbq(opponent_query,project_id='miscellaneous-projects-444203'))
            team_data = pd.DataFrame(pandas_gbq.read_gbq(team_query,project_id='miscellaneous-projects-444203'))
        else:
            player_data = pd.DataFrame(pandas_gbq.read_gbq(player_query,project_id='miscellaneous-projects-444203',credentials=credentials))
            opponent_data = pd.DataFrame(pandas_gbq.read_gbq(opponent_query,project_id='miscellaneous-projects-444203',credentials=credentials))
            team_data = pd.DataFrame(pandas_gbq.read_gbq(team_query,project_id='miscellaneous-projects-444203',credentials=credentials))

        player_data['player'] = player_data['player'].apply(clean_player_name)
        opponent_data = opponent_data.rename(columns={
        col: ('matchup' if col == 'team' else 'game_id' if col == 'game_id' else f'opponent_{col}') for col in team_data.columns})

        full_data = games.merge(player_data, on = ['player'], how = 'inner',suffixes=('','remove'))
        full_data = full_data.merge(opponent_data,on = ['matchup'],how = 'inner',suffixes=('','remove'))
        full_data = full_data.merge(team_data, on = ['team'],how = 'inner',suffixes=('','remove'))
        full_data.drop([column for column in full_data.columns if 'remove' in column],axis = 1 , inplace=True) 
        full_data.drop([column for column in full_data.columns if '_1' in column],axis = 1 , inplace=True)
        
        full_data['player'] = full_data['player'].apply(clean_player_name)
        odds_data=pull_odds()

        return full_data,odds_data



def predict_games(full_data,odds_data):
    models = joblib.load('/home/aportra99/Capstone/models/models.pkl')
    for key in odds_data.keys():

        data_ordered = full_data.sort_values('game_date')
        
        data_ordered = data_ordered[data_ordered['player'].isin(odds_data[key]['Player'])]
        print(len(data_ordered))
        data_ordered['pts_per_min_3gm'] = data_ordered['pts_3gm_avg']/data_ordered['min_3gm_avg']
        data_ordered['pts_per_min_season'] = data_ordered['pts_season']/data_ordered['min_season']
        data_ordered['pts_per_min_momentum'] = data_ordered['pts_per_min_3gm'] - data_ordered['pts_per_min_season']

        data_ordered['3pm_per_min_3gm'] = data_ordered['3pm_3gm_avg']/data_ordered['min_3gm_avg']
        data_ordered['3pm_per_min_season'] = data_ordered['3pm_season']/data_ordered['min_season']
        data_ordered['3pm_per_min_momentum'] = data_ordered['3pm_per_min_3gm'] - data_ordered['3pm_per_min_season'] 

        data_ordered['reb_per_min_3gm'] = data_ordered['reb_3gm_avg']/data_ordered['min_3gm_avg']
        data_ordered['reb_per_min_season'] = data_ordered['reb_season']/data_ordered['min_season']
        data_ordered['reb_per_min_momentum'] = data_ordered['3pm_per_min_3gm'] - data_ordered['reb_per_min_season']
        
        # Ensure data is sorted correctly for chronological calculations
        data_ordered = data_ordered.sort_values(by=['player', 'season', 'game_date'])

        # Fill missing values for early season games
        # data_ordered = data_ordered.fillna(0,downcast='infer')
        try:
            credentials = service_account.Credentials.from_service_account_file('/home/aportra99/scraping_key.json') #For Google VM
            local = False
        except FileNotFoundError:
            print("File not found continuing as if on local")
            local = True
            credentials = False

# Load the models
        if key == 'points':
            category = 'pts'
        elif key == 'rebounds':
            category = 'reb'
        elif key == 'assists':
            category = 'ast'
        else:
            category = '3pm'

        for model in models[category]:
            if model.lower() != 'xgboost' and model.lower() != 'sarimax' and model.lower() != 'mlp' and model.lower() != 'random_forest':
                features = [f.replace("\n", "").strip() for f in models[category][model].feature_names_in_]
                if set(features).issubset(data_ordered.columns):  # Ensure all required features exist
                    y_pred = models[category][model].predict(data_ordered[features])
                    data_ordered[f'{category}_{model}'] = y_pred
                else:
                    print(f"Skipping model {model} for {category}: Missing features")

                # elif model.lower() == 'xgboost':
                #     features = models[category][model].get_booster().feature_names
                #     scaler = StandardScaler()
                #     scaled_data = scaler.fit_transform(data_ordered[features])

                #     scaled_data = pd.DataFrame(scaled_data,columns=features)
                #     if set(features).issubset(data_ordered.columns):  # Ensure all required features exist
                #         y_pred = models[category][model].predict(scaled_data)
                #         data_ordered[f'{category}_{model}'] = y_pred
                # elif model.lower() == 'mlp':
                #     features = [f.replace("\n", "").strip() for f in models[category][model].feature_names_in_]
                #     scaler = StandardScaler()
                #     scaled_data = scaler.fit_transform(data_ordered[features])
                #     scaled_data = pd.DataFrame(scaled_data,columns=features)
                #     if set(features).issubset(data_ordered.columns):  # Ensure all required features exist
                #         y_pred = models[category][model].predict(scaled_data)
                #         data_ordered[f'{category}_{model}'] = y_pred
                #     else:
                #         print(f"Skipping XGBoost model {model} for {category}: Missing features")

    # Standardizing player names
        data_ordered['player'] = data_ordered['player'].apply(clean_player_name)
        players = data_ordered['player'].tolist()

        data = odds_data[key]

        data['Over'] = pd.to_numeric(
            data['Over'].astype(str).str.replace('−', '-', regex=False).str.replace('+', '', regex=False),
            errors='coerce'
        ).fillna(0).astype(int)

        data['Under'] = pd.to_numeric(
            data['Under'].astype(str).str.replace('−', '-', regex=False).str.replace('+', '', regex=False),
            errors='coerce'
        ).fillna(0).astype(int)

        # Standardize player names
        data['Player'] = data['Player'].str.strip().str.lower()
        data['Player'] = data['Player'].apply(clean_player_name)




        # Assign model predictions to data
        for category in models.keys():
            for idx, row in data.iterrows():
                
                player_name = row['Player']

 
                        # Find matching player rows in data_ordered
               
                matching_rows = data_ordered[data_ordered['player'] == player_name]
            
                if not matching_rows.empty:
                    for model in models[category]:
                        if model not in ['lightgbm','linear_model']:
                            continue  # Skip SARIMAX
                        
                        col_name = f'{category}_{model}'
                        if col_name in matching_rows.columns:
                            data.at[idx, col_name] = matching_rows[col_name].values[0]  # Assign the first matching value
                            prediction_value = pd.to_numeric(matching_rows[col_name].values[0])

                            data.at[idx, col_name] = prediction_value  # Assign the first matching value
                            # Add recommendation logic for each category and model
                            actual_value = float(row[key])  # Get the actual value from the category (points, rebounds, etc.)

                            # Compare predicted value with the actual data
                            if prediction_value > actual_value:
                                recommendation = 'Over'
                            else:
                                recommendation = 'Under'
                            # Store recommendation in new column
                            recommendation_col = f'recommendation_{category}_{model}'
                            data.at[idx, recommendation_col] = recommendation

                        else:
                            print(f"Missing column {col_name} for {player_name} in {category}")
                else:   
                    print(f"Warning: No match found for player {player_name} in category {category}")
            
        if local:
            pandas_gbq.to_gbq(data, f'miscellaneous-projects-444203.capstone_data.{key}_predictions', if_exists='append')
        else:
            pandas_gbq.to_gbq(data, f'miscellaneous-projects-444203.capstone_data.{key}_predictions', credentials=credentials, if_exists='append')
            print(f"✅ Successfully uploaded {key} predictions!")

def run_predictions():
    data = gather_data_to_model()
    games = scrape_roster(data)
    full_data,odds_data= recent_player_data(games)
    predict_games(full_data,odds_data)
