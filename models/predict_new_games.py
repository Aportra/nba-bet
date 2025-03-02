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
from statsmodels.tsa.statespace.sarimax import SARIMAX 

import joblib
import utils
import pandas as pd
import pandas_gbq
import time

def gather_data_to_model():
    query = """
    select team,opponent,date
    from `capstone_data.schedule`
    where date = current_date()
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

    if local:
        teams_playing = [team_mapping.get(team,team) for team in pd.DataFrame(pandas_gbq.read_gbq(query,project_id='miscellaneous-projects-444203',credentials=credentials))['team']]
        opponents = [team_mapping.get(opponent,opponent) for opponent in pd.DataFrame(pandas_gbq.read_gbq(query,project_id='miscellaneous-projects-444203',credentials=credentials))['opponent']]
    else:
        teams_playing = [team_mapping.get(team,team) for team in pd.DataFrame(pandas_gbq.read_gbq(query,project_id='miscellaneous-projects-444203'))['team']]
        opponents = [team_mapping.get(opponent,opponent) for opponent in pd.DataFrame(pandas_gbq.read_gbq(query,project_id='miscellaneous-projects-444203'))['opponent']]

    data = pd.DataFrame(data={'team':teams_playing,'opponent':opponents})

    return data



def scrape_roster(data):

    driver = utils.establish_driver(local = True)

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



def recent_player_data(games):

    players = games['player'].unique()
    teams = games['team'].unique()
    opponents = games['opponent'].unique()

    games = games.rename(columns={'opponent':'matchup'})

    existing_players_query = """
    SELECT DISTINCT player FROM `capstone_data.player_prediction_data`
    """

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

    # Filter players list to only include those in BigQuery
    filtered_players = [player for player in players if player in existing_players_set]
    
    today = date.today().date()
    
    if today.month >= 10:
        season = today.year 

    else: 
        season = today.year - 1

    if not filtered_players:
        print("No valid players found in the dataset.")
    else:
        # Now run the query with only valid players
        player_query = f"""
        WITH RankedGames AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY player ORDER BY game_date DESC) AS game_rank
            FROM `capstone_data.player_prediction_data_partitioned`
            WHERE player IN ({','.join([f'"{player}"' for player in filtered_players])}) and season_start_year = {season}
        )
        SELECT *
        FROM RankedGames
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
        ORDER BY team, game_date DESC;
        """


        if local:
            player_data = pd.DataFrame(pandas_gbq.read_gbq(player_query,project_id='miscellaneous-projects-444203'))
            opponent_data = pd.DataFrame(pandas_gbq.read_gbq(opponent_query,project_id='miscellaneous-projects-444203'))
            team_data = pd.DataFrame(pandas_gbq.read_gbq(team_query,project_id='miscellaneous-projects-444203'))
        else:
            player_data = pd.DataFrame(pandas_gbq.read_gbq(player_query,project_id='miscellaneous-projects-444203',credentials=credentials))
            opponent_data = pd.DataFrame(pandas_gbq.read_gbq(opponent_query,project_id='miscellaneous-projects-444203',credentials=credentials))
            team_data = pd.DataFrame(pandas_gbq.read_gbq(team_query,project_id='miscellaneous-projects-444203',credentials=credentials))

        opponent_data = opponent_data.rename(columns={
        col: ('matchup' if col == 'team' else 'game_id' if col == 'game_id' else f'opponent_{col}') for col in team_data.columns})

        full_data = games.merge(player_data, on = ['player'], how = 'inner',suffixes=('','remove'))
        full_data = full_data.merge(opponent_data,on = ['matchup'],how = 'inner',suffixes=('','remove'))
        full_data = full_data.merge(team_data, on = ['team'],how = 'inner',suffixes=('','remove'))
        full_data.drop([column for column in full_data.columns if 'remove' in column],axis = 1 , inplace=True) 
        full_data.drop([column for column in full_data.columns if '_1' in column],axis = 1 , inplace=True)

        return full_data,filtered_players,teams,opponents

def pull_odds(filtered_players):

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
        where date(Date_Updated) = date('{date.today().strftime('%Y-%m-%d')}') and Player in 
        """)
        if local:    
            odds_data[table] = pd.DataFrame(pandas_gbq.read_gbq(odds_query,project_id='miscellaneous-projects-444203'))
        else: 
            odds_data[table] = pd.DataFrame(pandas_gbq.read_gbq(odds_query,project_id='miscellaneous-projects-444203',credentials=credentials))
    return odds_data 

def predict_games():
    data = gather_data_to_model()
    games = scrape_roster(data)
    full_data,filtered_players,teams,opponents = recent_player_data(games)
    
    data_ordered = full_data.sort_values('game_date')
   
     
    data_ordered['pts_per_min_3gm'] = data_ordered['pts_3gm_avg']/data_ordered['min_3gm_avg']
    data_ordered['pts_per_min_season'] = data_ordered['pts_season']/data_ordered['min_season']
    data_ordered['pts_per_min_momentum'] = data_ordered['pts_per_min_3gm'] - data_ordered['pts_per_min_season']

    data_ordered['3pm_per_min_3gm'] = data_ordered['3pm_3gm_avg']/data_ordered['min_3gm_avg']
    data_ordered['3pm_per_min_season'] = data_ordered['3pm_season']/data_ordered['min_season']
    data_ordered['3pm_per_min_momentum'] = data_ordered['3pm_per_min_3gm'] - data_ordered['3pm_per_min_season'] 

    data_ordered['reb_per_min_3gm'] = data_ordered['reb_3gm_avg']/data_ordered['min_3gm_avg']
    data_ordered['reb_per_min_season'] = data_ordered['reb_season']/data_ordered['min_season']
    data_ordered['reb_per_min_momentum'] = data_ordered['3pm_per_min_3gm'] - data_ordered['reb_per_min_season']

    home_performance = data_ordered[data_ordered['home'] == 1]
    away_performance = data_ordered[data_ordered['away'] == 1]    

    # Ensure data is sorted correctly for chronological calculations
    data_ordered = data_ordered.sort_values(by=['player', 'season', 'game_date'])

    data_ordered.dropna(inplace=True)

    try:
        credentials = service_account.Credentials.from_service_account_file('/home/aportra99/scraping_key.json') #For Google VM
        local = False
    except FileNotFoundError:
        print("File not found continuing as if on local")
        local = True
        credentials = False

   # Load the models
    models = joblib.load('models/models.pkl')

    for category in models.keys():
        for model in models[category]:
            if model.lower() != 'xgboost' and model.lower() != 'sarimax':
                print(f"Processing {model}")
                features = [f.replace("\n", "").strip() for f in models[category][model].feature_names_in_]
                data = data_ordered[features]
                print(data)
                y_pred = models[category][model].predict(data)
                data_ordered[f'{category}_{model}'] = y_pred

            elif model.lower() == 'xgboost':

                features = models[category][model].get_booster().feature_names
                data = data_ordered[features]
                y_pred = models[category][model].predict(data)
                data_ordered[f'{category}_{model}'] = y_pred
        
    odds_data=pull_odds()
    print(data_ordered.columns)
    for key in odds_data.keys():
        data = odds_data[key]

        data['Over'] = pd.to_numeric(data['Over'].astype(str).str.replace('−', '-', regex=False).str.replace('+', '', regex=False),errors='coerce').fillna(0).astype(int)

        data['Under'] = pd.to_numeric(data['Under'].astype(str).str.replace('−', '-', regex=False).str.replace('+', '', regex=False),errors='coerce').fillna(0).astype(int)
        
        filtered_full_data = data_ordered[data_ordered['player'].isin(data['Player'])]
        
        if key == 'points':
            category = 'pts'
        elif key == 'assists':
            category = 'ast'
        elif key == 'threes_made':
            category = '3pm'
        elif key == 'rebounds':
            category = 'reb'
        for model in models[category]:
            if model == 'sarimax':
                continue
            data[f'{category}_{model}'] = filtered_full_data[f'{category}_{model}']

        if local:
            pandas_gbq.to_gbq(data,f'miscellaneous-projects-444203.capstone_data.{key}_predictions',if_exists='replace')
        else: 
            pandas_gbq.to_gbq(data,f'miscellaneous-projects-444203.capstone_data.{key}_predictions',credentials=credentials)
            
predict_games()