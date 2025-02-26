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

    if not filtered_players:
        print("No valid players found in the dataset.")
    else:
        # Now run the query with only valid players
        player_query = f"""
        WITH RankedGames AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY player ORDER BY game_date DESC) AS game_rank
            FROM `capstone_data.player_prediction_data`
            WHERE player IN ({','.join([f'"{player}"' for player in filtered_players])})
        )
        SELECT *
        FROM RankedGames
        WHERE game_rank <= 1
        ORDER BY player, game_date DESC;
        """
    
    opponent_query = f"""
    WITH RankedGames AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY team ORDER BY game_date DESC) AS game_rank
        FROM `capstone_data.team_prediction_data`
        WHERE team IN ({','.join([f'"{opponent}"' for opponent in opponents])})
    )
    SELECT *
    FROM RankedGames
    WHERE game_rank <= 1
    ORDER BY team, game_date DESC;
    """

    team_query = f"""
    WITH RankedGames AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY team ORDER BY game_date DESC) AS game_rank
        FROM `capstone_data.team_prediction_data`
        WHERE team IN ({','.join([f'"{team}"' for team in teams])})
    )
    SELECT *
    FROM RankedGames
    WHERE game_rank <= 1
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

    full_data = games.merge(player_data, on = ['player','team'], how = 'inner',suffixes=('','remove'))
    full_data = games.merge(opponent_data,on = ['matchup'],how = 'inner',suffixes=('','remove'))
    full_data = games.merge(team_data, on = ['team'],how = 'inner',suffixes=('','remove'))
    full_data.drop([column for column in full_data.columns if 'remove' in column],axis = 1 , inplace=True) 
    full_data.drop([column for column in full_data.columns if '_1' in column],axis = 1 , inplace=True)


    return full_data,filtered_players,teams,opponents

def pull_odds(games):
    players = games['players'].unique()

    tables = ['points','rebounds','assists','threes_made']

    odds_data = {col:[] for col in tables}


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
        from `player_{table}_odds
        where date(Date_Updated) = {str(date.today().date())}
        """)
        if local:    
            odds_data[table].append(pd.DataFrame(pandas_gbq.read_gbq(odds_query,project_id='miscellaneous-projects-444203')))
       else: 
            odds_data[table].append(pd.DataFrame(pandas_gbq.read_gbq(odds_query,project_id='miscellaneous-projects-444203',credentials=credentials)))
    return odds_data 

def predict_games():
    data = gather_data_to_model()
    games = scrape_roster(data)
    full_data,filtered_players,teams,opponents = recent_player_data(games)
    

    try:
        credentials = service_account.Credentials.from_service_account_file('/home/aportra99/scraping_key.json') #For Google VM
        local = False
    except FileNotFoundError:
        print("File not found continuing as if on local")
        local = True
        credentials = False

    player_query = f"""
    WITH RankedGames AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY player ORDER BY game_date DESC) AS game_rank
        FROM `capstone_data.player_modeling_data`
        WHERE player IN ({','.join([f'"{player}"' for player in filtered_players])})
    )
    SELECT *
    FROM RankedGames
    WHERE game_rank > 1 and game_rank <= 10
    ORDER BY player, game_date DESC;
    """
    
    opponent_query = f"""
    WITH RankedGames AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY team ORDER BY game_date DESC) AS game_rank
        FROM `capstone_data.team_modeling_data`
        WHERE team IN ({','.join([f'"{opponent}"' for opponent in opponents])})
    )
    SELECT *
    FROM RankedGames
    WHERE game_rank > 1 and game_rank <= 10
    ORDER BY team, game_date DESC;
    """

    team_query = f"""
    WITH RankedGames AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY team ORDER BY game_date DESC) AS game_rank
        FROM `capstone_data.team_modeling_data`
        WHERE team IN ({','.join([f'"{team}"' for team in teams])})
    )
    SELECT *
    FROM RankedGames
    WHERE game_rank > 1 and game_rank <= 10
    ORDER BY team, game_date DESC;
    """

    if local:
        team_data = pd.DataFrame(pandas_gbq.read_gbq(team_query,project_id='miscellaneous-projects-444203'))
        player_data = pd.DataFrame(pandas_gbq.read_gbq(player_query,project_id='miscellaneous-projects-444203'))
        opponents_data = pd.DataFrame(pandas_gbq.read_gbq(opponent_query,project_id='miscellaneous-projects-444203'))
    else:
        team_data = pd.DataFrame(pandas_gbq.read_gbq(team_query,project_id='miscellaneous-projects-444203',credentials=credentials))
        player_data = pd.DataFrame(pandas_gbq.read_gbq(player_query,project_id='miscellaneous-projects-444203',credentials=credentials))
        opponents_data = pd.DataFrame(pandas_gbq.read_gbq(opponent_query,project_id='miscellaneous-projects-444203',credentials=credentials))

    opponents_data = opponents_data.rename(columns={
    col: ('matchup' if col == 'team' else 'game_id' if col == 'game_id' else f'opponent_{col}') for col in team_data.columns})

    sarima_data = player_data.merge(opponents_data,on = ['matchup'],how = 'inner',suffixes=('','remove'))
    sarima_data = team_data.merge(team_data, on = ['team'],how = 'inner',suffixes=('','remove'))
    sarima_data.drop([column for column in full_data.columns if 'remove' in column],axis = 1 , inplace=True) 
    sarima_data.drop([column for column in full_data.columns if '_1' in column],axis = 1 , inplace=True)

    sarima_data = sarima_data.sort_values(by='game_date',ascending=True)
    sarima_data = sarima_data.set_index('game_date')
    sarima_data = sarima_data.asfreq(pd.infer_freq(sarima_data['game_date']))

    # Load the models
    models = joblib.load('models/models.pkl')

    for category in models.keys():
        for model in models[category]:
            if model.lower() != 'xgboost' and model.lower() != 'sarimax':

                features = models[category][model].feature_names_in_
                data = full_data[features]    
                y_pred = models[category][model].predict(data)
                full_data[f'{category}_{model}'] = y_pred

            elif model.lower() == 'xgboost':

                features = models[category][model].get_booster().feature_names
                data = full_data[features]
                y_pred = models[category][model].predict(data)
                full_data[f'{category}_{model}'] = y_pred

            if model.lower() == 'sarimax':

                order = models[category][model]['order']
                seasonal_order = models[category][model]['seasonal_order']
                exog_columns = models[category][model]['exog_columns']
                
                exog_data = full_data[exog_columns]

                sarimax_model = SARIMAX(
                    sarima_data[category],
                    order=order,
                    seasonal_order=seasonal_order,
                    exog_columns=exog_data
                )
                
                data = full_data[exog_columns]
                data = data.sort_values(by='game_date',ascending=True)
                data = data.set_index('game_date')
                
                forecast_steps = len(filtered_players)
                
                pred = sarimax_model.get_forecast(steps=forecast_steps, exog=exog_data if exog_data else None)

                full_data[f'{category}_{model}'] = pred
        
    odds_data= pull_odds(games)
    
    for key in odds_data.key():
        data = odds_data[key]
        data['Over'] = data['Over'].str.replace('+','',regex = False).astype(int)
        data['Under'] = data['Under'].astype(int)
        filtered_full_data = full_data[full_data['players'].isin(data['players'])]

        for category in models.keys():
            for model in models[category]:
                data[f'{category}_{model}'] = filtered_full_data[f'{category}_{model}']
        if local:
            pandas_gbq.to_gbq(data,f'miscellaneous-projects-444203.capstone_data.{key}_predictions')
        else: 
            pandas_gbq.to_gbq(data,f'miscellaneous-projects-444203.capstone_data.{key}_predictions',credentials=credentials)
            
predict_games()
