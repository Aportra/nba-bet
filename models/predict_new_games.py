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

import pickle as pkl
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
    existing_players_df = pandas_gbq.read_gbq(existing_players_query, project_id="miscellaneous-projects-444203")

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

    player_data = pd.DataFrame(pandas_gbq.read_gbq(player_query,project_id='miscellaneous-projects-444203'))
    opponent_data = pd.DataFrame(pandas_gbq.read_gbq(opponent_query,project_id='miscellaneous-projects-444203'))
    team_data = pd.DataFrame(pandas_gbq.read_gbq(team_query,project_id='miscellaneous-projects-444203'))

    opponent_data = opponent_data.rename(columns={
    col: ('matchup' if col == 'team' else 'game_id' if col == 'game_id' else f'opponent_{col}') for col in team_data.columns})

    full_data = games.merge(player_data, on = ['player','team'], how = 'inner',suffixes=('','remove'))
    full_data = games.merge(opponent_data,on = ['matchup'],how = 'inner',suffixes=('','remove'))
    full_data = games.merge(team_data, on = ['team'],how = 'inner',suffixes=('','remove'))
    full_data.drop([column for column in full_data.columns if 'remove' in column],axis = 1 , inplace=True) 
    full_data.drop([column for column in full_data.columns if '_1' in column],axis = 1 , inplace=True)


    return full_data


# def predict_games(player_data):
#     for 



def predict_games():
    
    # Load the models
    with open('models/models.pkl', "rb") as file:
        models = pkl.load(file)

    # Check the loaded models
      # Check if it's a dict, list, or something else
    print(models.keys() if isinstance(models, dict) else models)  # Print model names if dict

predict_games()
# data = gather_data_to_model()
# games = scrape_roster(data)
# full_data = recent_player_data(games)
