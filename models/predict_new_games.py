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
from scraping_data.utils import establish_driver

import pandas as pd
import pandas_gbq
import time

def gather_data_to_model():
    query = """
    select team,opponent
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

    driver = establish_driver(local = True)

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
    print(len(players))
    print(len(opponent))
    print(len(teams))
    games = pd.DataFrame(data = {'player':players,'team':teams,'opponent':opponent})
    return print(games)



def recent_player_data(games):

    players = games['player']
    teams = games['teams'].unique()
    opponents = games['opponent'].unique()

    player_query = f"""
    WITH RankedGames AS (
        SELECT game_date,matchup,
            ROW_NUMBER() OVER (PARTITION BY player ORDER BY game_date DESC) AS game_rank
        FROM `capstone_data.NBA_Cleaned`
        WHERE player IN ({','.join([f'"{player}"' for player in players])})
    )
    SELECT *
    FROM RankedGames
    WHERE game_rank <= 3
    ORDER BY player, game_date DESC;
    """
    opponent_query = f"""
    WITH RankedGames AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY player ORDER BY game_date DESC) AS game_rank
        FROM `capstone_data.Cleaned_team_ratings`
        WHERE `match up` IN ({','.join([f'"{opponent}"' for opponent in opponents])})
    )
    SELECT defrtg as opponent_drtg,`oreb%` as opponent_off_reb, `dreb%` as opponent_def_reb
    FROM RankedGames
    WHERE game_rank <= 3
    ORDER BY opponents, game_date DESC;
    """

    team_query = f"""
    WITH RankedGames AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY player ORDER BY game_date DESC) AS game_rank
        FROM `capstone_data.Cleaned_team_ratings`
        WHERE team IN ({','.join([f'"{team}"' for team in teams])})
    )
    SELECT *
    FROM RankedGames
    WHERE game_rank <= 3
    ORDER BY opponents, game_date DESC;
    """

    player_data = pd.DataFrame(pandas_gbq.read_gbq(query,project_id='miscellaneous-projects-444203'))
    opponent = pd.DataFrame(pandas_gbq.read_gbq(query,project_id='miscellaneous-projects-444203'))

#     for player in players:
#         print(player_data[player_data['player'] == player])


data = gather_data_to_model()

players = scrape_roster(data)

