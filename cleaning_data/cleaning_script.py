#!/home/aportra99/venv/bin/activate
from google.cloud import bigquery
from datetime import datetime as date
from google.oauth2 import service_account
from scraping_data.utils import send_email

import regex as re
import pandas as pd
import pandas_gbq


def convert_minutes_to_decimal(min_played):
    min,sec = map(int,min_played.split(':'))

    return round(min + (sec/60),2)


def clean_current_player_data(modeling_data):
    try:
        credentials = service_account.Credentials.from_service_account_file('/home/aportra99/scraping_key.json')
        local = False
        print("Credentials file loaded.")
    except:
        local = True
        print("Running with default credentials")

    modeling_data.dropna(inplace = True, ignore_index = True)
    
    modeling_data['player'] = modeling_data['player'].str.replace('.', '', regex=False) 

    for column in modeling_data.columns:
        modeling_data.rename(columns = {column:column.lower()},inplace= True)

    modeling_data['min'] = modeling_data['min'].apply(convert_minutes_to_decimal)
    
    features_for_rolling = [feature for feature in modeling_data.columns[1:21]] 

    players = modeling_data['player'].unique()
    print(players)

    modeling_query = f"""
    WITH RankedGames AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY player ORDER BY game_date DESC) AS game_rank
        FROM `capstone_data.player_modeling_data`
        WHERE player IN ({','.join([f'"{player}"' for player in players])})
    )
    SELECT *
    FROM RankedGames
    WHERE game_rank <= 3
    ORDER BY player, game_date DESC;
    """
    
    prediction_query = f"""
    WITH RankedGames AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY player ORDER BY game_date DESC) AS game_rank
        FROM `capstone_data.player_prediction_data`
        WHERE player IN ({','.join([f'"{player}"' for player in players])})
    )
    SELECT *
    FROM RankedGames
    WHERE game_rank <= 3
    ORDER BY player, game_date DESC;
    """
    print('pulling past modeling_data')
    if local:
        modeling_data = pd.DataFrame(pandas_gbq.read_gbq(modeling_query,project_id='miscellaneous-projects-444203'))
        predict_data = pd.DataFrame(pandas_gbq.read_gbq(prediction_query,project_id='miscellaneous-projects-444203'))
    else:
        modeling_data = pd.DataFrame(pandas_gbq.read_gbq(modeling_query,project_id='miscellaneous-projects-444203',credentials=credentials))
        predict_data = pd.DataFrame(pandas_gbq.read_gbq(prediction_query,project_id='miscellaneous-projects-444203',credentials=credentials))


    player_dfs = []
    prediction_dfs = []

    for player in players:
        player_data = modeling_data[modeling_data['player'] == f'{player}'].copy()
        prediction_data = predict_data[predict_data['player'] == f'{player}'].copy()

        data_for_rolling = modeling_data[modeling_data['player'] == player].sort_values(by='game_date')
        prediction_data_rolling = predict_data[predict_data['player'] == player].sort_values(by='game_date')
        for feature in features_for_rolling:
            
            rolling_avg = data_for_rolling[data_for_rolling['player'] == player][f'{feature}'].rolling(window = 3).mean().shift(1).reset_index(level = 0,drop=True)
            prediction_rolling_avg = prediction_data_rolling[prediction_data_rolling['player'] == player][f'{feature}'].rolling(window = 3).mean().reset_index(level = 0,drop = True)

            player_data[f'{feature}_3gm_avg']  = round(rolling_avg.iloc[-1],2)
            prediction_data[f'{feature}_3gm_avg']  = round(rolling_avg.iloc[-1],2)

        player_data.dropna(inplace = True, ignore_index = True)
        prediction_data.dropna(inplace = True, ignore_index = True)
        
        prediction_dfs.append(prediction_data)
        player_dfs.append(player_data)
        
    print('rolling features calculated')
    model_data = pd.concat(player_dfs,ignore_index = True)
    predict_data = pd.concat(prediction_dfs,ignore_index = True)

    if local:
        pandas_gbq.to_gbq(model_data,destination_table = f'capstone_data.player_modeling_data',project_id='miscellaneous-projects-444203',if_exists= 'append',credentials=credentials,table_schema=[{'name':'game_date','type':'DATE'},])
        pandas_gbq.to_gbq(predict_data,destination_table = f'capstone_data.player_prediction_data',project_id='miscellaneous-projects-444203',if_exists= 'append',credentials=credentials,table_schema=[{'name':'game_date','type':'DATE'},])
    else:
        pandas_gbq.to_gbq(model_data,destination_table = f'capstone_data.player_modeling_data',project_id='miscellaneous-projects-444203',if_exists= 'append',table_schema=[{'name':'game_date','type':'DATE'},])
        pandas_gbq.to_gbq(predict_data,destination_table = f'capstone_data.player_prediction_data',project_id='miscellaneous-projects-444203',if_exists= 'append',table_schema=[{'name':'game_date','type':'DATE'},])
send_email(
    subject="NBA PLAYER DATA CLEANED",
    body="Data uploaded to NBA_Cleaned"
    )





def clean_past_player_data():
    try:
        credentials = service_account.Credentials.from_service_account_file('/home/aportra99/scraping_key.json')
        local = False
        print("Credentials file loaded.")
    except:
        local = True
        print("Running with default credentials")

    tables = ['NBA_Season_2021-2022_uncleaned','NBA_Season_2022-2023_uncleaned','NBA_Season_2023-2024_uncleaned','NBA_Season_2024-2025_uncleaned']

    model_data = []
    predict_data = []

    for table in tables:
        modeling_query = f"""
        SELECT *
        FROM `capstone_data.{table}`
        ORDER BY game_date ASC
        """
        if local:
            modeling_data = pd.DataFrame(pandas_gbq.read_gbq(modeling_query, project_id = 'miscellaneous-projects-444203'))
        if not local:
            modeling_data = pd.DataFrame(pandas_gbq.read_gbq(modeling_query, project_id = 'miscellaneous-projects-444203',credentials=credentials))

        modeling_data.dropna(inplace = True, ignore_index = True)
        
        modeling_data['player'] = modeling_data['player'].str.replace('.', '', regex=False) 

        modeling_data['min'] = modeling_data['min'].apply(convert_minutes_to_decimal)

        features_for_rolling = [feature for feature in modeling_data.columns[1:21]]
        
        modeling_data = modeling_data.sort_values(by = ['player', 'game_date'])

        prediction_data = modeling_data.copy()

        #using shifted windows for rolling data to prevent data leakage
        for feature in features_for_rolling:
            modeling_data[f'{feature}_3gm_avg'] = modeling_data.groupby(by = 'player')[f'{feature}'].rolling(window = 3).mean().shift(1).reset_index(level = 0,drop=True).round(2)
            prediction_data[f'{feature}_3gm_avg'] = prediction_data.groupby(by = 'player')[f'{feature}'].rolling(window = 3).mean().reset_index(level = 0,drop=True).round(2)
        

        modeling_data.dropna(inplace=True, ignore_index=True)
        prediction_data.dropna(inplace=True, ignore_index=True)

        model_data.append(modeling_data)
        predict_data.append(prediction_data)

    model_data = pd.concat(model_data,ignore_index = True)
    predict_data = pd.concat(predict_data,ignore_index = True)

    model_data.dropna(inplace= True, ignore_index= True)
    predict_data.dropna(inplace= True, ignore_index= True)

    if local:
        pandas_gbq.to_gbq(model_data,destination_table = f'capstone_data.player_modeling_data',project_id='miscellaneous-projects-444203',if_exists='replace')
        pandas_gbq.to_gbq(predict_data,destination_table = f'capstone_data.player_prediction_data',project_id='miscellaneous-projects-444203',if_exists='replace')
    else:
        pandas_gbq.to_gbq(model_data,destination_table = f'capstone_data.player_modeling_data',project_id='miscellaneous-projects-444203',if_exists='replace',credentials=credentials)
        pandas_gbq.to_gbq(predict_data,destination_table = f'capstone_data.player_prediction_data',project_id='miscellaneous-projects-444203',if_exists='replace',credentials=credentials)


def clean_past_team_ratings():
    model_data = []
    predict_data = []
    try:
        credentials = service_account.Credentials.from_service_account_file('/home/aportra99/scraping_key.json')
        local = False
        print("Credentials file loaded.")
    except:
        local = True
        print("Running with default credentials")
    tables = ['2021-2022_team_ratings',
              '2022-2023_team_ratings',
              '2023-2024_team_ratings',
              '2024-2025_team_ratings']


    for season in tables:
        modeling_query = f"""
        select *
        from `capstone_data.{season}`
        order by `game date` asc
        """

        if local:
            modeling_data = pd.DataFrame(pandas_gbq.read_gbq(modeling_query,project_id='miscellaneous-projects-444203'))
        else:
            modeling_data = pd.DataFrame(pandas_gbq.read_gbq(modeling_query,project_id='miscellaneous-projects-444203',credentials=credentials))
        modeling_data.rename(columns = {'game date':'game_date'},inplace = True)
        
        modeling_data = modeling_data.sort_values(by = ['team','game_date'])

        prediction_data = modeling_data.copy()

        num_columns = modeling_data.columns[5:19]


        #using shifted windows for rolling data to prevent data leakage
        for column in num_columns:
            modeling_data[f'{column}_3gm_avg'] = modeling_data.groupby(by = 'team')[column].rolling(window = 3).mean().shift(1).reset_index(level = 0,drop = True).round(2)
            prediction_data[f'{column}_3gm_avg'] = prediction_data.groupby(by = 'team')[column].rolling(window = 3).mean().reset_index(level = 0,drop = True).round(2)


        modeling_data.dropna(inplace = True, ignore_index= True)
        prediction_data.dropna(inplace = True, ignore_index= True)

        model_data.append(modeling_data)
        predict_data.append(prediction_data)

    model_data = pd.concat(model_data,ignore_index = True)
    predict_data = pd.concat(predict_data,ignore_index = True)
    if local:
        pandas_gbq.to_gbq(model_data,destination_table='capstone_data.team_modeling_data',project_id='miscellaneous-projects-444203',table_schema=[{'name':'game date','type':'DATE'}],if_exists='replace')
        pandas_gbq.to_gbq(predict_data,destination_table='capstone_data.team_prediction_data',project_id='miscellaneous-projects-444203',table_schema=[{'name':'game date','type':'DATE'}],if_exists='replace')
    else:
        pandas_gbq.to_gbq(model_data,destination_table='capstone_data.team_modeling_data',project_id='miscellaneous-projects-444203',table_schema=[{'name':'game date','type':'DATE'}],credentials=credentials,if_exists='replace')
        pandas_gbq.to_gbq(predict_data,destination_table='capstone_data.team_prediction_data',project_id='miscellaneous-projects-444203',table_schema=[{'name':'game date','type':'DATE'}],if_exists='replace')




def clean_current_team_ratings(game_data):
    try:
        credentials = service_account.Credentials.from_service_account_file('/home/aportra99/scraping_key.json')
        local = False
        print("Credentials file loaded.")
    except:
        local = True
        print("Running with default credentials")

    teams = game_data['team'].unique()
    game_data.rename(columns = {'game date':'game_date'},inplace = True)
    modeling_query = f"""
    WITH RankedGames AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY team ORDER BY game_date DESC) AS game_rank
        FROM `capstone_data.team_modeling_data`
        WHERE team IN ({','.join([f'"{team}"' for team in teams])})
    )
    SELECT *
    FROM RankedGames
    WHERE game_rank <= 3
    ORDER BY team, game_date desc;
    """
    prediction_query = f"""
    WITH RankedGames AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY team ORDER BY game_date DESC) AS game_rank
        FROM `capstone_data.team_modeling_data`
        WHERE team IN ({','.join([f'"{team}"' for team in teams])})
    )
    SELECT *
    FROM RankedGames
    WHERE game_rank <= 3
    ORDER BY team, game_date desc;
    """

    team_dfs = []
    predict_dfs = []
    features_for_rolling = game_data.columns[5:19]
    if local:
        modeling_data = pd.DataFrame(pandas_gbq.read_gbq(modeling_query,project_id='miscellaneous-projects-444203'))
        prediction_data = pd.DataFrame(pandas_gbq.read_gbq(prediction_query,project_id='miscellaneous-projects-444203'))

    else:
        modeling_data = pd.DataFrame(pandas_gbq.read_gbq(modeling_query,project_id='miscellaneous-projects-444203',credentials=credentials))
        prediction_data = pd.DataFrame(pandas_gbq.read_gbq(prediction_query,project_id='miscellaneous-projects-444203',credentials=credentials))

    for team in teams:
        team_data = game_data[game_data['team'] == f'{team}'].copy()
        predict_data = game_data[game_data['team'] == f'{team}'].copy()

        data_for_rolling = modeling_data[modeling_data['team'] == team].sort_values(by='game_date')
        predict_data_for_rolling = prediction_data[prediction_data['team'] == team].sort_values(by='game_date')

        for feature in features_for_rolling:
            
            #using shifted windows for rolling data to prevent data leakage
            rolling_avg = data_for_rolling[data_for_rolling['team'] == team][f'{feature}'].rolling(window = 3).mean().shift(1).reset_index(level = 0,drop = True)
            predict_avg = predict_data_for_rolling[predict_data_for_rolling['team'] == team][f'{feature}'].rolling(window = 3).mean().reset_index(level = 0,drop = True)

            team_data[f'{feature}_3gm_avg'] = round(rolling_avg.iloc[-1], 2) if not rolling_avg.empty else 0
            predict_data[f'{feature}_3gm_avg'] = round(predict_avg.iloc[-1], 2) if not predict_avg.empty else 0

        team_data.dropna(inplace = True, ignore_index = True)
        predict_data.dropna(inplace = True, ignore_index = True)

        team_dfs.append(team_data)
        predict_dfs.append(predict_data)

    model_data = pd.concat(team_dfs,ignore_index=True)
    predict_data = pd.concat(predict_dfs,ignore_index= True)
    print('cleaning has been completed')

    if local:
        pandas_gbq.to_gbq(team_data,destination_table='capstone_data.team_modeling_data',project_id='miscellaneous-projects-444203',table_schema=[{'name':'game date','type':'DATE'}],if_exists='replace')
        pandas_gbq.to_gbq(predict_data,destination_table='capstone_data.team_prediction_data',project_id='miscellaneous-projects-444203',table_schema=[{'name':'game date','type':'DATE'}],if_exists='replace')
    else:
        pandas_gbq.to_gbq(modeling_data,destination_table='capstone_data.team_modeling_data',project_id='miscellaneous-projects-444203',table_schema=[{'name':'game date','type':'DATE'}],credentials=credentials,if_exists='replace')
        pandas_gbq.to_gbq(predict_data,destination_table='capstone_data.team_prediction_data',project_id='miscellaneous-projects-444203',table_schema=[{'name':'game date','type':'DATE'}],credentials=credentials,if_exists='replace')
    send_email(
    subject="NBA TEAM DATA CLEANED",
    body="Data uploaded to Cleaned_team_ratings"
    )

