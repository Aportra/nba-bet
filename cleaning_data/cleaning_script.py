#!/home/aportra99/venv/bin/activate
from google.cloud import bigquery
from datetime import datetime as date
from google.oauth2 import service_account

import regex as re
import pandas as pd
import pandas_gbq

def convert_minutes_to_decimal(min_played):
    min,sec = map(int,min_played.split(':'))

    return round(min + (sec/60),2)

def clean_current_player_data(data):
    try:
        credentials = service_account.Credentials.from_service_account_file('/home/aportra99/scraping_key.json')
        local = False
        print("Credentials file loaded.")
    except:
        local = True
        print("Running with default credentials")

        data.dropna(inplace = True, ignore_index = True)
        
        data['player'] = data['player'].str.replace('.', '', regex=False) 
        name = "^(?:(?:Fred VanFleet)|(?:DeMar DeRozan)|(TJ McConnell)|(?:[A-Z][a-zA-Z']*(?:-[A-Z][a-z]+)?(?:\s[A-Z][a-z]+(?:-[A-Z][a-z]+)*)?(?:-[A-Z][a-z]+)*))(?:\s(?:Jr\.|Sr\.|III|IV))?"

        for column in data.columns:
            data.rename(columns = {column:column.lower()},inplace= True)

        data['player'] = data['player'].apply(lambda x: re.search(name,x).group(0) if re.search(name,x) else None)
        data['min'] = data['min'].apply(convert_minutes_to_decimal)
        
        features_for_rolling = [feature for feature in data.columns[1:21]] 

        players = data['player'].unique()
        print(players)
        query = f"""
        WITH RankedGames AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY player ORDER BY game_date DESC) AS game_rank
            FROM `capstone_data.NBA_Cleaned`
            WHERE player IN ({','.join([f'"{player}"' for player in players])})
        )
        SELECT *
        FROM RankedGames
        WHERE game_rank <= 3
        ORDER BY player, game_date DESC;
        """

        print('pulling past data')
        if local:
            all_player_data = pd.DataFrame(pandas_gbq.read_gbq(query,project_id='miscellaneous-projects-444203'))
        else:
            all_player_data = pd.DataFrame(pandas_gbq.read_gbq(query,project_id='miscellaneous-projects-444203',credentials=credentials))

        print(all_player_data)
        data['game_date'] = pd.to_datetime(data['game_date']).dt.date
        all_player_data['game_date'] = pd.to_datetime(all_player_data['game_date']).dt.date

        player_dfs = []

        for player in players:
            player_data = data[data['player'] == f'{player}'].copy()
            data_for_rolling = all_player_data[all_player_data['player'] == player].sort_values(by='game_date')

            for feature in features_for_rolling:
                
                rolling_avg = data_for_rolling[data_for_rolling['player'] == player][f'{feature}'].rolling(window = 3).mean().reset_index(0,drop = True)
                player_data[f'{feature}_3gm_avg']  = round(rolling_avg.iloc[-1],2)
    
            
            
            player_dfs.append(player_data)
            
        print('rolling features calculated')
        all_data = pd.concat(player_dfs,ignore_index = True)

        if local:
            pandas_gbq.to_gbq(all_data,destination_table = f'capstone_data.NBA_Cleaned',project_id='miscellaneous-projects-444203',if_exists= 'append',credentials=credentials,table_schema=[{'name':'game_date','type':'DATE'},])
        else:
             pandas_gbq.to_gbq(all_data,destination_table = f'capstone_data.NBA_Cleaned',project_id='miscellaneous-projects-444203',if_exists= 'append',table_schema=[{'name':'game_date','type':'DATE'},])





def clean_past_player_data():

    tables = ['NBA_Season_2021-2022_uncleaned','NBA_Season_2022-2023_uncleaned','NBA_Season_2023-2024_uncleaned','NBA_Season_2024-2025_uncleaned']

    all_data = []

    for table in tables:
        query = f"""
        SELECT *
        FROM `capstone_data.{table}`
        ORDER BY game_date ASC
        """

        data = pd.DataFrame(pandas_gbq.read_gbq(query, project_id = 'miscellaneous-projects-444203',credentials=credentials))

        data.dropna(inplace = True, ignore_index = True)
        
        data['player'] = data['player'].replace('.','')
        name = "^(?:(?:DeMar DeRozan)|(?:[A-Z][a-zA-Z']*(?:-[A-Z][a-z]+)?(?:\s[A-Z][a-z]+(?:-[A-Z][a-z]+)*)?(?:-[A-Z][a-z]+)*))(?:\s(?:Jr\.|Sr\.|III|IV))?"

        data['player'] = data['player'].apply(lambda x: re.search(name,x).group(0) if re.search(name,x) else None)
        data['min'] = data['min'].apply(convert_minutes_to_decimal)

        features_for_rolling = [feature for feature in data.columns[1:21]]
        
        for feature in features_for_rolling:
            data[f'{feature}_3gm_avg'] = data.groupby(['player'])[f'{feature}'].rolling(window = 3).mean().shift(1).reset_index(0,drop = True,)
            data[f'{feature}_3gm_avg'] = round(data[f'{feature}_3gm_avg'],2)
        data.dropna(inplace=True, ignore_index=True)

        all_data.append(data)

    nba_data_cleaned = pd.concat(all_data,ignore_index = True)
    
    pandas_gbq.to_gbq(nba_data_cleaned,destination_table = f'capstone_data.NBA_Cleaned',project_id='miscellaneous-projects-444203',if_exists='replace')



# clean_team_data