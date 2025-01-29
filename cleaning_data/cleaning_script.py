#!/home/aportra99/venv/bin/activate
from google.cloud import bigquery
from datetime import datetime as date

import regex as re
import pandas as pd
import pandas_gbq

def convert_minutes_to_decimal(min_played):
    min,sec = map(int,min_played.split(':'))

    return round(min + (sec/60),2)

def clean_current_player_data(data,credentials):
        
        data.dropna(inplace = True, ignore_index = True)
        name = '^([A-Z][a-z]*[a-zA-Z]*(?:-[A-Z][a-z]+)?(?:\s[A-Z][a-z]+(?:-[A-Z][a-z]+)*)?(?:\s(?:Jr\.|Sr\.|III|IV))?)'

        for column in data.columns:
            data.rename(columns = {column:column.lower()},inplace= True)

        data['player'] = data['player'].apply(lambda x: re.search(name,x).group(0) if re.search(name,x) else None)
        data['min'] = data['min'].apply(convert_minutes_to_decimal)
        


        players = data['player'].unique()


        query = f"""
        select *
        from `capstone_data.NBA_Season_2024-2025_cleaned`
        where player in ({','.join([f"'{player}'" for player in players])})
        limit {3 * len(players)}
        """
        print('pulling past data')
        all_player_data = pd.DataFrame(pandas_gbq.read_gbq(query,project_id='miscellaneous-projects-444203'))

        all_player_data = pd.concat([all_player_data,data], ignore_index = True)

        features_for_rolling = [feature for feature in data.columns[1:20]] 

        player_dfs = []

        for player in players:
            player_data = all_player_data[all_player_data['player'] == f'{player}'].sort_values(by='game_date',ascending = True)

            for feature in features_for_rolling:
                player_data[f'rolling_avg_{feature}'] = player_data[player_data['player'] == f'{player}'][f'{feature}'].rolling(window = 3).mean().reset_index(0,drop = True)
                
            new_player_data = player_data[player_data['game_date'].isin(data['game_date'])]
            print(len(new_player_data))
            player_dfs.append(new_player_data)
            
        print('rolling features calculated')
        rolling_data = pd.concat(player_dfs,ignore_index = True)

        print(rolling_data)


        rolling_data.dropna(inplace = True, ignore_index = True)
    
        pandas_gbq.to_gbq(rolling_data,destination_table = f'capstone_data.test',project_id='miscellaneous-projects-444203',if_exists= 'replace',credentials=credentials)





def clean_past_player_data():

    tables = ['NBA_Season_2021-2022_uncleaned','NBA_Season_2022-2023_uncleaned','NBA_Season_2023-2024_uncleaned','NBA_Season_2024-2025_uncleaned']

    for table in tables:
        query = f"""
            select * 
            from `capstone_data.{table}`
            order by game_date asc
        """
        data = pd.DataFrame(pandas_gbq.read_gbq(query, project_id = 'miscellaneous-projects-444203'))

        data.dropna(inplace = True, ignore_index = True)
        

        name = '^([A-Z][a-z]*[a-zA-Z]*(?:-[A-Z][a-z]+)?(?:\s[A-Z][a-z]+(?:-[A-Z][a-z]+)*)?(?:\s(?:Jr\.|Sr\.|III|IV))?)'

        data['player'] = data['player'].apply(lambda x: re.search(name,x).group(0) if re.search(name,x) else None)
        data['min'] = data['min'].apply(convert_minutes_to_decimal)

        features_for_rolling = [feature for feature in data.columns[1:20]]
        
        for feature in features_for_rolling:
            data[f'rolling_avg_{feature}'] = data.groupby(['player'])[f'{feature}'].rolling(window = 3).mean().reset_index(0,drop = True,)
            
        data.dropna(inplace=True, ignore_index=True)


        
        print(data)

        pandas_gbq.to_gbq(data,destination_table = f'capstone_data.{table.rstrip("uncleaned")}cleaned',project_id='miscellaneous-projects-444203',if_exists='replace')

# clean_past_player_data()


# clean_team_data