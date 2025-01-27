#!/home/aportra99/venv/bin/activate
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
import regex as re
from datetime import datetime as date

def convert_minutes_to_decimal(min_played):
    min,sec = map(int,min_played.split(':'))

    return round(min + (sec/60),2)

def clean_current_player_data(data,credentials):

        name = '^([A-Z][a-z]*[a-zA-Z]*(?:-[A-Z][a-z]+)?(?:\s[A-Z][a-z]+(?:-[A-Z][a-z]+)*)?(?:\s(?:Jr\.|Sr\.|III|IV))?)'

        data['PLAYER'] = data['PLAYER'].apply(lambda x: re.search(name,x).group(0) if re.search(name,x) else None)
        data['game_date'] = data['game_date'].apply(lambda x: x.date())
        data['MIN'] = data['MIN'].apply(convert_minutes_to_decimal)

        features_for_rolling = [feature for feature in data.columns[1:20]]
        
        for feature in features_for_rolling:
            data[f'rolling_avg_{feature}'] = data.groupby(['PLAYER'])[f'{feature}'].rolling(window = 2).mean().reset_index(0,drop = True,)
        
        data.dropna(inplace = True, ignore_index = True)

        print(data)

        
        pandas_gbq.to_gbq(data,destination_table = f'capstone_data.NBA_Season_2024-2025_uncleaned_cleaned',project_id='miscellaneous-projects-444203',if_exists= 'append',credentials=credentials)





def clean_past_player_data():

    tables = ['NBA_Season_2021-2022_uncleaned']
            #   ,'NBA_Season_2022-2023_uncleaned','NBA_Season_2023-2024_uncleaned','NBA_Season_2024-2025_uncleaned']

    for table in tables:
        query = f"""select a.*
                    from `capstone_data.{table}` a
                    inner join (
                    select distinct PLAYER,game_id
                    from `capstone_data.{table}`) b
                    on a.PLAYER = b.PLAYER and a.game_id = b.game_id
                    order by game_date asc
                    limit 100"""
        data = pd.DataFrame(pandas_gbq.read_gbq(query, project_id = 'miscellaneous-projects-444203'))

        data.dropna(inplace = True, ignore_index = True)
        

        name = '^([A-Z][a-z]*[a-zA-Z]*(?:-[A-Z][a-z]+)?(?:\s[A-Z][a-z]+(?:-[A-Z][a-z]+)*)?(?:\s(?:Jr\.|Sr\.|III|IV))?)'

        data['PLAYER'] = data['PLAYER'].apply(lambda x: re.search(name,x).group(0) if re.search(name,x) else None)
        data['game_date'] = data['game_date'].apply(lambda x: x.date())
        data['MIN'] = data['MIN'].apply(convert_minutes_to_decimal)

        features_for_rolling = [feature for feature in data.columns[1:20]]
        
        for feature in features_for_rolling:
            data[f'rolling_avg_{feature}'] = data.groupby(['PLAYER'])[f'{feature}'].rolling(window = 2).mean().reset_index(0,drop = True,)
        
        data.dropna(inplace=True, ignore_index=True)

        for column in data.columns:
             data.rename(columns = {column:column.lower()},inplace= True)
        
        print(data)

        # pandas_gbq.to_gbq(data,destination_table = f'capstone_data.{table.rstrip('uncleaned')}cleaned',project_id='miscellaneous-projects-444203',if_exists='replace')

# clean_past_player_data()


# clean_team_data