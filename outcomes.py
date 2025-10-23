import pandas_gbq
import pandas as pd
from datetime import datetime as dt
from scraping_data import utils


from google.oauth2 import service_account
def clean_player_name(name):
    """Standardizes player names by removing special characters and handling known name variations."""
    name = name.lower().strip()  # Convert to lowercase & remove extra spaces
    name = name.replace(".", "")  # Remove periods

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

def classify_result(row,table,cat):
    return "Under" if row[f'{table}'] > row[f'{cat}'] else "Over"

def past_outcomes():
    tables = ["points"]
    categories = ['pts']
    try:
        credentials = service_account.Credentials.from_service_account_file(
        "/home/aportra99/scraping_key.json"
    )
        local = False
    except FileNotFoundError:
        local = True
    today = dt.today().date()
    season = today.year if today.month >= 10 else today.year - 1
    game_query =f"""
        select  *
        from `capstone_data.player_prediction_data_partitioned`
        where season_start_year = {season}
        """
    game_data =  pandas_gbq.read_gbq(game_query, project_id='miscellaneous-projects-444203',credentials=credentials if not local else None)

    for table,cat in zip(tables,categories):
        
        predict_query = f"""
            WITH ranked_predictions AS (
                SELECT *, 
                    ROW_NUMBER() OVER (PARTITION BY Player, date(Date_Updated) ORDER BY Date_Updated DESC) AS rn
                FROM `capstone_data.{table}_classifications`
                where recommendation != 'No Bet Recommendation'
            )
            SELECT * 
            FROM ranked_predictions
            WHERE rn = 1"""


        try:
            credentials = service_account.Credentials.from_service_account_file(
            "/home/aportra99/scraping_key.json"
        )
            local = False
        except FileNotFoundError:
            credentials = False
            local = True

        predict_data = pandas_gbq.read_gbq(predict_query, project_id='miscellaneous-projects-444203',credentials=credentials if not local else None)
        
        predict_data['player'] = predict_data['player'].apply(clean_player_name)
        predict_data['Date_Updated'] = pd.to_datetime(predict_data['Date_Updated']).dt.date
        game_data['game_date'] = pd.to_datetime(game_data['game_date']).dt.date
        predict_data.rename(columns={'Date_Updated':'game_date'},inplace=True)
        game_data['player'] = game_data['player'].apply(clean_player_name)

        game_data = game_data[game_data['player'].isin(predict_data['player'])]

        full_data = game_data.merge(predict_data,on=['player','game_date'])

        full_data[f'{table}'] = pd.to_numeric(full_data[f'{table}'])

        full_data['result'] = full_data.apply(lambda row:classify_result(row,table,cat), axis=1)
        
        full_data = full_data.drop_duplicates(subset=['player','game_date'])

        data_to_upload = full_data[['player',f'{table}',f'{cat}','game_date','result','recommendation','proba',]]
        table_schema = [{"name": "game_date", "type": "DATE"}]
        table_id = f"miscellaneous-projects-444203.capstone_data.{cat}_cl_outcome"
        pandas_gbq.to_gbq(
                data_to_upload,
                project_id="miscellaneous-projects-444203",
                destination_table=table_id,
                if_exists="replace",
                credentials=credentials if credentials else None,
                table_schema=table_schema,
            )



def current_outcome(data,date):
    try:
        game_data = data
        game_data['game_date'] = date
        game_data.rename(columns = {'player_name':'player','fg3m':'3pm'},inplace=True)
        tables = ["points"]
        categories = ['pts']
        try:
            credentials = service_account.Credentials.from_service_account_file(
            "/home/aportra99/scraping_key.json"
        )
            local = False
        except FileNotFoundError:
            credentials = False
            local = True

        for table,cat in zip(tables,categories):
            
            predict_query = f"""
            WITH ranked_predictions AS (
                    SELECT *, 
                        ROW_NUMBER() OVER (PARTITION BY Player, Date_Updated ORDER BY Date_Updated DESC) AS rn
                    FROM `capstone_data.{cat}_classifications`
                    where date(Date_Updated) = current_date('America/Los_Angeles') and recommendation != 'No Bet Recommendation'
                )
                SELECT * 
                FROM ranked_predictions
                WHERE rn = 1"""
            

            try:
                credentials = service_account.Credentials.from_service_account_file(
                "/home/aportra99/scraping_key.json"
            )
                local = False
            except FileNotFoundError:
                local = True
                
            predict_data = pandas_gbq.read_gbq(predict_query, project_id='miscellaneous-projects-444203',credentials=credentials if not local else None)
            print(predict_data)
            predict_data['player'] = predict_data['player'].apply(clean_player_name).copy()
            predict_data['Date_Updated'] = pd.to_datetime(predict_data['Date_Updated']).dt.date
            game_data['game_date'] = pd.to_datetime(game_data['game_date']).dt.date
            predict_data.rename(columns={'Date_Updated':'game_date'},inplace=True)
            game_data['player'] = game_data['player'].apply(clean_player_name)

            game_data = game_data[game_data['player'].isin(predict_data['player'])]

            full_data = game_data.merge(predict_data,on=['player','game_date'])

            full_data[f'{table}'] = pd.to_numeric(full_data[f'{table}'])

            full_data.loc[:,'result'] = full_data.apply(lambda row:classify_result(row,table,cat), axis=1)
            
            full_data['outcome'] = (full_data['result']==full_data[f'recommendation'])

            full_data = full_data.drop_duplicates(subset=['player','game_date'])

            data_to_upload = full_data[['player',f'{table}',f'{cat}','game_date','result','recommendation','proba']]
            table_schema = [{"name": "game_date", "type": "DATE"}]
            table_id = f"miscellaneous-projects-444203.capstone_data.{cat}_cl_outcome"
            print(data_to_upload)
            print("Number of rows:", len(data_to_upload)) 
            pandas_gbq.to_gbq(
                    data_to_upload,
                    project_id="miscellaneous-projects-444203",
                    destination_table=table_id,
                    if_exists="append",
                    credentials=credentials if credentials else None,
                    table_schema=table_schema,
                )
            project_id = 'miscellaneous-projects-444203'
            categories = ['pts']
            results = {}
            for cat in categories:
                query = f"""
                SELECT 
                    SUM(CASE WHEN result = recommendation THEN 1 ELSE 0 END) / COUNT(Player) AS accuracy
                FROM `capstone_data.{cat}_cl_outcome`
                where game_date = CURRENT_DATE('America/Los_Angeles')
                """
                df = pandas_gbq.read_gbq(query, project_id=project_id, dialect='standard')
                results[f"{cat}_accuracy"] = df['accuracy'].iloc[0]

            for result in results:
                print(results[result])
                if results[result] < .524:
                    print(result)
                    utils.send_email(
                            subject=f"Warning This Model {result} Underperfoming",
                            body=f"{result}: {results[result]}"
                    )            


            utils.send_email(
            subject="Outcome Posted to GBQ",
            body=str([f"{key}: {results[key]}" for key in results])
                )
    except Exception as e:
        print(e)
        utils.send_email(
        subject="Outcomes Error",
        body=f"Error {e}")

