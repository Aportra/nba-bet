"""Module for cleaning and processing current NBA player data before uploading to BigQuery."""

from google.cloud import bigquery
from datetime import datetime as dt
from google.oauth2 import service_account
from scraping_data.utils import send_email

import pandas as pd
import pandas_gbq
import unicodedata


def remove_accents(input_str):
    """Removes accents from a given string.

    Args:
        input_str (str): Input string with potential accents.

    Returns:
        str: String without accents.
    """
    return ''.join(
        c for c in unicodedata.normalize('NFKD', input_str) if not unicodedata.combining(c)
    )


def convert_minutes_to_decimal(min_played):
    """Converts minutes played from MM:SS format to a decimal representation.

    Args:
        min_played (str): Time in MM:SS format.

    Returns:
        float: Decimal representation of minutes.
    """
    minutes, seconds = map(int, min_played.split(':'))
    return round(minutes + (seconds / 60), 2)


def clean_current_player_data(data):
    """Cleans and processes NBA player data for modeling and prediction.

    Args:
        data (pd.DataFrame): Raw player game data.

    Raises:
        Exception: If data cleaning or processing fails.
    """
    try:
        # Load credentials for Google BigQuery
        try:
            credentials = service_account.Credentials.from_service_account_file('/home/aportra99/scraping_key.json')
            local = False
            print("Credentials file loaded.")
        except FileNotFoundError:
            local = True
            credentials = None
            print("Running with default credentials.")

        # Drop missing values and reset index
        data.dropna(inplace=True, ignore_index=True)

        # Normalize player names (remove dots and accents)
        data['player'] = data['player'].str.replace('.', '', regex=False)
        data['player'] = data['player'].apply(remove_accents)

        # Convert time played to decimal format
        data['min'] = data['min'].apply(convert_minutes_to_decimal)

        # Standardize column names to lowercase
        data.rename(columns=str.lower, inplace=True)

        # Identify current season
        today = dt.today().date()
        season = today.year if today.month >= 10 else today.year - 1

        # Generate season format for each row
        data['season'] = data['game_date'].apply(
            lambda x: f"{x.year}-{x.year + 1}" if x.month >= 10 else f"{x.year - 1}-{x.year}"
        )

        # Extract unique player names
        players = data['player'].unique()
        print(f"Processing {len(players)} players.")

        # Define SQL queries for fetching past data
        modeling_query = f"""
        WITH RankedGames AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY player ORDER BY game_date DESC) AS game_rank
            FROM `capstone_data.player_modeling_data_partitioned`
            WHERE player IN ({','.join([f'"{player}"' for player in players])}) 
            AND season_start_year = {season}
        )
        SELECT * FROM RankedGames
        ORDER BY player, game_date DESC;
        """

        prediction_query = f"""
        WITH RankedGames AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY player ORDER BY game_date DESC) AS game_rank
            FROM `capstone_data.player_prediction_data_partitioned`
            WHERE player IN ({','.join([f'"{player}"' for player in players])}) 
            AND season_start_year = {season}
        )
        SELECT * FROM RankedGames
        ORDER BY player, game_date DESC;
        """

        print("Fetching past modeling and prediction data...")

        # Fetch past modeling and prediction data from BigQuery
        modeling_data = pandas_gbq.read_gbq(
            modeling_query, project_id='miscellaneous-projects-444203', credentials=credentials
        ) 

        predict_data = pandas_gbq.read_gbq(
            prediction_query, project_id='miscellaneous-projects-444203', credentials=credentials
        )

        # Define features for rolling averages
        features_for_rolling = list(data.columns[1:21])

        # Prepare data for modeling and prediction
        model_dfs = []
        prediction_dfs = []

        for player in players:
            model_df = data[data['player'] == player].copy()
            prediction_data = data[data['player'] == player].copy()

            past_model_data = modeling_data[modeling_data['player'] == player].sort_values(by='game_date')
            past_predict_data = predict_data[predict_data['player'] == player].sort_values(by='game_date')

            for feature in features_for_rolling:
                # Compute 3-game rolling averages, preventing data leakage
                rolling_avg = past_model_data.groupby('player')[feature].shift(1).rolling(3, min_periods=3).mean()
                predict_avg = past_predict_data.groupby('player')[feature].rolling(3, min_periods=3).mean()

                model_df[f'{feature}_3gm_avg'] = rolling_avg.iloc[-1] if not rolling_avg.empty else 0
                prediction_data[f'{feature}_3gm_avg'] = predict_avg.iloc[-1] if not predict_avg.empty else 0

                # Compute season averages and momentum
                season_avg = past_model_data.groupby(['player', 'season'])[feature].expanding().mean().shift(1)
                predict_season_avg = past_predict_data.groupby(['player', 'season'])[feature].expanding().mean()

                model_df[f'{feature}_season'] = season_avg.iloc[-1] if not season_avg.empty else 0
                prediction_data[f'{feature}_season'] = predict_season_avg.iloc[-1] if not predict_season_avg.empty else 0

                model_df[f'{feature}_momentum'] = model_df[f'{feature}_season'] - model_df[f'{feature}_3gm_avg']
                prediction_data[f'{feature}_momentum'] = prediction_data[f'{feature}_season'] - prediction_data[f'{feature}_3gm_avg']

            model_df.dropna(inplace=True, ignore_index=True)
            prediction_data.dropna(inplace=True, ignore_index=True)

            model_dfs.append(model_df)
            prediction_dfs.append(prediction_data)

        print("Rolling features calculated.")

        # Combine and format final datasets
        model_data = pd.concat(model_dfs, ignore_index=True)
        predict_data = pd.concat(prediction_dfs, ignore_index=True)

        # Fill NaNs with 0 for modeling
        model_data.fillna(0, inplace=True)
        predict_data.fillna(0, inplace=True)

        # Assign season start year
        model_data['season_start_year'] = season
        predict_data['season_start_year'] = season

        # Upload to BigQuery
        if not local:
            for dataset, table in [(model_data, "player_modeling_data_partitioned"), 
                                   (predict_data, "player_prediction_data_partitioned")]:
                pandas_gbq.to_gbq(dataset, destination_table=f'capstone_data.{table}',
                                  project_id='miscellaneous-projects-444203', if_exists='append',
                                  credentials=credentials, table_schema=[{'name': 'game_date', 'type': 'DATE'}])
        

        else:
            for dataset, table in [(model_data, "player_modeling_data_partitioned"), 
                                   (predict_data, "player_prediction_data_partitioned")]:
                pandas_gbq.to_gbq(dataset, destination_table=f'capstone_data.{table}',
                                  project_id='miscellaneous-projects-444203', if_exists='append',
                                table_schema=[{'name': 'game_date', 'type': 'DATE'}])
        send_email(subject="NBA PLAYER DATA CLEANED", body="Data successfully uploaded to NBA_Cleaned.")

    except Exception as e:
        send_email(subject="NBA PLAYER Cleaning Failed", body=f"Error: {e}")


def clean_past_player_data():
    """Cleans and processes past NBA player data for modeling and prediction.

    This function retrieves historical player data from BigQuery, performs cleaning, 
    computes rolling averages and momentum, and then uploads the processed data back 
    to BigQuery.

    Raises:
        Exception: If data cleaning or processing fails.
    """
    try:
        # Load credentials for Google BigQuery
        try:
            credentials = service_account.Credentials.from_service_account_file(
                "/home/aportra99/scraping_key.json"
            )
            local = False
            print("Credentials file loaded.")
        except FileNotFoundError:
            local = True
            credentials = None
            print("Running with default credentials.")

        # Generate table names for each season
        tables = [f"{i}-{i+1}_uncleaned" for i in range(2015, 2025)]

        model_data = []
        predict_data = []

        for table in tables:
            modeling_query = f"""
            SELECT *
            FROM `capstone_data.{table}`
            ORDER BY game_date ASC
            """

            print(f"Fetching data from table: {table}")

            modeling_data = pandas_gbq.read_gbq(
                modeling_query,
                project_id="miscellaneous-projects-444203",
                credentials=credentials if not local else None
            )

            # Drop missing values and reset index
            modeling_data.dropna(inplace=True, ignore_index=True)

            # Normalize player names
            modeling_data["player"] = modeling_data["player"].str.replace(".", "", regex=False)
            modeling_data["player"] = modeling_data["player"].apply(remove_accents)

            # Convert time played to decimal format
            modeling_data["min"] = modeling_data["min"].apply(convert_minutes_to_decimal)

            # Define rolling features
            features_for_rolling = list(modeling_data.columns[1:21])

            # Sort data for rolling calculations
            modeling_data.sort_values(by=["player", "game_date"], inplace=True)

            # Create a copy for prediction data
            prediction_data = modeling_data.copy()

            # Generate season format
            for df in [modeling_data, prediction_data]:
                df["season"] = df["game_date"].apply(
                    lambda x: f"{x.year}-{x.year + 1}" if x.month >= 10 else f"{x.year - 1}-{x.year}"
                )

            # Compute rolling averages and momentum
            for feature in features_for_rolling:
                # 3-game rolling average (shifted to prevent data leakage)
                modeling_data[f"{feature}_3gm_avg"] = (
                    modeling_data.groupby("player")[feature]
                    .apply(lambda x: x.shift(1).rolling(3, min_periods=3).mean())
                    .reset_index(level=0, drop=True)
                    .round(2)
                )

                prediction_data[f"{feature}_3gm_avg"] = (
                    prediction_data.groupby("player")[feature]
                    .rolling(3, min_periods=3)
                    .mean()
                    .reset_index(level=0, drop=True)
                    .round(2)
                )

                # Season-long rolling average (shifted to prevent data leakage)
                modeling_data[f"{feature}_season"] = (
                    modeling_data.groupby(["player", "season"])[feature]
                    .expanding()
                    .mean()
                    .shift(1)
                    .reset_index(level=[0, 1], drop=True)
                    .round(2)
                )

                prediction_data[f"{feature}_season"] = (
                    prediction_data.groupby(["player", "season"])[feature]
                    .expanding()
                    .mean()
                    .reset_index(level=[0, 1], drop=True)
                    .round(2)
                )

                # Momentum feature
                modeling_data[f"{feature}_momentum"] = (
                    modeling_data[f"{feature}_season"] - modeling_data[f"{feature}_3gm_avg"]
                )
                prediction_data[f"{feature}_momentum"] = (
                    prediction_data[f"{feature}_season"] - prediction_data[f"{feature}_3gm_avg"]
                )

            # Append processed data
            model_data.append(modeling_data)
            predict_data.append(prediction_data)

        print("Merging all seasons...")

        # Combine all processed data
        model_data = pd.concat(model_data, ignore_index=True)
        predict_data = pd.concat(predict_data, ignore_index=True)

        # Fill NaN values (excluding game_date)
        for df in [model_data, predict_data]:
            df.loc[:, df.columns != "game_date"] = df.drop(columns=["game_date"]).fillna(0)

        # Assign season start year
        for df in [model_data, predict_data]:
            df["season_start_year"] = df["season"].apply(lambda x: int(x.split("-")[0])).astype("Int64")

        print("Uploading processed data to BigQuery...")

        # Upload data to BigQuery
        destination_tables = {
            "capstone_data.player_modeling_data_partitioned": model_data,
            "capstone_data.player_prediction_data_partitioned": predict_data,
        }

        for table_name, df in destination_tables.items():
            pandas_gbq.to_gbq(
                df,
                destination_table=table_name,
                project_id="miscellaneous-projects-444203",
                if_exists="replace",
                credentials=credentials if not local else None,
                table_schema=[{"name": "game_date", "type": "DATE"}],
            )

        print("Data upload complete.")

        send_email(
            subject="PAST NBA PLAYER DATA CLEANED",
            body="Past data successfully uploaded to BigQuery."
        )

    except Exception as e:
        send_email(
            subject="PAST NBA PLAYER Cleaning Failed",
            body=f"Error: {e}"
        )



def clean_past_team_ratings():
    """Cleans and processes past NBA team ratings for modeling and prediction.

    This function retrieves historical team data from BigQuery, performs data 
    cleaning, computes rolling averages and momentum, and uploads the processed 
    data back to BigQuery.
    
    Raises:
        Exception: If data cleaning or processing fails.
    """
    model_data = []
    predict_data = []

    # Load BigQuery credentials
    try:
        credentials = service_account.Credentials.from_service_account_file(
            "/home/aportra99/scraping_key.json"
        )
        local = False
        print("Credentials file loaded.")
    except FileNotFoundError:
        local = True
        credentials = None
        print("Running with default credentials.")

    # Generate table names for each season
    tables = [f"{i}-{i+1}_team_ratings" for i in range(2015, 2025)]

    for season in tables:
        modeling_query = f"""
        SELECT *
        FROM `capstone_data.{season}`
        ORDER BY `game date` ASC
        """

        print(f"Fetching data from table: {season}")

        # Retrieve data from BigQuery
        modeling_data = pandas_gbq.read_gbq(
            modeling_query,
            project_id="miscellaneous-projects-444203",
            credentials=credentials if not local else None,
        )

        # Standardize column names
        modeling_data.rename(columns={"game date": "game_date"}, inplace=True)

        # Sort data for rolling calculations
        modeling_data.sort_values(by=["team", "game_date"], inplace=True)

        # Copy for prediction data
        prediction_data = modeling_data.copy()

        # Identify numerical columns for rolling calculations
        num_columns = modeling_data.columns[5:19]

        # Assign season values
        for df in [modeling_data, prediction_data]:
            df["season"] = df["game_date"].apply(
                lambda x: f"{x.year}-{x.year + 1}" if x.month >= 10 else f"{x.year - 1}-{x.year}"
            )
            df["season_start_year"] = df["season"].apply(lambda x: int(x.split("-")[0]))

        # Compute rolling averages and momentum
        for column in num_columns:
            # 3-game rolling average (shifted to prevent data leakage)
            modeling_data[f"{column}_3gm_avg"] = (
                modeling_data.groupby("team")[column]
                .apply(lambda x: x.shift(1).rolling(3, min_periods=3).mean())
                .reset_index(level=0, drop=True)
                .round(2)
            )

            prediction_data[f"{column}_3gm_avg"] = (
                prediction_data.groupby("team")[column]
                .rolling(3, min_periods=3)
                .mean()
                .reset_index(level=0, drop=True)
                .round(2)
            )

            # Season-long rolling average (shifted to prevent data leakage)
            modeling_data[f"{column}_season"] = (
                modeling_data.groupby(["team", "season"])[column]
                .expanding()
                .mean()
                .shift(1)
                .reset_index(level=[0, 1], drop=True)
                .round(2)
            )

            prediction_data[f"{column}_season"] = (
                prediction_data.groupby(["team", "season"])[column]
                .expanding()
                .mean()
                .reset_index(level=[0, 1], drop=True)
                .round(2)
            )

            # Momentum feature
            modeling_data[f"{column}_momentum"] = (
                modeling_data[f"{column}_season"] - modeling_data[f"{column}_3gm_avg"]
            )
            prediction_data[f"{column}_momentum"] = (
                prediction_data[f"{column}_season"] - prediction_data[f"{column}_3gm_avg"]
            )

        # Append processed data
        model_data.append(modeling_data)
        predict_data.append(prediction_data)

    print("Merging all seasons...")

    # Combine all processed data
    model_data = pd.concat(model_data, ignore_index=True)
    predict_data = pd.concat(predict_data, ignore_index=True)

    # Fill NaN values (excluding game_date)
    for df in [model_data, predict_data]:
        df.loc[:, df.columns != "game_date"] = df.drop(columns=["game_date"]).fillna(0)

    # Assign season start year (again for safety)
    for df in [model_data, predict_data]:
        df["season_start_year"] = df["season"].apply(lambda x: int(x.split("-")[0])).astype("Int64")

    print("Uploading processed data to BigQuery...")

    # Upload data to BigQuery
    destination_tables = {
        "capstone_data.team_modeling_data_partitioned": model_data,
        "capstone_data.team_prediction_data_partitioned": predict_data,
    }

    for table_name, df in destination_tables.items():
        pandas_gbq.to_gbq(
            df,
            destination_table=table_name,
            project_id="miscellaneous-projects-444203",
            table_schema=[{"name": "game_date", "type": "DATE"}],
            credentials=credentials if not local else None,
            if_exists="replace",
        )

    print("Data upload complete.")

    send_email(
        subject="PAST NBA TEAM RATINGS CLEANED",
        body="Past team ratings successfully uploaded to BigQuery."
    )



def clean_current_team_ratings(game_data):
    """Cleans and processes current NBA team ratings for modeling and prediction.

    Args:
        game_data (pd.DataFrame): Raw team ratings data.

    Raises:
        Exception: If data cleaning or processing fails.
    """
    try:
        # Load BigQuery credentials
        try:
            credentials = service_account.Credentials.from_service_account_file(
                "/home/aportra99/scraping_key.json"
            )
            local = False
            print("Credentials file loaded.")
        except FileNotFoundError:
            local = True
            credentials = None
            print("Running with default credentials.")

        # Determine the current season
        today = dt.today().date()
        season = today.year if today.month >= 10 else today.year - 1

        print(f"Processing current team ratings for season {season}...")

        # Standardize column names
        game_data.rename(columns={"game date": "game_date"}, inplace=True)

        # Extract unique teams
        teams = game_data["team"].unique()

        # SQL queries for past data
        modeling_query = f"""
        WITH RankedGames AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY team ORDER BY game_date DESC) AS game_rank
            FROM `capstone_data.team_modeling_data_partitioned`
            WHERE team IN ({','.join([f'"{team}"' for team in teams])}) 
            AND season_start_year = {season}
        )
        SELECT * FROM RankedGames
        ORDER BY team, game_date DESC;
        """

        prediction_query = f"""
        WITH RankedGames AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY team ORDER BY game_date DESC) AS game_rank
            FROM `capstone_data.team_prediction_data_partitioned`
            WHERE team IN ({','.join([f'"{team}"' for team in teams])}) 
            AND season_start_year = {season}
        )
        SELECT * FROM RankedGames
        ORDER BY team, game_date DESC;
        """

        # Retrieve past modeling and prediction data
        print("Fetching past modeling and prediction data...")
        modeling_data = pandas_gbq.read_gbq(
            modeling_query, project_id="miscellaneous-projects-444203", credentials=credentials if not local else None
        )

        prediction_data = pandas_gbq.read_gbq(
            prediction_query, project_id="miscellaneous-projects-444203", credentials=credentials if not local else None
        )

        # Assign season values
        for df in [modeling_data, prediction_data]:
            df["season"] = df["game_date"].apply(
                lambda x: f"{x.year}-{x.year + 1}" if x.month >= 10 else f"{x.year - 1}-{x.year}"
            )

        # Identify numerical features for rolling calculations
        features_for_rolling = game_data.columns[5:19]

        # Store cleaned team data
        team_dfs = []
        predict_dfs = []

        for team in teams:
            team_data = game_data[game_data["team"] == team].copy()
            predict_data = game_data[game_data["team"] == team].copy()

            data_for_rolling = modeling_data[modeling_data["team"] == team].sort_values(by="game_date")
            predict_data_for_rolling = prediction_data[prediction_data["team"] == team].sort_values(by="game_date")

            for feature in features_for_rolling:
                # 3-game rolling average (shifted to prevent data leakage)
                rolling_avg = data_for_rolling.groupby("team")[feature].shift(1).rolling(3, min_periods=3).mean()
                predict_avg = predict_data_for_rolling.groupby("team")[feature].rolling(3, min_periods=3).mean()

                team_data[f"{feature}_3gm_avg"] = rolling_avg.iloc[-1] if not rolling_avg.empty else 0
                predict_data[f"{feature}_3gm_avg"] = predict_avg.iloc[-1] if not predict_avg.empty else 0

                # Season-long rolling average (shifted to prevent data leakage)
                team_data[f"{feature}_season"] = (
                    modeling_data.groupby(["team", "season"])[feature].expanding().mean().shift(1).reset_index(level=[0, 1], drop=True)
                )

                predict_data[f"{feature}_season"] = (
                    prediction_data.groupby(["team", "season"])[feature].expanding().mean().reset_index(level=[0, 1], drop=True)
                )

                # Momentum feature
                team_data[f"{feature}_momentum"] = team_data[f"{feature}_season"] - team_data[f"{feature}_3gm_avg"]
                predict_data[f"{feature}_momentum"] = predict_data[f"{feature}_season"] - predict_data[f"{feature}_3gm_avg"]

            team_dfs.append(team_data)
            predict_dfs.append(predict_data)

        print("Combining processed data...")

        # Combine all processed data
        team_data = pd.concat(team_dfs, ignore_index=True)
        predict_data = pd.concat(predict_dfs, ignore_index=True)

        # Fill NaN values (excluding game_date)
        for df in [team_data, predict_data]:
            df.loc[:, df.columns != "game_date"] = df.drop(columns=["game_date"]).fillna(0)

        # Assign season start year
        for df in [team_data, predict_data]:
            df["season_start_year"] = season

        print("Uploading processed data to BigQuery...")

        # Upload data to BigQuery
        destination_tables = {
            "capstone_data.team_modeling_data_partitioned": team_data,
            "capstone_data.team_prediction_data_partitioned": predict_data,
        }

        for table_name, df in destination_tables.items():
            pandas_gbq.to_gbq(
                df,
                destination_table=table_name,
                project_id="miscellaneous-projects-444203",
                table_schema=[{"name": "game_date", "type": "DATE"}],
                credentials=credentials if not local else None,
                if_exists="append",
            )

        print("Data upload complete.")

        send_email(
            subject="NBA TEAM DATA CLEANED",
            body="Data successfully uploaded to Cleaned_team_ratings."
        )

    except Exception as e:
        send_email(
            subject="NBA TEAM DATA Cleaning Failed",
            body=f"Error: {e}"
        )
