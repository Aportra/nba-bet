"""Module for cleaning and processing current NBA player data before uploading to BigQuery."""

from datetime import datetime as dt
from google.oauth2 import service_account
from scraping_data.utils import send_message
from scraping_data.utils import psql

import numpy as np
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
    return ''.join(c for c in unicodedata.normalize('NFKD', input_str) if not unicodedata.combining(c))


def convert_minutes_to_decimal(min_played):
    """Converts minutes played from MM:SS format to a decimal representation.

    Args:
        min_played (str): Time in MM:SS format.

    Returns:
        float: Decimal representation of minutes.
    """
    if not isinstance(min_played, str) or ':' not in min_played:
        return np.nan  # Or np.nan, or whatever default you want
    else:
        minutes, seconds = map(int, min_played.split(':'))
        return minutes + seconds / 60


def clean_current_player_data(data, date):
    """Cleans and processes NBA player data for modeling and prediction.

    Args:
        data (pd.DataFrame): Raw player game data.

    Raises:
        Exception: If data cleaning or processing fails.
    """
    conn = psql()
    try:
        # Drop missing values and reset index
        data.rename(columns={'team_abbreviation':'team','player_name':'player'},inplace=True)
        print("Columns after renaming:", data.columns)
        # Normalize player names (remove dots and accents)
        data['player'] = data['player'].str.replace('.', '', regex=False)
        data['player'] = data['player'].apply(remove_accents)
        data['game_date'] = date
        # Convert time played to decimal format
        data['min'] = data['min'].apply(convert_minutes_to_decimal)
        data.dropna(inplace=True, ignore_index=True)
        # Standardize column names to lowercase
        data.rename(columns=str.lower, inplace=True)

        # Identify current season
        today = dt.today().date()
        season = today.year if today.month >= 10 else today.year - 1
        print(season)
        # Generate season format for each row
        data['season'] = data['game_date'].apply(
            lambda x: f"{x.year}-{x.year + 1}" if x.month >= 10 else f"{x.year - 1}-{x.year}"
        )

        # Extract unique player names
        players = data['player'].unique()
        print(f"Processing {len(players)} players.")

        # Define SQL queries for fetching past data

        prediction_query = f"""
        WITH RankedGames AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY player ORDER BY game_date DESC) AS game_rank
            FROM clean_player_data
            WHERE player IN ({','.join([f"'{player}'" for player in players])})
        )
        SELECT * FROM RankedGames
        where game_rank <= 5
        ORDER BY player, game_date DESC;
        """

        print("Fetching past modeling and prediction data...")

        # Fetch past modeling and prediction data from BigQuery

        predict_data = conn.query(prediction_query)

        # Define features for rolling averages
        exclude_cols = ["team_id", "game_id", "player_id"]
        features_for_rolling = [col for col in data.select_dtypes(include=['int64', 'float64']).columns if col not in exclude_cols]



        # Prepare data for modeling and prediction
        prediction_dfs = []

        for player in players:
            prediction_data = data[data['player'] == player].copy()

            past_predict_data = predict_data[predict_data['player'] == player].sort_values(by='game_date')

            for feature in features_for_rolling:
                # Compute 3-game rolling averages, preventing data leakage
                if feature == 'fgthree_m':
                    feat = 'fgthree_m'
                    predict_avg = past_predict_data.groupby('player')[feat].rolling(3, min_periods=3).mean()
                else:
                    predict_avg = past_predict_data.groupby('player')[feature].rolling(3, min_periods=3).mean()
                prediction_data[f'{feature}_three_gm_avg'] = predict_avg.iloc[-1] if not predict_avg.empty else 0

                # Compute season averages and momentum
                if feature == 'fgthree_m':
                    feat = 'fgthree_m'
                    predict_season_avg = past_predict_data.groupby(['player', 'season'])[feat].expanding().mean()

                else:
                    predict_season_avg = past_predict_data.groupby(['player', 'season'])[feature].expanding().mean()

                prediction_data[f'{feature}_season'] = predict_season_avg.iloc[-1] if not predict_season_avg.empty else 0

                prediction_data[f'{feature}_momentum'] = prediction_data[f'{feature}_season'] - prediction_data[f'{feature}_three_gm_avg']

            prediction_dfs.append(prediction_data)

        print("Rolling features calculated.")

        # Combine and format final datasets
        predict_data = pd.concat(prediction_dfs, ignore_index=True)


        print(f"Prediction Data Shape: {predict_data.shape}")

        # Fill NaNs with 0 for modeling
        predict_data.fillna(0, inplace=True)

        # Assign season start year
        predict_data['season_start_year'] = season

        (predict_data.drop_duplicates(subset=['game_id', 'player_id'],
                                      keep='first',
                                      inplace=True))
        print(predict_data)
        # Upload to BigQuery

        conn.upload_data(predict_data, 'clean_player_data')
        send_message("player_data cleaned and uploaded")
    except Exception as e:
        send_message(f"Cleaning Script Crashed: {e}")
        print('Error:', e)


def clean_current_team_ratings(game_data):
    """Cleans and processes current NBA team ratings for modeling and prediction.

    Args:
        game_data (pd.DataFrame): Raw team ratings data.

    Raises:
        Exception: If data cleaning or processing fails.
    """
    conn = psql()
    try:
        # Load BigQuery credentials

        # Determine the current season
        today = dt.today().date()
        season = today.year if today.month >= 10 else today.year - 1

        print(f"Processing current team ratings for season {season}...")

        # Standardize column names
        game_data.rename(columns={'team_abbreviation':'team'},inplace=True)
        # Extract unique teams
        teams = game_data["team"].unique()

        # SQL queries for past data

        prediction_query = f"""
        WITH RankedGames AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY team ORDER BY game_date DESC) AS game_rank
            FROM clean_team_data
            WHERE team IN ({','.join([f"'{team}'" for team in teams])})
        )
        SELECT * FROM RankedGames
        where game_rank <= 5
        ORDER BY team, game_date DESC;
        """

        # Retrieve past modeling and prediction data
        print("Fetching past modeling and prediction data...")

        prediction_data = conn.query(prediction_query)
        # Assign season values
        for df in [prediction_data]:
            df["season"] = df["game_date"].apply(
                lambda x: f"{x.year}-{x.year + 1}" if x.month >= 10 else f"{x.year - 1}-{x.year}"
            )

        # Identify numerical features for rolling calculations
        exclude_cols = ["team_id", "game_id", "player_id"]
        features_for_rolling = [col for col in game_data.select_dtypes(include=['int64', 'float64']).columns if col not in exclude_cols]



        # Store cleaned team data
        team_dfs = []
        predict_dfs = []

        for team in teams:
            predict_data = game_data[game_data["team"] == team].copy()

            predict_data_for_rolling = prediction_data[prediction_data["team"] == team].sort_values(by="game_date")

            for feature in features_for_rolling:
                # 3-game rolling average (shifted to prevent data leakage)
                predict_avg = predict_data_for_rolling.groupby("team")[feature].rolling(3, min_periods=3).mean()

                predict_data[f"{feature}_three_gm_avg"] = predict_avg.iloc[-1] if not predict_avg.empty else 0

                # Season-long rolling average (shifted to prevent data leakage)

                predict_data[f"{feature}_season"] = (
                    prediction_data.groupby(["team", "season"])[feature].expanding().mean().reset_index(level=[0, 1], drop=True)
                )

                # Momentum feature
                predict_data[f"{feature}_momentum"] = predict_data[f"{feature}_season"] - predict_data[f"{feature}_three_gm_avg"]

            predict_dfs.append(predict_data)

        print("Combining processed data...")

        # Combine all processed data
        predict_data = pd.concat(predict_dfs, ignore_index=True)

        # Fill NaN values (excluding game_date)
        for df in [predict_data]:
            df.loc[:, df.columns != "game_date"] = df.drop(columns=["game_date"]).fillna(0)

        # Assign season start year
        for df in [predict_data]:
            df["season_start_year"] = season

        # Upload data to BigQuery
        destination_tables = {
            "clean_team_data": predict_data,
        }

        for table_name, df in destination_tables.items():
            conn.upload_data(df, table_name)

        print("Data upload complete.")

        send_message("NBA Team data cleaned and uploaded")

    except Exception as e:
        print(f'data failed {e}')
        send_message(
            f"NBA TEAM DATA Cleaning Failed Error: {e}"
        )
