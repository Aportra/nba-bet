import pandas as pd
import pandas_gbq
import datetime as dt
from google.oauth2 import service_account
import requests


def get_matchups(local=False):
    # Set today's date in MM/DD/YYYY format
    today = dt.date.today().strftime('%m/%d/%Y')

    # Load credentials or set local mode
    try:
        credentials = service_account.Credentials.from_service_account_file(
            "/home/aportra99/scraping_key.json"
        )
        local = False
    except FileNotFoundError:
        print("File not found. Continuing as if on local.")
        local = True
        credentials = None

    # NBA API endpoint
    url = "https://stats.nba.com/stats/scoreboardv2"
    params = {
        "GameDate": today,
        "LeagueID": "00",
        "DayOffset": "0"
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.nba.com/",
        "Origin": "https://www.nba.com",
        "Accept": "application/json",
        "Connection": "keep-alive"
    }

    # Make the request
    response = requests.get(url, headers=headers, params=params)
    data = response.json()

    # BigQuery: pull team ID mapping
    season = 2024
    query = f""" 
        SELECT DISTINCT team, team_id
        FROM `capstone_data.team_prediction_data_partitioned`
        WHERE season_start_year = {season}
    """
    team_key = pandas_gbq.read_gbq(query, project_id="miscellaneous-projects-444203")

    # Parse game data
    result_set = data['resultSets'][0]  # GameHeader
    games_df = pd.DataFrame(result_set['rowSet'], columns=result_set['headers'])

    # Merge team abbreviations
    games_with_home = games_df.merge(
        team_key.rename(columns={'team_id': 'HOME_TEAM_ID', 'team': 'HOME_TEAM'}),
        on='HOME_TEAM_ID', how='left'
    )
    games_with_both = games_with_home.merge(
        team_key.rename(columns={'team_id': 'VISITOR_TEAM_ID', 'team': 'VISITOR_TEAM'}),
        on='VISITOR_TEAM_ID', how='left'
    )

    # Keep necessary columns
    games_clean = games_with_both[[
        'GAME_ID', 'GAME_DATE_EST', 'HOME_TEAM', 'VISITOR_TEAM'
    ]]

    # First half: home team perspective
    home_rows = games_clean.rename(columns={
        'HOME_TEAM': 'team',
        'VISITOR_TEAM': 'opponent'
    })[['GAME_ID', 'GAME_DATE_EST', 'team', 'opponent']]
    home_rows['home'] = 1

    # Second half: visitor team perspective
    away_rows = games_clean.rename(columns={
        'VISITOR_TEAM': 'team',
        'HOME_TEAM': 'opponent'
    })[['GAME_ID', 'GAME_DATE_EST', 'team', 'opponent']]
    away_rows['home'] = 0

    # Combine and sort
    flattened_schedule = pd.concat([home_rows, away_rows], ignore_index=True)
    flattened_schedule.sort_values(by=['GAME_DATE_EST', 'GAME_ID'], inplace=True)

    # Upload to BigQuery
    pandas_gbq.to_gbq(
        flattened_schedule,
        project_id="miscellaneous-projects-444203",
        destination_table='capstone_data.schedule',
        if_exists="append",
        credentials=credentials
    )
