import pandas as pd
import pandas_gbq
from datetime import date as dt
# from datetime import timedelta
from google.oauth2 import service_account
import requests


def get_matchups(local=False):
    # Set today's date in MM/DD/YYYY format
    today = dt.today()
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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nba.com/stats/",
    "Origin": "https://www.nba.com"}

    # Make the request
    response = requests.get(url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code == '200':
        return
    data = response.json()
    # BigQuery: pull team ID mapping

    # Parse game data
    result_set = data['resultSets'][0]  # GameHeader
    games_df = pd.DataFrame(result_set['rowSet'], columns=result_set['headers'])

    # Merge team abbreviations

    return games_df 
