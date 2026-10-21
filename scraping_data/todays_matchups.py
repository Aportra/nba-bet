import pandas as pd
import pandas_gbq
from datetime import date as dt
# from datetime import timedelta
import requests


def get_matchups(local=False):
    # Set today's date in MM/DD/YYYY format
    today = dt.today()

    # NBA API endpoint
    url = "https://stats.nba.com/stats/scoreboardv3"
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
    print(response)
    data = response.json()
    # if response.status_code == '200' and :
    #     return

    games = [data['scoreboard']['games'][i]['gameId'] for i in range(len(data['scoreboard']['games']))]

    return games
