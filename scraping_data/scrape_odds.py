"""Module for scraping NBA player prop odds from DraftKings and uploading to BigQuery."""
import json
import yaml
import time
import traceback
from datetime import datetime as dt
from datetime import timedelta,timezone
import requests
import pandas as pd
import pandas_gbq
from google.oauth2 import service_account

with open('scraping_data/config.yaml', mode='r') as file:
    config = yaml.safe_load(file)

    api_key = config['api']



def gather_events():

    today = dt.today().replace(
        hour=7, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
    )

    tomorrow = (dt.today() + timedelta(days=1)).replace(
        hour=7, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
    )
    tomorrow = tomorrow.strftime("%Y-%m-%dT%H:%M:%SZ")
    today = today.strftime("%Y-%m-%dT%H:%M:%SZ")
    data = requests.get(f'https://api.the-odds-api.com/v4/sports/basketball_nba/events?apiKey={api_key}&commenceTimeFrom={today}&commenceTimeTo={tomorrow}')
    events = [data.json()[event]['id'] for event in range(len(data.json()))]
    events = list(set(events))

    return events
# v4/sports/{sport}/events/{eventId}/odds?apiKey={apiKey}&regions={regions}&markets={markets}&dateFormat={dateFormat}&oddsFormat={oddsFormat}

def process_categories(events):
    """Scrapes NBA player prop odds from DraftKings and uploads data to BigQuery."""
    full_data = []
    for event in range(len(events)):
        url =f'https://api.the-odds-api.com/v4/sports/basketball_nba/events/{events[event]}/odds?apiKey={api_key}&regions=us&markets=player_points&oddsFormat=american'
        data = requests.get(url)
        for i in range(len(data.json())):
            full_data.append(data.json())

    return full_data


def gather_odds():
    events = gather_events()
    print(len(events))
    if len(events) > 4:
        data = process_categories(events[:4])
    else:
        data = process_categories(events)

    parsed_json = data[0]['bookmakers'][0]['markets'][0]['outcomes']
    output = {'Player':[],'points':[],'Over':[],'Under':[],'Date_Updated':[]}

    for i in range(len(parsed_json)):
        if parsed_json[i]['name'] == 'Over':
            output['Over'].append(parsed_json[i]['price'])
            output['Player'].append(parsed_json[i]['description'])
            output['points'].append(parsed_json[i]['point'])
        else:
            output['Under'].append(parsed_json[i]['price'])
            output['Date_Updated'].append(pd.to_datetime(dt.today()))

    df = pd.DataFrame(output)

    pandas_gbq.to_gbq(
        df,
        project_id="miscellaneous-projects-444203",
        destination_table="miscellaneous-projects-444203.capstone_data.player_points_odds",
        if_exists="append"
    )
