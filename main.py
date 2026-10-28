from scraping_data.scrape_games import scrape_current_games
from cleaning_data.cleaning_script import clean_current_player_data, clean_current_team_ratings
from outcomes import current_outcome
from scraping_data.todays_matchups import get_matchups
from datetime import datetime,timedelta

matchups = get_matchups()

if len(matchups) == 0:
    print("no games today")
else:
    print("Starting scraping of game data")
    dt = datetime.today()
    team_data, player_data, date = scrape_current_games(0, dt, 0)

    print("Cleaning Data")

    clean_current_player_data(player_data, dt)

    clean_current_team_ratings(team_data)

    # current_outcome(player_data, date)

