from scraping_data.scrape_games import scrape_current_games
from scraping_data.scrape_team_data import scrape_current_team_data
from cleaning_data.cleaning_script import clean_current_player_data

print("Starting scraping of game data")

data,credentials,length = scrape_current_games()

print("Cleaning Data")

clean_current_player_data(data,credentials,local = True)

# print("Starting scraping of team data")

# scrape_current_team_data(length) 