from scraping_data.scrape_games import scrape_current_games
from scraping_data.scrape_team_data import scrape_current_team_data
from cleaning_data.cleaning_script import clean_current_player_data,clean_current_team_ratings

print("Starting scraping of game data")

data,length = scrape_current_games()

print("Cleaning Data")

clean_current_player_data(data)
