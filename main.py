from scraping_data.scrape_games import scrape_current_games
from scraping_data.scrape_team_data import scrape_current_team_data
print("Starting scraping of game data")

length = scrape_current_games()

print("Starting scraping of team data")

scrape_current_team_data(length)