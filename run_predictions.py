from scraping_data.scrape_odds import process_categories
from models.predict_new_games import run_predictions
from scraping_data.todays_matchups import get_matchups

get_matchups()
process_categories()
run_predictions()
