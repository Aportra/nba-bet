from scraping_data.scrape_odds import gather_odds 
from models.predict_new_games import run_predictions
from scraping_data.todays_matchups import get_matchups
import pandas_gbq
matchups = get_matchups()
print(matchups)
if matchups is None:
    print("no games today")
else:
    odds = gather_odds()
    run_predictions(odds,matchups)
