# NBA Player Points Prediction — End-to-End ML Pipeline

A production-style machine learning system that predicts NBA player point totals and generates Over/Under betting recommendations against live DraftKings lines. The pipeline runs daily, pulling live data, generating predictions with a stacked ensemble model, and surfacing results through a Streamlit dashboard.

---

## Technical Highlights

- **Stacked ensemble architecture:** Linear Regression and LightGBM base models → weighted meta-model → calibrated classification model with custom Over/Under probability thresholds
- **Cloud-integrated pipeline:** Data stored and queried from Google BigQuery; predictions written back to BigQuery on each run
- **Real-time data ingestion:** Automated scraping of box scores from NBA.com and live betting lines from DraftKings using Selenium and BeautifulSoup
- **Feature engineering:** Rolling 3-game averages, season averages, and momentum indicators computed per player; opponent and team-level stats merged at inference time

---

## Architecture

```
NBA-Prediction-Project/
├── scrape_games.py           # Box scores and team stats from NBA.com
├── scrape_odds.py            # Live Over/Under lines from DraftKings
├── scrape_team_schedule.py   # Today's matchups and rosters
├── utils.py                  # Shared scraping utilities
├── cleaning_script.py        # Feature engineering and data prep
├── building_models.ipynb     # Model training and evaluation
├── model_utils.py            # Training helpers
├── predict_new_games.py      # Inference pipeline (roster → features → prediction)
├── main.py                   # Full historical data pipeline
├── run_predictions.py        # Daily: scrape odds → predict → upload to BigQuery
├── dashboard.py              # Streamlit dashboard
└── models/
    ├── models.pkl             # Base regression models (Linear, LightGBM)
    ├── meta_model.pkl         # Weighted ensemble of base models
    └── classification_models.pkl  # Over/Under classifier with tuned thresholds
```

---

## Modeling Pipeline

Predictions are generated in three stages:

**1. Base Regression Models**
Linear Regression and LightGBM independently predict a player's point total using player-level, team-level, and opponent-level features from BigQuery.

**2. Meta-Model**
A weighted ensemble combines the two base model predictions into a single point estimate. Weights are learned during training to minimize prediction error.

**3. Classification Model**
The ensemble prediction, raw model outputs, and a computed delta (prediction minus betting line) are fed into a calibrated classifier. Custom probability thresholds for Over and Under allow the model to abstain when confidence is low, outputting one of: `Over`, `Under`, or `No Bet Recommendation`.

---

## Data Pipeline

### Scraping
- `scrape_games.py` — pulls historical and current-season box scores from NBA.com's stats API using authenticated requests
- `scrape_odds.py` — extracts player point lines from DraftKings via Selenium
- `scrape_team_schedule.py` — identifies today's matchups and fetches current rosters from the NBA stats API

### Feature Engineering (`cleaning_script.py`)
- Rolling 3-game player averages (points, rebounds, assists, turnovers, etc.)
- Season-to-date averages
- Momentum indicators (recent trend vs. season baseline)
- Opponent and team stats merged at inference time from partitioned BigQuery tables

### Storage
All data is stored in Google BigQuery. Predictions and classifications are appended to BigQuery tables on each daily run, enabling historical tracking of model performance.

---

## Dashboard

The Streamlit dashboard (`dashboard.py`) displays:
- Today's player matchups with team logos and headshots
- Last 3 games of player stats
- Live DraftKings betting line
- Model recommendation (Over / Under / No Bet) with confidence probability

---

## How to Run

```bash
# Install dependencies
pip install -r requirements.txt

# Run the full historical data pipeline (scrape + clean + store)
python main.py

# Daily run: pull today's odds and generate predictions
python run_predictions.py

# Launch the dashboard
streamlit run dashboard.py
```

> **Note:** Requires Google Cloud service account credentials with BigQuery access.

---

## Stack

| Category | Tools |
|---|---|
| Data Collection | Selenium, BeautifulSoup, Requests |
| Storage | Google BigQuery, pandas-gbq |
| Modeling | scikit-learn, LightGBM, joblib |
| Dashboard | Streamlit |
| Language | Python 3 |
