# NBA Player Performance Prediction and Betting Recommendation System

## Overview
This project automates the process of scraping, cleaning, modeling, and predicting NBA player performances to provide betting recommendations based on Over/Under odds.

The pipeline involves:
1. Scraping NBA game data
2. Cleaning and structuring the data for modeling
3. Building predictive models (Linear Regression, LightGBM, etc.)
4. Comparing predictions against sportsbook Over/Under odds
5. Providing betting recommendations (Over/Under)
6. Displaying results in a Streamlit dashboard

---

## Project Structure

```
NBA-Prediction-Project/
│── scraping_data/          # Collects NBA player, team, and odds data  
│   ├── scrape_games.py  
│   ├── scrape_odds.py  
│   ├── scrape_team_data.py  
│   ├── scrape_team_schedule.py  
│   ├── utils.py  
│── cleaning_data/          # Cleans and prepares data for modeling  
│   ├── cleaning_script.py  
│── models/                 # Builds models and predicts game outcomes  
│   ├── building_models.ipynb  
│   ├── model_utils.py  
│   ├── predict_new_games.py  
│   ├── models.pkl          # Saved machine learning models  
│── dashboard/              # Visualizes predictions in Streamlit  
│   ├── dashboard.py  
│── main.py                 # Runs the full pipeline  
│── README.md               # Project documentation  
```

---

## Pipeline Workflow

### 1. Scraping Data (`scraping_data/`)
**Purpose:** Collect real-time and historical data on NBA players, teams, and odds.

- `scrape_games.py` - Scrapes box score data from NBA.com
- `scrape_team_data.py` - Collects team stats and ratings
- `scrape_team_schedule.py` - Scrapes team schedules
- `scrape_odds.py` - Extracts Over/Under betting lines from DraftKings

**Tools used:** Selenium, BeautifulSoup, Google BigQuery

---

### 2. Cleaning and Feature Engineering (`cleaning_data/`)
**Purpose:** Process raw data into structured datasets for modeling.

- `cleaning_script.py`  
  - Removes missing or incorrect values  
  - Converts time-based stats into usable formats  
  - Calculates rolling averages and momentum indicators (three-game average, season average)  

**Key transformations:**
- Player statistics rolling averages (last three games)
- Seasonal averages
- Momentum metrics (change in performance trends)

---

### 3. Building and Training Models (`models/`)
**Purpose:** Train machine learning models to predict player performance.

- `building_models.ipynb` - Trains models such as Linear Regression, LightGBM, and RandomForest
- `model_utils.py` - Helper functions for model training
- `models.pkl` - Pre-trained models for fast inference

**Models Used:**
- Linear Regression - Simple, interpretable baseline model
- LightGBM - Efficient gradient boosting for structured data
- RandomForest - Captures non-linear relationships

---

### 4. Making Predictions (`predict_new_games.py`)
**Purpose:** Predict player statistics for upcoming games and compare them to betting odds.

- Loads trained models (`models.pkl`)
- Predicts points, rebounds, assists, and three-pointers for each player
- Compares predictions with Over/Under lines
- Recommends "Over" or "Under" for each player

**Key logic:**
- If the predicted value is greater than the sportsbook line, the system recommends "Over"
- If the predicted value is less than the sportsbook line, the system recommends "Under"

---

### 5. Dashboard Visualization (`dashboard.py`)
**Purpose:** Display predictions and betting recommendations in a user-friendly dashboard.

Utilizes Streamlit to present:
- Player headshots
- Predicted statistics
- Sportsbook odds
- Over/Under recommendations

---

## How to Run the Project

### 1. Install Dependencies
```
pip install -r requirements.txt
```

### 2. Run the Full Pipeline
```
python main.py
```

### 3. Launch the Dashboard
```
streamlit run dashboard.py
```

---

## Future Improvements
- Enhance feature engineering, including team synergy, opponent strength, and fatigue factor
- Improve models by incorporating XGBoost and deep learning approaches
- Automate deployment with scheduled daily runs and alert notifications for top picks

---

## Conclusion
This project automates NBA player performance prediction and betting recommendations, combining web scraping, data engineering, machine learning, and visualization into a streamlined workflow.

