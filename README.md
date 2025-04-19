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
│   ├── scrape_team_schedule.py  
│   ├── utils.py  
│── cleaning_data/          # Cleans and prepares data for modeling  
│   ├── cleaning_script.py  
│── models/                 # Builds models and predicts game outcomes  
│   ├── building_models.ipynb  
│   ├── model_utils.py  
│   ├── predict_new_games.py
|   ├── models.pkl            # Saved machine learning models  
|   ├── classification_models.pkl 
│   ├── meta_models.pkl          
│── main.py                  # Runs the full data pipeline  
│── run_predictions.py       # Scrapes odds and generates predictions
│──  dashboard.py           # Visualizes predictions in Streamlit  
│── README.md               # Project documentation  
```

---

## Pipeline Workflow

### 1. Scraping Data (`scraping_data/`)
**Purpose:** Collect real-time and historical data on NBA players, teams, and odds.

- `scrape_games.py` - Scrapes box score data and team stats from NBA.com
- `scrape_team_schedule.py` - Scrapes team schedules
- `scrape_odds.py` - Extracts Over/Under betting lines from DraftKings

**Tools used:** Selenium, BeautifulSoup, Requests, Google BigQuery

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

- Loads trained models (`models.pkl`,`meta_model.pkl`,`classification_models.pkl`)
- Predicts points, rebounds, assists, and three-pointers for each player
- Feeds predictions into meta_model
- Meta model, and both regressions model then fed into classifcation_models
- Classification model then generates an Over or Under prediction
- Recommends "Over" or "Under" for each player

---

### 5. Dashboard Visualization (`dashboard.py`)
**Purpose:** Display predictions and betting recommendations in a user-friendly dashboard.

Utilizes Streamlit to present:
- Player headshots
- Team logos
- Past 3 games statistics
- Sportsbook odds
- Over/Under recommendations

---

## How to Run the Project

### 1. Install Dependencies
```
pip install -r requirements.txt
```

### 2. Run the Full Data Pipeline
```
python main.py
```

### 3. Pull todays odds and generate predictions
```
python run_predictions.py
```

### 4. Launch the Dashboard
```
streamlit run dashboard.py
```

---

## Future Improvements
- Enhance feature engineering, including team synergy, opponent strength, game context and fatigue factor
- Improve models by incorporating XGBoost and deep learning approaches


---

## Conclusion
This project automates NBA player performance prediction and betting recommendations, combining web scraping, data engineering, machine learning, and visualization into a streamlined workflow.

