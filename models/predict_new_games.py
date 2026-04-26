from google.oauth2 import service_account
from datetime import datetime as date
from models import model_utils
import joblib
import pandas as pd
import os

current_wd = os.getcwd()
print(current_wd)
#Connect to PSQL
conn = model_utils.psql()


def recent_player_data(odds_data, games):
    """Fetches recent player, team, and opponent data from BigQuery."""
    print("Fetching recent player, team, and opponent data...")

    today = date.today()
    season =int(today.year if today.month >= 10 else today.year - 1)

    filtered_players = tuple(games['player_id'])
    teams = tuple(set(games['team_id']))

    print(f'player length:{len(filtered_players)}')
    print(f'team length: {len(teams)}')
    if not filtered_players:
        print("No valid players found.")
        return None, None
    queries = {
        "player_data": f"""
        WITH RankedGames AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY player ORDER BY game_date DESC) AS game_rank
            FROM clean_player_data
            WHERE player_id IN {filtered_players}
            AND season_start_year = '{season}'
        )
        SELECT *
        FROM RankedGames
        where game_rank = 1;
    """,
        "team_data": f"""
        WITH RankedGames AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY team ORDER BY game_date DESC) AS game_rank
            FROM clean_team_data
            WHERE team_id IN {teams}
            AND season_start_year = '{season}'
        )
        SELECT *
        FROM RankedGames
        where game_rank = 1;
    """}

    # Fetch player, opponent, and team data
    player_data, team_data = [conn.query(queries[q]) for q in queries]

    print('queries complete')
    # Standardize player names in player_data
    print('cleaning names')
    len(f'player_data length: {player_data}')
    len(f'team_data length:{team_data}')

    team_data_1 = team_data.copy()

    team_data_1.rename(columns={'team_id':'opponent'},inplace=True)

    # print(player_data)
    # print(team_data)
    # print(team_data_1)

    pd.set_option('display.max_rows', None)  # Show all rows
    pd.set_option('display.max_columns', None)  # Show all columns
    pd.set_option('display.expand_frame_repr', False)
    # Merge datasets while keeping only necessary columns
    full_data = (player_data.merge(games, on="player_id", how="inner",
                 suffixes=("", "_remove")))
    full_data = full_data.merge(team_data, on = 'team_id', how = 'left',
                suffixes = ("", "_remove"))
    full_data = full_data.merge(team_data_1, on= 'opponent', how = 'left',suffixes = ("","_opponent"))

    print('print full data post merge', len(full_data['player'].unique()))
    # Drop duplicate or unnecessary columns

    full_data.dropna(axis=1, inplace=True)
    print(full_data['player'])
    return full_data, odds_data


def predict_games(full_data, odds_raw):
    """Predicts NBA player stats using pre-trained models and compares with betting odds."""
    print('loading models...')
    # Load models
    # models = joblib.load('/home/aportra99/nba-bet/models/models.pkl')
    full_data = full_data
    odds_data = {'points':odds_raw}
    odds = {}
    lowest_data = {}
    print(odds_data['points'])
    print(current_wd)
    models = joblib.load(f'{current_wd}/models/models.pkl')
    for key, odds_df in odds_data.items():
        # Filter relevant players
        data_ordered = full_data[full_data['player'].isin(odds_df['Player'])].copy()

        print(f"Filtered {len(data_ordered)} players for {key} predictions.")
        # Players in odds_df that are not in full_data
        players_not_in_full_data = set(odds_df['Player']) - set(full_data['player'])

        # Players in odds_df that are not in data_ordered
        players_not_in_data_ordered = set(odds_df['Player']) - set(data_ordered['player'])

        # Display results
        print("Players in odds_df but not in full_data:", players_not_in_full_data)
        print("Players in odds_df but not in data_ordered:", players_not_in_data_ordered)


        # Ensure chronological order for calculations
        data_ordered.sort_values(by=['player', 'game_date'], inplace=True)

        latest_rows = data_ordered.groupby('player', as_index=False).tail(1)
        # Load Google Cloud credentials
        try:
            credentials = service_account.Credentials.from_service_account_file('/home/aportra99/scraping_key.json')
            local = False
        except FileNotFoundError:
            print("File not found, running in local mode.")
            local = True
            credentials = None

        # Determine category for prediction
        category_mapping = {
            "points": "pts"
        }
        category = category_mapping[key]

        # Run predictions using trained models
        for model_name, model in models[category].items():
            print(category)
            print(model_name)
            if model_name.lower() not in ["xgboost", "sarimax", "mlp", "random_forest"]:
                features = [f.strip() for f in model.feature_names_in_]

                if set(features).issubset(latest_rows.columns):
                    latest_rows[f'{category}_{model_name}'] = model.predict(latest_rows[features])
                else:
                    missing = set(features) - set(latest_rows.columns)
                    print(f"Skipping {model_name} for {category}: Missing features: {missing}")


        # Convert betting odds to numeric values
        for col in ['Over', 'Under']:
            odds_df[col] = pd.to_numeric(
                odds_df[col].astype(str).str.replace('−', '-', regex=False).str.replace('+', '', regex=False),
                errors='coerce'
            ).fillna(0).astype(int)
        # Merge predictions with odds data
        for idx, row in odds_df.iterrows():
            player_name = row['Player']
            matching_rows = latest_rows[latest_rows['player'] == player_name]

            if not matching_rows.empty:
                for model_name in models[category]:
                    if model_name not in ['lightgbm', 'linear_model']:
                        continue

                    col_name = f'{category}_{model_name}'
                    if col_name in matching_rows.columns:
                        print('0',matching_rows[col_name].values[0],matching_rows['game_date'].values[0],matching_rows['player'].values[0])
                        prediction_value = matching_rows[col_name].values[0]
                        print(col_name)
                        odds_df.at[idx, col_name] = prediction_value

                        # Determine betting recommendation
                        actual_value = float(row[key])
                        recommendation = 'Over' if prediction_value > actual_value else 'Under'
                        odds_df.at[idx, f'recommendation_{category}_{model_name}'] = recommendation
                    else:
                        print(f"Missing column {col_name} for {player_name} in {category}")
            else:
                print(f"Warning: No match found for {player_name} in category {category}")

        # Upload predictions to PSQL
        table_name = f'{key}_predictions'
        odds_df.dropna(axis=0, inplace = True)
        odds_df.drop_duplicates(keep='first', inplace=True)
        conn.upload_data(odds_df, table_name)
        odds[category] = odds_df
        lowest_data[category] = latest_rows
        print(f"Successfully uploaded {key} predictions.")

    return lowest_data, odds

#best bets tab added in

def classification(lowest_data,odds):
        
    ensemble = joblib.load(f'{current_wd}/models/meta_model.pkl')
    models = joblib.load(f'{current_wd}/models/classification_models.pkl')

    # Display settings
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.max_rows', None)

    categories = ['pts']
    lines = ['points']

    # Backup
    odds_raw = {cat: df.copy() for cat, df in odds.items()}

    for cat, line in zip(categories, lines):
        print(f"\n=== Category: {cat.upper()} ===")

        # Format odds
        for col in ['Over', 'Under']:
            odds[cat][col] = pd.to_numeric(
                odds[cat][col].astype(str).str.replace('−', '-', regex=False).str.replace('+', '', regex=False),
                errors='coerce'
            ).fillna(0).astype(int)

        odds[cat].rename(columns={'Player': 'player'}, inplace=True)

        # Check required model columns exist
        linear_col = f'{cat}_linear_model'
        lightgbm_col = f'{cat}_lightgbm'


        print(f"Columns available for {cat}: {list(odds[cat].columns)}")
        print(f"Looking for: {linear_col}, {lightgbm_col}")

        if linear_col not in odds[cat].columns or lightgbm_col not in odds[cat].columns:
            print(f"Skipping {cat} due to missing model cols.")
            continue
        # Ensemble score
        coef_linear, coef_lightgbm = ensemble[cat].coef_
        odds[cat][f'{cat}_ensemble'] = (
            odds[cat][linear_col] * coef_linear +
            odds[cat][lightgbm_col] * coef_lightgbm
        )

        # Merge with features
        all_data = odds[cat].merge(lowest_data[cat], on='player', how='inner')
        all_data[line] = pd.to_numeric(all_data[line], errors='coerce')

        # Recalc delta
        all_data[f'{cat}_delta'] = all_data[f'{cat}_ensemble'] - all_data[line]

        #Load model + thresholds
        model_dict = models[cat]
        clf = model_dict['Fitted_Model']
        threshold_over = model_dict['Over_Threshold']
        threshold_under = model_dict['Under_Threshold']

        # Ensure all expected features exist
        expected_features = list(clf.feature_names_in_)
        
        for col in expected_features:
            if col not in all_data.columns:
                all_data[col] = 0.0

        # Slice and match expected order
        filtered_data = all_data[expected_features].astype(float)
        assert list(filtered_data.columns) == list(clf.feature_names_in_)

        #Predict
        try:
            proba = clf.predict_proba(filtered_data.to_numpy())[:, 1]
        except Exception as e:
            print(f"Error predicting for {cat}: {e}")
            continue

        def classify(p): return 'Over' if p > threshold_over else 'Under' if p < threshold_under else 'No Bet Recommendation'

        all_data['proba'] = proba
        all_data['recommendation'] = all_data['proba'].apply(classify)

        for col in ['recommendation', 'proba']:
            if col in odds[cat].columns:
                odds[cat].drop(columns=col, inplace=True)

        # Clean up from all_data
        merged_predictions = all_data[['player', 'recommendation', 'proba']].drop_duplicates(subset='player')

        # Merge cleanly
        odds[cat] = odds[cat].merge(merged_predictions, on='player', how='left')


        #Optional sanity check
        if odds[cat].duplicated(subset='player').any():
            print(f"Duplicates found in {cat} after merge!")
        odds[cat].drop(columns =[f'{cat}_linear_model',f'{cat}_lightgbm',f'recommendation_{cat}_linear_model',f'recommendation_{cat}_lightgbm',f'{cat}_ensemble'],inplace= True)
        odds[cat].dropna(axis=0,inplace=True)
        odds[cat].drop_duplicates(keep='first',inplace=True)
        table_name = f'{cat}_classifications'
        conn.upload_data(odds[cat], table_name)


def run_predictions(odds_data, matchups):
    """Runs the full prediction pipeline from data gathering to model inference."""
    print("Running game predictions...")

    try:

        full_data, odds = recent_player_data(odds_data, matchups)

        lowest_data, odds = predict_games(full_data, odds_data)

        classification(lowest_data, odds)
        if full_data is None or odds_data is None:
            print("Failed to retrieve necessary data. Exiting...")
            return
    except Exception as e:
        print(e)

    conn.close()

