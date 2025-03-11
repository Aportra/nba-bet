import pandas as pd
import pandas_gbq
import streamlit as st
from google.oauth2 import service_account
from datetime import datetime as dt,timedelta

# Function to clean player names for consistency
def clean_player_name(name):
    """Standardizes player names by removing special characters and handling known name variations."""
    name = name.lower().strip()  # Convert to lowercase & remove extra spaces
    name = name.replace(".", "")  # Remove periods
    name_corrections = {
        "alexandre sarr": "alex sarr",
        "jimmy butler": "jimmy butler iii",
        "nicolas claxton": "nic claxton",
        "kenyon martin jr": "kj martin",
        "carlton carrington": "bub carrington"
    }
    return name_corrections.get(name, name)

@st.cache_data
def pull_odds():
    tables = ['points', 'rebounds', 'assists', 'threes_made']
    odds_data = {}

    try:
        credentials = service_account.Credentials.from_service_account_file('/home/aportra99/scraping_key.json')
        local = False
    except FileNotFoundError:
        print("File not found, continuing as if on local")
        local = True
        credentials = None

    for table in tables:
        current_day = True
        odds_query = f"""
        SELECT * 
        FROM `capstone_data.{table}_predictions`
        WHERE DATE(Date_Updated) = CURRENT_DATE('America/Los_Angeles')

        """
        if local:
            odds_data[table] = pd.DataFrame(pandas_gbq.read_gbq(odds_query, project_id='miscellaneous-projects-444203'))
        else:
            odds_data[table] = pd.DataFrame(pandas_gbq.read_gbq(odds_query, project_id='miscellaneous-projects-444203', credentials=credentials))


        if odds_data[table].empty:
            current_day = False
            odds_query = f"""
            SELECT * 
            FROM `capstone_data.{table}_predictions`
            WHERE DATE(Date_Updated) = Date_sub(CURRENT_DATE('America/Los_Angeles'),interval 1 day)

            """
            if local:
                odds_data[table] = pd.DataFrame(pandas_gbq.read_gbq(odds_query, project_id='miscellaneous-projects-444203'))
            else:
                odds_data[table] = pd.DataFrame(pandas_gbq.read_gbq(odds_query, project_id='miscellaneous-projects-444203', credentials=credentials))


        # Clean player names for consistency
        odds_data[table]['Player'] = odds_data[table]['Player'].apply(clean_player_name)

    return odds_data,current_day

@st.cache_data
def pull_images():
    query = "SELECT * FROM `capstone_data.player_images`"
    player_images = pandas_gbq.read_gbq(query, project_id='miscellaneous-projects-444203')

    player_images['images'] = player_images['images'].fillna('')
    player_images['images'] = player_images['images'].apply(lambda x: x.replace("h=80", "h=254").replace("w=110", "w=350"))
    player_images['players'] = player_images['players'].apply(clean_player_name)
    return player_images

def make_dashboard(player_images, odds_data,current_day):
    if current_day:
        today = dt.today().date()
        st.title(f"NBA Player Betting Odds {today}")
    else:
        today = dt.today().date() - timedelta(days=1)
        st.title(f"NBA Player Betting Odds {today}")
        st.subheader("Displaying yesterday's values. Today's values have not been updated yet")

    if player_images.empty:
        st.error("No player images found. Please check your dataset.")
        return

    # Category filter dropdown
    category = st.selectbox("Select a category:", ["All", "Points", "Assists", "Rebounds", "Threes Made"])

    # Search bar for players
 

    # Dynamically populate players based on category selection
    available_players = set()
    if category == "All":
        # Collect all unique players across all categories
        for table in odds_data:
            if not odds_data[table].empty:
                available_players.update(odds_data[table]['Player'].unique())
    else:
        # Filter based on selected category
        category_key = category.lower().replace(" ", "_")
        if category_key in odds_data and not odds_data[category_key].empty:
            available_players = odds_data[category_key]['Player'].unique()

    available_players = sorted([player.title() for player in available_players])
    # Player selection dropdown
    player_selected = st.selectbox("Search or Select a Player:", [""] + available_players).lower()

    # Prepare for image lookup
    player_images['players_lower'] = player_images['players'].str.lower()

    selected_image = None
    player_name = None

    # Search functionality (exact match using lower case)
 
    if player_selected != "None":
        player_row = player_images.loc[player_images['players'] == player_selected]
        player_name = player_selected
        selected_image = player_row['images'].values[0] if len(player_row['images'].values) > 0 else None

    # Display player image
    if selected_image:
        st.subheader(f"{player_name.title()}")
        st.image(selected_image, width=250)
    else:
        st.write("No player found. Please check the name.")

    # Show betting odds for the selected player
    if player_name:
        st.subheader(f"Betting Odds for {player_name.title()}")

        # Collect odds across categories and add a category label to each subset
        player_odds = []
        for (table_name, df),cat in zip(odds_data.items(),['pts','reb','ast','3pm']):
            if player_name in df['Player'].values:
                temp_df = df[df['Player'] == player_name][[f'{table_name}','Over','Under',f'recommendation_{cat}_linear_model',f'recommendation_{cat}_lightgbm']].copy()  # Label the odds with the category
                player_odds.append((table_name,temp_df))

        if player_odds:
            # If odds exist in just one category, display that table directly
            if len(player_odds) == 1:
                st.dataframe(player_odds[0][1])
            else:
                # Display odds for each category separately
                for table_name,odds in player_odds:
                    st.markdown(f"**{table_name.capitalize()} Odds**")
                    st.dataframe(odds.style.hide(axis='index'))
        else:
            st.write("No odds available for this player today.")

images = pull_images()
odds_data,current_day = pull_odds()
make_dashboard(images, odds_data,current_day)
