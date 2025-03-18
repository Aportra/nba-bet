import streamlit as st
import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
from datetime import timedelta
import datetime as dt

# Function to clean player names for consistency
def clean_player_name(name):
    name = name.lower().strip().replace(".", "")
    name_corrections = {
        "alexandre sarr": "alex sarr",
        "jimmy butler": "jimmy butler iii",
        "nicolas claxton": "nic claxton",
        "kenyon martin jr": "kj martin",
        "carlton carrington": "bub carrington",
        "ron holland ii": "ronald holland ii",
        "cameron thomas": "cam thomas"
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
        local = True
        credentials = None

    for table in tables:
        odds_query = f"""
        SELECT * 
        FROM `capstone_data.{table}_predictions`
        WHERE DATE(Date_Updated) = CURRENT_DATE('America/Los_Angeles')
        """
        odds_data[table] = pandas_gbq.read_gbq(odds_query, project_id='miscellaneous-projects-444203', credentials=credentials)

        if odds_data[table].empty:
            odds_query = f"""
            SELECT * 
            FROM `capstone_data.{table}_predictions`
            WHERE DATE(Date_Updated) = DATE_SUB(CURRENT_DATE('America/Los_Angeles'), INTERVAL 1 DAY)
            """
            odds_data[table] = pandas_gbq.read_gbq(odds_query, project_id='miscellaneous-projects-444203', credentials=credentials)

        odds_data[table]['Player'] = odds_data[table]['Player'].apply(clean_player_name)

    return odds_data
@st.cache_data
def pull_stats(odds_data):
    
    season = dt.date.today().year if dt.date.today().month >= 10 else dt.date.today().year - 1
    players = set()
    for table in odds_data:
        for player in odds_data[table]['Player']:
            players.add(player)

    query = f"""
    with player_data as(
        select 
            player,
            team_name,
            matchup,
            pp.game_date,
            pp.min,
            pp.fgm,
            pp.fga,
            pp.fg_pct,
            `3pm`,
            pp.fg3a,
            pp.fg3_pct,
            pp.ftm,
            pp.fta,
            pp.ft_pct,
            pp.reb,
            pp.ast,
            pp.pts,
            pp.plus_minus,
            row_number() over(partition by player order by pp.game_date desc) rn
        from `capstone_data.player_prediction_data_partitioned` pp
        inner join `capstone_data.team_prediction_data_partitioned` tp
            on pp.game_id = tp.game_id
        where pp.season_start_year = {season} and tp.season_start_year = {season} and lower(player) in ({','.join([f'"{player}"' for player in players])})
        )
        select * except(rn)
        from player_data
        where rn <= 3
        """

    try:
        credentials = service_account.Credentials.from_service_account_file('/home/aportra99/scraping_key.json')
        local = False
    except FileNotFoundError:
        local = True
        credentials = None
    
    player_data = pandas_gbq.read_gbq(query, project_id='miscellaneous-projects-444203', credentials=credentials)

    return player_data

@st.cache_data
def pull_images():
    query = "SELECT * FROM `capstone_data.player_images`"
    player_images = pandas_gbq.read_gbq(query, project_id='miscellaneous-projects-444203')

    player_images['images'] = player_images['images'].fillna('')
    player_images['images'] = player_images['images'].apply(lambda x: x.replace("h=80", "h=254").replace("w=110", "w=350"))
    player_images['players'] = player_images['players'].apply(clean_player_name)
    player_images["players_lower"] = player_images["players"].str.lower()
    return player_images

@st.cache_data
def get_available_players(category, odds_data):
    available_players = set()
    category_key = category.lower().replace(" ", "_")

    if category == "All":
        for table in odds_data:
            available_players.update(odds_data[table]["Player"].unique())
    else:
        if category_key in odds_data:
            available_players = odds_data[category_key]["Player"].unique()

    return sorted(available_players)

@st.cache_data
def get_player_odds(player_selected, category, odds_data):
    """Fetch betting odds + model recommendations for a selected player"""
    category_map = {
        "Points": "pts",
        "Rebounds": "reb",
        "Assists": "ast",
        "Threes Made": "3pm"
    }
    
    category_key = category_map.get(category, "pts")  # Default to "pts" if category is "All"
    player_odds = []

    if category == "All":
        # Fetch all categories
        for (table_name, df), cat in zip(odds_data.items(), ["pts", "reb", "ast", "3pm"]):
            if player_selected in df["Player"].values:
                available_columns = [col for col in [
                    f"{table_name}", "Over", "Under",
                    f"recommendation_{cat}_linear_model",
                    f"recommendation_{cat}_lightgbm"
                ] if col in df.columns]
                
                temp_df = df[df["Player"] == player_selected][available_columns].copy()
                player_odds.append((table_name, temp_df))
    else:
        # Only fetch the selected category's odds
        if category_key in odds_data and player_selected in odds_data[category_key]["Player"].values:
            df = odds_data[category_key]

            available_columns = [col for col in [
                f"{category_key}", "Over", "Under",
                f"recommendation_{category_key}_linear_model",
                f"recommendation_{category_key}_lightgbm"
            ] if col in df.columns]
            
            temp_df = df[df["Player"] == player_selected][available_columns].copy()
            player_odds.append((category_key, temp_df))  #Only return selected category

    return player_odds

def make_dashboard(player_images, odds_data,player_data):
    today = dt.date.today()
    st.title(f"NBA Player Betting Odds {today}")

    if "selected_player" not in st.session_state:
        st.session_state["selected_player"] = ""

    if "selected_category" not in st.session_state:
        st.session_state["selected_category"] = "All"

    # Category filter should reset selected player if filtering "All Players"
    category = st.selectbox("Select a category (affects All Players view only):", ["All", "Points", "Assists", "Rebounds", "Threes Made"])
    
    if category != st.session_state["selected_category"]:
        st.session_state["selected_category"] = category
        
        # Reset selected player when filtering in "All Players" mode
        if not st.session_state["selected_player"]:
            st.session_state["selected_player"] = ""
        
        st.rerun()

    available_players = get_available_players(category, odds_data)

    # Ensure selecting the blank option resets the selected player
    player_options = [""] + [p.title() for p in available_players]
    default_index = player_options.index(st.session_state["selected_player"].title()) if st.session_state["selected_player"] in available_players else 0

    player_selected = st.selectbox("Search or Select a Player:", player_options, index=default_index)

    # If blank is selected, reset to "All Players" mode
    if player_selected == "":
        if st.session_state["selected_player"] != "":
            st.session_state["selected_player"] = ""
            st.rerun()
    else:
        player_selected_lower = player_selected.lower()
        if st.session_state["selected_player"] != player_selected_lower:
            st.session_state["selected_player"] = player_selected_lower
            st.rerun()

    if st.session_state["selected_player"]:
        # Selected Player's Page (Shows All Categories)
        player_row = player_images.loc[player_images["players_lower"] == st.session_state["selected_player"]]
        selected_image = player_row["images"].values[0] if not player_row.empty else None

        col1, col2 = st.columns([2, 3])

        with col1:
            if selected_image:
                st.image(selected_image, width=300)
            

        # Always show all categories for the selected player
        st.subheader(f"Betting Odds for {st.session_state['selected_player'].title()}")
        player_odds = get_player_odds(st.session_state["selected_player"], "All", odds_data)

        if player_odds:
            for table_name, odds in player_odds:
                st.markdown(f"**{table_name.capitalize()} Odds**")
                st.dataframe(odds.style.hide(axis="index"), use_container_width=True)
        else:
            st.write("No odds available for this player today.")

    else:
        # Category filter applies only to "All Players"
        if category == 'All':
            st.subheader(f"All Players")
        else:
            st.subheader(f"All Players: {category}")
        for player in available_players:
            player_lower = player.lower()
            player_row = player_images.loc[player_images["players_lower"] == player_lower]
            player_image = player_row["images"].values[0] if not player_row.empty else None

            with st.container():
                col1, col2 = st.columns([1, 3])

                with col1:
                    if player_image:
                        st.image(player_image, width=200)

                with col2:
                    if st.button(f"**{player.title()}**", key=f"btn_{player}"):
                        st.session_state["selected_player"] = player_lower
                        st.rerun()

                st.markdown("<br><hr><br>", unsafe_allow_html=True)



# Run the dashboard
images = pull_images()
odds_data = pull_odds()
player_data = pull_stats(odds_data)
make_dashboard(images, odds_data,player_data)
