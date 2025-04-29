import streamlit as st
import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
from datetime import timedelta
import datetime as dt
import requests

st.set_page_config(
    page_title="NBA Pro Picks",
    page_icon="🏀",  # fallback icon
    layout="wide"
)

        
def smart_title(name):
    # Words to preserve as all-uppercase
    exceptions = {"iii", "ii", "iv"}
    
    return " ".join([
        word.upper() if word.lower() in exceptions else word.capitalize()
        for word in name.split()
    ])


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
def convert_minute(data):
    data = float(data)
    minutes = int(data)
    seconds = round((data - minutes) * 60)
    
    if seconds >= 60:
        minutes += 1
        seconds = 0

    return f"{minutes}:{seconds:02}"

@st.cache_data
def pull_odds():
    tables = ['points', 'rebounds', 'assists', 'threes_made']
    identifiers = ['pts','reb','ast','3pm']
    odds_data = {}


    credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])


    for table,cat in zip(tables,identifiers):
        odds_query = f"""
            SELECT DISTINCT *
            FROM `capstone_data.{cat}_classifications`
            WHERE DATE(Date_Updated) = Current_date('America/Los_Angeles')
            AND recommendation != 'No Bet Recommendation'
        """
        
        odds_data[table] = pandas_gbq.read_gbq(odds_query, project_id='miscellaneous-projects-444203', credentials=credentials)
        
        odds_data[table].rename(columns={'Date_Updated':'game_date'},inplace=True)
        odds_data[table]['game_date'] = odds_data[table]['game_date'].dt.date
        odds_data[table].drop_duplicates(subset = ['player','game_date'],inplace = True)

        odds_data[table]['player'] = odds_data[table]['player'].apply(clean_player_name)

    return odds_data, odds_data['points']['game_date'].values[0]

@st.cache_data
def pull_stats(odds_data):
    
    season = dt.date.today().year if dt.date.today().month >= 10 else dt.date.today().year - 1
    players = set()
    for table in odds_data:
        for player in odds_data[table]['player']:
            players.add(player)

    query = f"""
    WITH deduped_data AS (
        SELECT 
            player,
            pp.team,
            tp.team_name,
            matchup,
            pp.game_date,
            pp.min,
            pp.pts,
            pp.reb,
            pp.ast,
            pp.fgm,
            pp.fga,
            (pp.fg_pct*100) as `FG %`,
            `3pm`,
            pp.fg3a,
            (pp.fg3_pct*100) as `FG3 %`,
            pp.ftm,
            pp.fta,
            (pp.ft_pct*100) as `FT %`,
            pp.plus_minus,
            pp.game_id,
            tp.team_id,
            ROW_NUMBER() OVER (PARTITION BY player, pp.game_id ORDER BY pp.game_date DESC) AS rn
        FROM `capstone_data.player_prediction_data_partitioned` pp
        INNER JOIN `capstone_data.team_prediction_data_partitioned` tp
            ON pp.game_id = tp.game_id and pp.team = tp.team
        WHERE pp.season_start_year = {season}
          AND tp.season_start_year = {season}
          AND LOWER(player) IN ({','.join([f'"{player}"' for player in players])})
    ),
    latest_games AS (
        SELECT *
        FROM deduped_data
        WHERE rn = 1
    ),
    ranked_games AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY player ORDER BY game_date DESC) AS game_rank
        FROM latest_games
    )
    SELECT * EXCEPT(rn, game_rank, game_id)
    FROM ranked_games
    WHERE game_rank <= 3
    """

    try:
        credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
        local = False
    except FileNotFoundError:
        local = True
        credentials = None
    
    url = "https://stats.nba.com/stats/scoreboardv2"
    
    games_query = f""" 
    select team,opponent,home
    from `capstone_data.schedule`
    where date(GAME_DATE_EST) = Current_date("America/Los_Angeles")
    """

    games = pandas_gbq.read_gbq(games_query, project_id='miscellaneous-projects-444203', credentials=credentials)


    player_data = pandas_gbq.read_gbq(query, project_id='miscellaneous-projects-444203', credentials=credentials)

    player_data.rename(columns = {'team_name':'Team Name','game_date':'Game Date','plus_minus':'Plus Minus'},inplace=True)
    return player_data,games

@st.cache_data
def pull_images():
    try:
        credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
    except Exception as e:
        st.error("Could not load GCP credentials.")
        st.exception(e)
        credentials = None
    query = "SELECT * FROM `capstone_data.player_images`"
    player_images = pandas_gbq.read_gbq(query, project_id='miscellaneous-projects-444203',credentials=credentials)

    player_images['images'] = player_images['images'].fillna('')
    player_images['images'] = player_images['images'].apply(lambda x: x.replace("h=80", "h=254").replace("w=110", "w=350"))
    player_images['players'] = player_images['players'].apply(clean_player_name)
    player_images["players_lower"] = player_images["players"].str.lower()




    team_query = "SELECT * FROM `capstone_data.team_logos`"
    team_images = pandas_gbq.read_gbq(team_query, project_id='miscellaneous-projects-444203',credentials=credentials)

    team_images['images'] = team_images['images'].fillna('')


    return player_images,team_images

@st.cache_data
def get_available_players(category, odds_data):
    available_players = set()
    category_key = category.lower().replace(" ", "_")

    if category == "All":
        for table in odds_data:
            available_players.update(odds_data[table]["player"].unique())
    else:
        if category_key in odds_data:
            available_players = odds_data[category_key]["player"].unique()


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
            if player_selected in df["player"].values:
                available_columns = [col for col in [
                    f"{table_name}", "Over", "Under",
                    "recommendation"
                ] if col in df.columns]
                
                temp_df = df[df["player"] == player_selected][available_columns].copy()
                temp_df.rename(columns={'Over':'Under_1','Under':'Over'},inplace=True)
                temp_df.rename(columns={'Under_1':'Under'},inplace=True)
                player_odds.append((table_name, temp_df))
    else:
        # Only fetch the selected category's odds
        if category_key in odds_data and player_selected in odds_data[category_key]["player"].values:
            df = odds_data[category_key]

            available_columns = [col for col in [
                f"{category_key}", "Over", "Under",
                f"recommendation_{category_key}_linear_model",
                f"recommendation_{category_key}_lightgbm"
            ] if col in df.columns]
            
            temp_df = df[df["player"] == player_selected][available_columns].copy()
            player_odds.append((category_key, temp_df))  #Only return selected category

    
    return player_odds

def make_dashboard(player_images,team_images, odds_data,player_data,games):

    main_time = dt.date.today()
    side_col,main_col = st.columns([1,10])

    with side_col:
        nba_logo = team_images[team_images['teams']=='nba']['images'].values[0]
        st.image(nba_logo,width=120)

    with main_col:
        st.title(f"NBA Pro Picks")
        st.write(f'{main_time}')
        
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
    player_options = [""] + [smart_title(p) for p in available_players]
    default_index = player_options.index(smart_title(st.session_state["selected_player"])) if st.session_state["selected_player"] in available_players else 0

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

        team = player_data[player_data['player'].apply(lambda x: x.lower()) == st.session_state['selected_player']]['team'].values[0]
        team_name = player_data[player_data['player'].apply(lambda x: x.lower()) == st.session_state['selected_player']]['Team Name'].values[0]
        team_selected_image = team_images[team_images['teams'] == team]['images'].values[0]
       
        if games[games['team']==team]['home'].values[0] == 1:
            divider = 'vs'
            opponent = games[games['team']==team]['opponent'].values[0]
        else:
            divider = '@'
            opponent = games[games['team']==team]['opponent'].values[0]

        col1, col2, = st.columns(2)

        with col1:
            if selected_image:

                st.image(team_selected_image,width=77)
                st.image(selected_image, width=320)
                st.header(f"{smart_title(st.session_state['selected_player'])} | {team_name}")
                st.write(f"Next Game: {team} {divider} {opponent}")
                

        # Always show all categories for the selected player
        player_odds = get_player_odds(st.session_state["selected_player"], "All", odds_data)
        

        if player_odds:
            player_data.columns = [col.replace("_","").title() for col in player_data.columns]
            filtered_player_df = player_data[player_data['Player'].apply(lambda x:x.lower())==st.session_state['selected_player']].copy()
            filtered_player_df.drop(['Player','Team','Team Name','Teamid'],axis = 1,inplace=True)
            filtered_player_df['Min'] = filtered_player_df['Min'].apply(lambda x: convert_minute(x))
            st.dataframe(filtered_player_df,hide_index=True)
            for table_name, odds in player_odds:
                st.markdown(f"**{table_name.replace('_',' ').title()} Odds**")
                odds.columns = [col.replace("_", " ").title() for col in odds.columns]
                st.dataframe(odds,hide_index=True, use_container_width=True)
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
                    if st.button(f"**{smart_title(player)}**", key=f"btn_{player}"):
                        st.session_state["selected_player"] = player_lower
                        st.rerun()

                st.markdown("<br><hr><br>", unsafe_allow_html=True)



# Run the dashboard
images,team_images = pull_images()
odds_data,date = pull_odds()
player_data,games = pull_stats(odds_data)
make_dashboard(images,team_images, odds_data,player_data,games)
