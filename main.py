import pandas as pd
import time
from datetime import datetime as date
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from nba_api.stats.endpoints import LeagueGameFinder, BoxScoreTraditionalV2
from nba_api.stats.library.parameters import SeasonAll
import os


def get_past_games(season):
    game_finder = LeagueGameFinder(season_nullable=season,season_type_nullable='Regular Season')
    game = game_finder.get_data_frames()[0]
    return game[['GAME_ID', 'GAME_DATE', 'MATCHUP']]

def get_box_score(game_id):
    try:
        box_score = BoxScoreTraditionalV2(game_id=game_id)
    except:
        return game_id
    return box_score.player_stats.get_data_frame()

def send_email(subject,body):
    sender_email = os.getenv('SERVER_EMAIL')
    receiver_email = os.getenv('EMAIL_USERNAME')
    password = os.getenv('EMAIL_PASSWORD')
    print("Sender email:", sender_email)
    print("Receiver email:", receiver_email)
    print("Password set:", password)
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject
    
    msg.attach(MIMEText(body,'plain'))

    try:
        server = smtplib.SMTP('smtp.gmail.com',587)
        server.starttls()
        server.login(sender_email,password)
        server.send_message(msg)
    except Exception as e:
        print(f"failed due to send email: {e}")
    finally:
        server.quit()

#Makes it so we are not connecting to driver on import