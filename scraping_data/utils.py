"""Selenium Utility Module for Web Scraping and Automation."""

import yaml
from io import StringIO
import io
import os
import smtplib
import pandas as pd
import requests
import psycopg2
from datetime import datetime as dt
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from bs4 import BeautifulSoup
from tqdm import tqdm


def establish_requests(url,params=False):
    # Headers to mimic a real browser request (prevents bot blocking)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.nba.com/stats/",
        "Origin": "https://www.nba.com"
    }
    if not params:
        # Send request
        response = requests.get(url, headers=headers)
    else:
        response = requests.get(url, headers=headers, params=params)

    print(response.status_code)
    return response


def send_email(subject, body):
    """Sends an email notification.

    Args:
        subject (str): The email subject.
        body (str): The email body content.
    """
    try:
        load_dotenv('/home/aportra99/Capstone/.env')
        print('Loaded the .env file.')
    except FileNotFoundError:
        print('Could not load .env file.')

    sender_email = os.getenv('SERVER_EMAIL')
    receiver_email = os.getenv('EMAIL_USERNAME')
    password = os.getenv('EMAIL_PASSWORD')

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, password)
        server.send_message(msg)
    except Exception as e:
        print(f"Failed to send email: {e}")
    finally:
        server.quit()


def convert_date(date_str):
    """Converts a date string to a datetime object with the correct year.

    Args:
        date_str (str): The date string in the format "Weekday, Month Day" (e.g., "Tue, Oct 10").

    Returns:
        datetime.date or None: The converted date object or None if invalid.
    """
    try:
        date_obj = dt.strptime(date_str, "%a, %b %d")
        # date_obj = dt.strptime(date_str, "%a, %m/%d")

        # Assign correct year based on NBA season start (October)
        assumed_year = 2025 if date_obj.month >= 10 else 2026
        date_obj = date_obj.replace(year=assumed_year)

        return date_obj.date()
    except ValueError as e:
        print(f"Skipping invalid date: {date_str} - {e}")
        return None


def upload_data(data, table_name):
    df = data.copy()
    print('uploading', table_name)
    with open('/home/aportra99/nba-bet/scraping_data/config.yaml', 'r') as file:
        config_data = yaml.safe_load(file)
    con = (psycopg2.connect(host=config_data['host'],
                            user=config_data['user'],
                            password=config_data['password'],
                            database=config_data['database']))
    cursor = con.cursor()

    print('connection succeeded')
    df.columns = df.columns.str.replace('%', '_pct')
    df.columns = df.columns.str.replace('3', 'three_')
    if 'to' in data.columns:
        df.columns = df.columns.str.replace('to', 'turnovers')
    cols = ','.join([f'{i}' for i in df.columns])
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cursor.copy_expert(
    f"""
    copy "{table_name}"
    ({cols})
    from stdin with (format csv)
        """, buffer
    )

    con.commit()


class psql:
    def __init__(self):
        os.chdir('..')
        base_dir = os.path.dirname(os.path.abspath(__file__))
        print(base_dir)
        config = os.path.join(base_dir, 'config.yaml')
        print(config)
        with open('config.yaml', 'r') as file:
            config = yaml.safe_load(file)

        try:
            print("database connection successful")
            self.connect = (psycopg2.connect(database=config['database'],
                                             user=config['user'],
                                             password=config['password'],
                                             host=config['host']))
        except psycopg2.operationalerror:
            print("database connection failed")
            return None

    def create_table(self, table, table_name):
        cur = self.connect.cursor()
        if cur is None:
            return

        dtype_converter = {
            "int64": "bigint",
            "int32": "integer",
            "float64": "double precision",
            "float32": "real",
            "bool": "boolean",
            "boolean": "boolean",
            "object": "text",
            "string": "text",
            "datetime64[ns]": "timestamp",
            "datetime64[ns, utc]": "timestamptz",
        }
        d = ([(col, dtype_converter[str(table[col].dtype)])
             for col in list(table.columns)])

        cols = ',\n'.join([f'\t{col} {typ}' for col, typ in d])

        query = f"""
        create table {table_name}(
        {cols}
        );
        """
        cur.execute(query)
        self.connect.commit()

        cur.close()

    def upload_data(self, table, table_name):
        cur = self.connect.cursor()
        if cur is None:
            return

        buffer = StringIO()

        table.to_csv(buffer, index=False, header=False)

        buffer.seek(0)

        cols = ',\n'.join([f'\t{col}' for col in table.columns])

        cur.copy_expert(
                f"""copy {table_name}
                ({cols})
                from stdin with (format csv)""", buffer)

        self.connect.commit()

    def query(self, query):
        cur = self.connect.cursor()

        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        data = pd.DataFrame(cur.fetchall(), columns=columns)

        return data

    def close(self):

        self.connect.close()


if __name__ == "__main__":
    establish_requests


