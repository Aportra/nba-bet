"""Selenium Utility Module for Web Scraping and Automation."""

import os
import time
import random
import smtplib
import signal
import psutil
import pandas as pd
import chromedriver_autoinstaller
import requests
from datetime import datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from tqdm import tqdm


def establish_driver(local=False):
    """Establishes a Selenium WebDriver for Chrome.

    Args:
        local (bool): If True, runs WebDriver locally. Otherwise, uses a remote setup.

    Returns:
        webdriver.Chrome: A configured instance of the Chrome WebDriver.
    """
    chrome_options = webdriver.ChromeOptions()  # Correct usage of ChromeOptions
    chrome_options.add_argument("--headless")  # Run without UI (remove if you need the UI)
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")  # Prevents GPU-related crashes
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36")

    if not local:
        # If not local, install chromedriver and set up the service for remote use
        chrome_path = chromedriver_autoinstaller.install()
        service = Service(chrome_path)
        chrome_options.binary_location = "/usr/bin/google-chrome-stable"  # Path to Chrome binary if using a custom installation
        driver = webdriver.Chrome(service=service, options=chrome_options)
    else:
        # If local, use the automatically installed chromedriver
        chrome_path = chromedriver_autoinstaller.install()
        service = Service(chrome_path)
        driver = webdriver.Chrome(service=service, options=chrome_options)
    
    # Set the window size for the Chrome browser (optional)
    driver.set_window_size(2560, 1440)

    return driver

def establish_requests(url):
    # Headers to mimic a real browser request (prevents bot blocking)

    USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:90.0) Gecko/20100101 Firefox/90.0",
        ]
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": "https://www.nba.com/",
        "Origin": "https://www.nba.com",
        "Accept": "application/json, text/plain, */*",
        "Host": "stats.nba.com",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9"
    }

    # Send request
    response = requests.get(url, headers=headers)
    
    return response


def terminate_firefox_processes():
    """Forcefully terminates lingering Firefox & Geckodriver processes."""
    for process in psutil.process_iter(attrs=["pid", "name"]):
        try:
            if process.info["name"].lower() in ("firefox-bin", "geckodriver", "firefox.exe"):
                os.kill(process.info["pid"], signal.SIGTERM)  # Terminate process
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass


def select_all_option(driver):
    """Selects the 'All' option in a dropdown menu on a webpage."""
    try:
        dropdown = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH, "/html/body/div[1]/div[2]/div[2]/div[3]/section[2]/div/div[2]/div[2]/div[1]/div[3]/div/label")
            )
        )
        driver.execute_script("arguments[0].click();", dropdown)

        all_option = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH, "/html/body/div[1]/div[2]/div[2]/div[3]/section[2]/div/div[2]/div[2]/div[1]/div[3]/div/label/div/select/option[1]")
            )
        )
        all_option.click()
        print("Successfully selected the 'All' option.")
    except Exception as e:
        print(f"Error selecting the 'All' option: {e}")

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

        # Assign correct year based on NBA season start (October)
        assumed_year = 2024 if date_obj.month >= 10 else 2025
        date_obj = date_obj.replace(year=assumed_year)

        return date_obj.date()
    except ValueError as e:
        print(f"Skipping invalid date: {date_str} - {e}")
        return None

if __name__ == "__main__":
    driver = establish_driver()
    select_all_option(driver)
