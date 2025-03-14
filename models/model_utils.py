from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from datetime import datetime as date
from datetime import timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
from selenium.webdriver import Remote
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

import os
import smtplib
import regex as re
import time
import pandas as pd
import signal
import psutil
import chromedriver_autoinstaller


def establish_driver(local=False):
    """Establishes a Selenium WebDriver for Chrome.

    Args:
        local (bool): If True, runs WebDriver locally. Otherwise, uses a remote setup.

    Returns:
        webdriver.Chrome: A configured instance of the Chrome WebDriver.
    """
    options = Options()
    options.add_argument("--headless")  # Run without UI
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")  # Prevents GPU-related crashes
    
    if not local:
        chrome_path = chromedriver_autoinstaller.install()
        service = Service(chrome_path)
        options.binary_location = "/usr/bin/google-chrome-stable"
        driver = webdriver.Chrome(service=service, options=options)
    else:
        # Use remote or automatically installed Chromedriver
        chrome_path = chromedriver_autoinstaller.install()
        print(chrome_path)
        service = Service(chrome_path)
        driver = webdriver.Chrome(service=service, options=options)

    driver.set_window_size(2560, 1440)
    return driver

def terminate_firefox_processes(): #Used for memory efficieny
    """
    Forcefully terminates all lingering Firefox & Geckodriver processes.
    """
    for process in psutil.process_iter(attrs=['pid', 'name']): #gathers all the current active signals
        try:
            if process.info['name'].lower() in ('firefox-bin', 'geckodriver','firefox.exe'): 
                os.kill(process.info['pid'], signal.SIGTERM)  #sends termination signal
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass

#Select all option only works when at least half screen due to blockage of the all option when not in headerless option

def select_all_option(driver):
    try:
        # Click the dropdown
        
        dropdown = WebDriverWait(driver,10).until(
            EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div[2]/div[2]/div[3]/section[2]/div/div[2]/div[2]/div[1]/div[3]/div/label"))
        )
        driver.execute_script("arguments[0].click();", dropdown)
        # Click the "All" option
        all_option = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div[2]/div[2]/div[3]/section[2]/div/div[2]/div[2]/div[1]/div[3]/div/label/div/select/option[1]"))
        )
        all_option.click()

        print("Successfully selected the 'All' option.")
    except Exception as e:
        print(f"Error selecting the 'All' option: {e}")

def send_email(subject,body):
    try:
        load_dotenv('/home/aportra99/Capstone/.env')
        print('loaded the .env')
    except:
        print('could not load .env')
    sender_email = os.getenv('SERVER_EMAIL')
    receiver_email = os.getenv('EMAIL_USERNAME')
    password = os.getenv('EMAIL_PASSWORD')
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
if __name__ == "__main__":
    driver = establish_driver()
    select_all_option(driver)
