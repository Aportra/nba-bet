from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

import os
import smtplib
import signal
import requests
import chromedriver_autoinstaller


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



def establish_requests(url,params=False):
    # Headers to mimic a real browser request (prevents bot blocking)

    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:90.0) Gecko/20100101 Firefox/90.0",
    ]

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
       response = requests.get(url, headers=headers,params=params) 

    print(response.status_code)
    return response


#Makes it so we are not connecting to driver on import
if __name__ == "__main__":
    driver = establish_driver()
    select_all_option(driver)
