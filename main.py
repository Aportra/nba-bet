import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from subprocess import getoutput
from bs4 import BeautifulSoup
import regex as re
import time
from datetime import datetime as date
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os



def establish_driver():
    from selenium.webdriver.firefox.options import Options
    from selenium.webdriver.firefox.service import Service
    from selenium import webdriver

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    # Explicit paths
    geckodriver_path = "/snap/bin/geckodriver"
    firefox_binary_path = "/snap/bin/firefox"

    options.binary_location = firefox_binary_path

    print(f"Using Geckodriver at: {geckodriver_path}")
    print(f"Using Firefox binary at: {firefox_binary_path}")

    service = Service(geckodriver_path)
    driver = webdriver.Firefox(service=service, options=options)

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


def process_page(page,game_id,game_date,home,away,driver):
    driver.get(page)
    
    driver.set_page_load_timeout(120)
    driver.implicitly_wait(10)

    ps = driver.page_source
    soup = BeautifulSoup(ps, 'html5lib')
    
    # Find all divs containing the data tables
    tables = soup.find_all('table', id=lambda x: x and "game-basic" in x, class_='sortable stats_table now_sortable')
    last_updated = date.today()
    df_data = []
    # Check if tables exist
    if tables:
        for table_index, table in enumerate(tables):
            # Extract the table rows
            rows = table.find_all('tr')
            if table_index == 1:
                t1 = home
                t2 = away
            else:
                t1 = away
                t2 = home
            # Get the header row (if it exists)
            thead = table.find('thead')
            if thead:
                header_row = thead.find_all('tr')[1]  # Second <tr> in <thead>
                headers = [th.get_text(strip=True) for th in header_row.find_all('th')]
                headers.extend(['team','game_id','game_date','matchup','url','last_updated'])
            else:
                headers = []  # Handle case if no headers are present

            tfoot = table.find('tfoot')
            tfoot_rows = []
            if tfoot:
                tfoot_rows = tfoot.find_all('tr')
            # Get the data rows
            data = []
            
            for row in rows:  # Skip the header row
                if row in tfoot_rows:
                    print(f"Skipping row in tfoot: {row.get_text(strip=True)}")
                    continue
                if 'thead' in row.get('class', []):
                    continue

                reason_cell = row.find('td', {'colspan': True})
                if reason_cell:
                    # Log the skipped row for debugging
                    print(f"Skipping row due to colspan: {reason_cell.get_text(strip=True)}")
                    continue
                
                first_cell = row.find('th')
                first_value = first_cell.get_text(strip=True) if first_cell else None
                if first_value in ["Starters", "Reserves", "Team Totals", ""] or not first_value:
                    print(f"Skipping label or invalid row: {first_value}")
                    continue
                # Extract all <td> elements from the row
                cols = row.find_all('td')
                row_data = [col.get_text(strip=True) for col in cols]
                row_data.extend([t1, game_id, game_date, t2, page, last_updated])
                # Prepend the first <th> value to the row
                if first_value:
                    row_data.insert(0, first_value)

                # Check row length
                if len(row_data) < len(headers):  # Pad short rows
                    print(f"Padding row: {row_data}")
                    row_data.extend([None] * (len(headers) - len(row_data)))
                elif len(row_data) > len(headers):  # Trim long rows
                    print(f"Trimming row: {row_data}")
                    row_data = row_data[:len(headers)]
                data.append(row_data)
    
            
            # Create a DataFrame for this table
            if headers and data:
                
                df = pd.DataFrame(data, columns=headers)
            else:
                df = pd.DataFrame(data)  
            
            df_data.append(df)
        
        final_df = pd.concat(df_data,ignore_index=True)
            # Append the DataFrame to the appropriate team entries in the dictionary
        return final_df
    else:
        print(f'Could not process: {page}')
        return game_id,game_date,home,away

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
if __name__ == "__main__":
    driver = establish_driver()
    select_all_option(driver)