import pandas as pd
from selenium import webdriver
import pandas_gbq
from bs4 import BeautifulSoup

driver = webdriver.Firefox()

urls = {'2021-2022_Team_Ratings':'https://www.basketball-reference.com/leagues/NBA_2022_ratings.html',
        '2022-2023_Team_Ratings':'https://www.basketball-reference.com/leagues/NBA_2023_ratings.html',
        '2023-2024_Team_Ratings':'https://www.basketball-reference.com/leagues/NBA_2024_ratings.html'}





for url in urls:
    driver.get(urls[url])

    page_source = driver.page_source

    soup = BeautifulSoup(page_source,'html5lib')

    table = soup.find_all('table',class_ = 'sortable stats_table now_sortable')

    print(table)

