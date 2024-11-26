#Imports
from bs4 import BeautifulSoup
import pandas as pd
from random import randint
from requests import get
from time import sleep
from warnings import warn

# Variables
DATA_DIR = '/Users/chrislee/PyCharmProjects/Web-Scrape-Basketball-Reference'
STAT_TYPES = ['_totals', '_per_game', '_per_minute', '_per_poss', '_advanced']

url = 'https://www.basketball-reference.com/leagues/NBA_2023_advanced.html'
response = get(url, timeout=5)
soup = BeautifulSoup(response.text, 'html.parser')

# Warning for non-200 status codes
if response.status_code != 200:
    warn('Error: Status code {}'.format(response.status_code))

header = [i.text for i in soup.find_all('tr')[0].find_all('th')]
header.append('Year')
header = header[1:]  # Drop rank column
header = [item.replace('%', '_percent').replace('/', '_').lower() for item in header]
# print(header)
# num_cols = len(header)
# print(num_cols)

rows = soup.find_all('tbody')[0].find_all('tr')
for i in range(len(rows)):
    player_id = [a['href'] for a in rows[i].find_all('td')[0].find_all('a', href=True) if a.text]
    if len(player_id) != 0:
        print(player_id[0][11:][:-5])
    else:
        print('league_average')
    # print([j.text for j in rows[i] if j.text != ' '])
