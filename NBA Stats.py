#Imports
from bs4 import BeautifulSoup
import pandas as pd
from random import randint
from requests import get
from time import sleep
from warnings import warn

# Variables
DATA_DIR = 'C:/Users/Chris/Desktop'
START_YEAR = 2019
END_YEAR = 2020
STAT_TYPES = ['_totals', '_per_game', '_per_minute', '_per_poss', '_advanced']

# Functions
def get_html(year, stat_type):
    # Get website
    url = ('https://www.basketball-reference.com/leagues/NBA_{}{}.html'.format(year, stat_type))
    response = get(url, timeout=5)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Warning for non-200 status codes
    if response.status_code != 200:
        warn('Error: Status code {}'.format(response.status_code))
    return soup

# Get variable headers for the statistics from the page
def get_header(soup):
    header = [i.text for i in soup.find_all('tr')[0].find_all('th')]
    header.append('Year')
    header = header[1:] # Drop rank column
    header = [item.replace('%', '_percent').replace('/', '_').lower() for item in header]
    return header

# Get the player statistics for the stat type
def get_stats(soup, headers):
    stats = []
    rows = soup.find_all('tr', class_=['full_table', 'italic_text partial_table'])
    for i in range(len(rows)):
        stats.append([j.text for j in rows[i].find_all('td')]) # Append each row
        stats[i].append(year)
    stats = pd.DataFrame(stats, columns=headers)
    return stats

# Clean up and format dataframe
def format_dataframe(player_stats, stat_type):
    player_stats = player_stats.copy()

    # Drop empty columns
    drop_cols = []
    for col in player_stats.columns:
        if (len(col) <= 1) and (col != 'g'):
            drop_cols.append(col)
    player_stats = player_stats.drop(columns=drop_cols)

    # Convert numerical columns to numeric
    cols = [i for i in player_stats.columns if i not in ['player', 'pos', 'tm']]
    for col in cols:
        player_stats[col] = pd.to_numeric(player_stats[col])

    # Fill blanks
    player_stats = player_stats.fillna(0)

    # Save to csv
    player_stats.to_csv('{}/nba_player_stats{}.csv'.format(DATA_DIR, stat_type), index=False)

# Main loop to get player statistics for all stat types for years of interest
for stat_type in STAT_TYPES:
    player_stats = pd.DataFrame()
    # Loop through the years
    for year in range(START_YEAR, END_YEAR + 1):
        # Slow down the web scrape
        sleep(randint(1, 4))

        # Get website
        html_soup = get_html(year, stat_type)

        # Get header
        if year == START_YEAR:
            headers = get_header(html_soup)

        # Get player stats
        player_stats = player_stats.append(get_stats(html_soup, headers), ignore_index=True)

        print('{} completed for {} table'.format(year, stat_type))

    # Format the datatframe
    format_dataframe(player_stats, stat_type)

# Read all stats into their own variables
totals = pd.read_csv('{}/nba_player_stats_totals.csv'.format(DATA_DIR))
per_game = pd.read_csv('{}/nba_player_stats_per_game.csv'.format(DATA_DIR))
per_min = pd.read_csv('{}/nba_player_stats_per_minute.csv'.format(DATA_DIR))
per_poss = pd.read_csv('{}/nba_player_stats_per_poss.csv'.format(DATA_DIR))
advanced = pd.read_csv('{}/nba_player_stats_advanced.csv'.format(DATA_DIR))

# Join all 5 stats tables into 1 massive table
# Merge per game stats into total stats table
all_data = totals.merge(per_game, on=['player', 'pos', 'age', 'tm', 'g', 'gs', 'fg_percent', '3p_percent',
                                        '2p_percent', 'efg_percent', 'ft_percent', 'year'], how='inner')
all_data.columns = all_data.columns.str.replace('_x', '').str.replace('_y', '_pg')

# Merge in per 36 minutes stats
all_data = all_data.merge(per_min, on=['player', 'pos', 'age', 'tm', 'g', 'gs', 'fg_percent', '3p_percent',
                                       '2p_percent', 'ft_percent', 'year'], how='inner')
all_data.columns = all_data.columns.str.replace('_x', '').str.replace('_y', '_p36m')

# Merge in per 100 possessions stats
all_data = all_data.merge(per_poss, on=['player', 'pos', 'age', 'tm', 'g', 'gs', 'fg_percent', '3p_percent',
                                        '2p_percent', 'ft_percent', 'year'], how='inner')
all_data.columns = all_data.columns.str.replace('_x', '').str.replace('_y', '_p100p')

# Merge in advanced stats
all_data = all_data.merge(advanced, on=['player', 'pos', 'age', 'tm', 'g', 'mp', 'year'], how='inner')
all_data.columns = all_data.columns.str.replace('_x', '_tot').str.replace('_y', '_adv')

# Save csv
all_data.to_csv('{}/all_nba_player_stats.csv'.format(DATA_DIR), index=False)
