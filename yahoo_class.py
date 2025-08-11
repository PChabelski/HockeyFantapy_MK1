import yfpy
from bs4 import BeautifulSoup, Comment
import requests
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np
import os
import time
from bs4 import BeautifulSoup, Comment
import requests
from urllib.request import urlopen

class YahooInstance:
    """
    Generic engine for running the code.
    """

    def __init__(self, control_file, current_directory, year):

        self.control_file       = control_file
        self.year               = year
        self.year_plus_one      = year + 1
        self.league_id          = str(self.control_file['Years'][str(year)]['league_id'])
        self.game_id            = int(self.control_file['Years'][str(year)]['game_id'])
        self.current_directory  = current_directory
        print(f'Initializing instance for Year {self.year} | League ID {self.league_id} | Game ID {self.game_id}')
        self.query = yfpy.YahooFantasySportsQuery(
            auth_dir='C:/Users/16472/PycharmProjects/Hockey_FantaPy/private',
            league_id=str(self.league_id),
            game_id=str(self.game_id),
            game_code='nhl',
            all_output_as_json_str=False
        )

    def METADATA_PARSE_SCHEDULE(self):
        """
        Parses NHL game schedules from the Hockey-Reference website for a given year.
        The method retrieves schedule data, processes it into a structured format, and saves it as a CSV file.
        """

        # Log the start of the parsing process
        print(f'>>>> [Rundate: {time.ctime()}] Parsing schedules from Hockey-Reference.com for {self.year}')

        # Construct the URL for the NHL games page for the given year
        games_page = f'https://www.hockey-reference.com/leagues/NHL_{self.year_plus_one}_games.html'

        # Fetch and parse the page content using BeautifulSoup
        page_content = urlopen(games_page).read()
        soup = BeautifulSoup(page_content, 'html.parser')

        # Extract schedule data from the HTML content
        items = soup.find(id="all_games").find_all(class_="left")
        schedule_data = []

        # Iterate over the extracted items in steps of 5 to process game details
        for i in range(5, len(items), 5):
            try:
                # Extract the game date from the HTML element
                date_str = items[i].find("a").getText()
            except:
                # Handle cases where the date is not wrapped in an <a> tag
                date_str = datetime.strptime(items[i].getText(), "%Y-%m-%d")

            # Convert the date string to a datetime object
            date = datetime.strptime(date_str, "%Y-%m-%d")

            # Extract the names of the away and home teams
            teamA = items[i + 2].find("a").getText()
            teamB = items[i + 3].find("a").getText()

            # Append the extracted data to the schedule_data list
            schedule_data.append(
                {'date': date.strftime("%Y-%m-%d"), 'away': teamA, 'home': teamB})

        # Log the completion of data extraction
        print(f'>>>> [Rundate: {time.ctime()}] Got all the home/away data')

        # Convert the schedule data into a Pandas DataFrame
        df_sched = pd.DataFrame(schedule_data)

        # Calculate cumulative game counts for each team as home and away
        df_sched['home_count'] = df_sched.groupby('home').cumcount() + 1
        df_sched['away_count'] = df_sched.groupby('away').cumcount() + 1
        # Extract game links from the HTML content
        list_of_games = [
            a['href'][11:-5] for a in soup.find_all('a', href=True)
            if a['href'].startswith('/boxscores/') and len(a['href'][11:-5]) > 11
        ]
        print(f'>>>> [Rundate: {time.ctime()}] Got list of boxscore codes')

        # Map the URLCODE to the schedule DataFrame based on the game date
        df_sched['urlcode'] = df_sched['date'].map(
            lambda d: next(
                (g for g in list_of_games if datetime.strptime(g[:8], '%Y%m%d').strftime('%Y-%m-%d') == d), None)
        )

        # Log the completion of URLCODE mapping
        print(f'>>>> [Rundate: {time.ctime()}] URLCODE added via mapper')


        # adding in the week number metadata to this file - it's all the same info
        game_weeks = self.query.get_game_weeks_by_game_id(self.game_id)
        df_sched['week_number'] = np.nan

        for i in range(0, len(game_weeks)):
            week_number = game_weeks[i].week
            start = pd.to_datetime(game_weeks[i].start)
            end = pd.to_datetime(game_weeks[i].end)
            df_sched.loc[
                (df_sched['date'] >= start.strftime('%Y-%m-%d')) & (df_sched['date'] <= end.strftime('%Y-%m-%d')),
                'week_number'] = int(week_number)



        # Save the schedule DataFrame to a CSV file
        df_sched.to_csv(f'{self.current_directory}/NHL_Schedules/{self.year}_NHL_Schedule.csv', index=False)

        # Log the successful completion of the schedule parsing process
        print(f'--> [Rundate: {time.ctime()}] {self.year} schedule successfully parsed')

    def METADATA_YAHOO_TEAMS(self):

        print(f'>>>> [Rundate: {time.ctime()}] Parsing team info for {self.year}')
        if self.year == 2012:
            print(f'>>>> [Rundate: {time.ctime()}] YFPY has an issue with the json decode for team metadata in year 2012...')
            print(f'>>>> [Rundate: {time.ctime()}] ...so we manually created the file and will skip this year')
            return

        teams_metadata = self.query.get_league_teams()
        df_teams = pd.DataFrame(columns=['Season', 'Team_Name', 'Team_Key', 'GM_Name', 'Division', 'Photo_URL'])
        for i in range(0, len(teams_metadata)):
            team_data = teams_metadata[i]
            team_name = team_data.name.decode('utf-8')
            team_key = team_data.team_key
            division = team_data.division_id
            if team_name == "Vintage'tingle'Boar":
                print(f'>>>> [Rundate: {time.ctime()}] Teams metadata conversion: Jordan to Tingle')
                nickname = 'Tingle'
            elif (team_name == "The Nerve"):
                print(f'>>>> [Rundate: {time.ctime()}] Teams metadata conversion: The Nerve to Ira')
                nickname = 'Ira'
            elif (team_name == "#G") | (team_name == "Grampa Jarzabek"):
                print(f'>>>> [Rundate: {time.ctime()}] Teams metadata conversion: #G and/or Grampa Jarzabek to A')
                nickname = 'A'
            else:
                nickname = team_data.managers[0].nickname
            if nickname == 'Doctor Kocktapus':
                nickname = 'Peter'
            if nickname == 't':
                nickname = 'Taylor'
            if nickname == 'Thomson McKnight':
                nickname = 'Thomson'
            if nickname == 'garrett':
                nickname = 'Garrett'
            if nickname == 'george':
                nickname = 'George'
            if nickname == 'Master':
                nickname = 'Yusko'
            if nickname == '--hidden--' and team_name == 'the dusters':
                nickname = 'Nick'
            if nickname == '--hidden--' and team_name == 'Unstopoulos':
                nickname = 'George'
            if nickname == '--hidden--' and team_name == "garrett's Team":
                nickname = 'George'
            if nickname == '--hidden--' and team_name == "Josh's Cool Team":
                nickname = 'Josh'
            if nickname == '--hidden--' and team_name == "Kessel/Trudeau 2015":
                nickname = 'Thomson'
            if nickname == '--hidden--' and team_name == "The T-BAGS":
                nickname = 'Taylor'
            if nickname == 'J' and team_name == "Bust-a-Kapanen":
                nickname = 'Jordan Sandler'
            team_photo_url = team_data.team_logos[0].url
            df_teams.loc[len(df_teams)] = (self.year, team_name, team_key, nickname, division, team_photo_url)
        df_teams.to_csv(f'{self.current_directory}/TEAMS_METADATA/{self.year}_teams.csv', index=False)
        print(f'>>>> [Rundate: {time.ctime()}] Successfully parsed {self.year} teams metadata')


