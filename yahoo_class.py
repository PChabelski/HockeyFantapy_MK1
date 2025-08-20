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
        df_nhl_codes = pd.read_csv(f'{self.current_directory}/MANUAL_DATA/Hockey_Team_Codes.csv')
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
        df_sched['urlcode'] = df_sched.apply(
            lambda x: str(x['date'].replace('-',''))+'0'+df_nhl_codes[df_nhl_codes['Team_Name']==x['home']]['Code'].values[0], axis=1)

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

        # Finally, go through and fill the gaps (start date to end date) with blank rows, just so we have a full schedule
        all_dates = pd.date_range(start=df_sched['date'].min(), end=df_sched['date'].max(), freq='D')
        for date in all_dates:
            if date.strftime('%Y-%m-%d') not in df_sched['date'].values:
                # Create a new row with NaN values for the missing date
                # but derive the week number using the previous day's week number
                previous_date = date - timedelta(days=1)
                previous_week_number = df_sched.loc[df_sched['date'] == previous_date.strftime('%Y-%m-%d'), 'week_number']
                if not previous_week_number.empty:
                    week_number = previous_week_number.values[0]
                new_row = (
                    date.strftime('%Y-%m-%d'),
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    week_number
                )
                df_sched.loc[len(df_sched)] = new_row

        # Save the schedule DataFrame to a CSV file
        df_sched.sort_values(by=['date'], inplace=True)
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

    def METADATA_PLAYERS(self):

        players = self.query.get_league_players()
        print(f'Successfully pulled yahoo player metadata for {self.year}:{self.league_id}:{self.game_id}')
        player_df = pd.DataFrame(columns=['Season', 'Name', 'Player_Key','Player_ID', 'Primary_Position', 'Headshot_URL'])
        for i in range(len(players)):
            try:
                player_row = players[i]
                name = player_row.name.full
                player_yhid = player_row.player_id
                player_key = player_row.player_key
                primary_position = player_row.primary_position
                headshot_url = player_row.image_url
            except:
                player_row = players[i]['player']
                name = player_row.name.full
                player_yhid = player_row.player_id
                player_key = player_row.player_key
                primary_position = player_row.primary_position
                headshot_url = player_row.image_url
            player_df.loc[len(player_df)] = (self.year, name, player_key, player_yhid, primary_position, headshot_url)

        print(f'Successfully dumped yahoo player metadata for {self.year}:{self.league_id}:{self.game_id}')

        # this is used by the draft / transaction df
        player_df.to_csv(f'{self.current_directory}/YAHOO_PLAYER_METADATA/Yahoo_Players_{self.year}.csv', index=False)

    def TRANSACTIONS(self):

            print(f'>>>> [Rundate: {time.ctime()}] Deriving transactions for {self.year}:{self.league_id}:{self.game_id}')
            df_trans = pd.DataFrame(columns=['season','week','transaction_date','transaction_type','transaction_id','status','player_id','name','draft_round','faab_bid','source','source_key','destination','destination_key','waiver','keeper'])

            # I should make these inherited from the class; run the baseline metadata parsers first
            # then run everything that requires this info

            try:
                df_weeks = pd.read_csv(f'{self.current_directory}/NHL_Schedules/{self.year}_NHL_Schedule.csv')
            except FileNotFoundError:
                print(f'>>>> [Rundate: {time.ctime()}] No schedule found for {self.year}. Skipping transactions parsing - make sure the data is available.')
                return
            try:
                df_teams = pd.read_csv(f'{self.current_directory}/TEAMS_METADATA/{self.year}_teams.csv')
            except FileNotFoundError:
                print(f'>>>> [Rundate: {time.ctime()}] No teams metadata found for {self.year}. Skipping transactions parsing - make sure the data is available.')
                return
            try:
                df_players = pd.read_csv(f'{self.current_directory}/YAHOO_PLAYER_METADATA/Yahoo_Players_{self.year}.csv')
            except FileNotFoundError:
                print(f'>>>> [Rundate: {time.ctime()}] No players metadata found for {self.year}. Skipping transactions parsing - make sure the data is available.')
                return



            transactions = self.query.get_league_transactions()
            # go through the transactions and parse them
            for i in range(0, len(transactions)):

                trans = transactions[i]
                trans_type = trans.type
                trans_time = datetime.fromtimestamp(trans.timestamp)
                trans_datetime = trans_time.strftime('%Y-%m-%d')
                try:
                    trans_week = df_weeks[df_weeks['date'] == trans_datetime]['week_number'].values[0]
                except:
                    trans_week = 0

                trans_id = str(self.year) + "_" + str(trans_week) + "_Trans_" + str(trans.transaction_id)
                trans_status = trans.status

                try: # some years we have no faab_bid
                    faab_bid = trans.faab_bid
                except:
                    faab_bid = np.nan
                if trans_type == 'add':
                    player_id = trans.players[0].player_key
                    player_name = trans.players[0].name.full
                    destination = trans.players[0].transaction_data.destination_team_name
                    destination_key = trans.players[0].transaction_data.destination_team_key
                    destination_type = trans.players[0].transaction_data.destination_type
                    source = 'Free Agency'
                    source_key = '99.l.99.t.99'
                    source_type = trans.players[0].transaction_data.source_type
                    waiver_check = 'YES' if source_type == 'waivers' else 'NO'
                    df_trans.loc[len(df_trans)] = (self.year, trans_week, trans_time, trans_type,
                                                   trans_id, trans_status, player_id,
                                                   player_name, '', faab_bid, source, source_key,
                                                   destination, destination_key, waiver_check, '')


                elif trans_type == 'drop':
                    player_id = trans.players[0].player_key
                    player_name = trans.players[0].name.full
                    source = trans.players[0].transaction_data.source_team_name
                    source_key = trans.players[0].transaction_data.source_team_key
                    source_type = trans.players[0].transaction_data.source_type
                    waiver_check = 'YES' if source_type == 'waivers' else 'NO'
                    destination = 'Free Agency'
                    destination_key = '99.l.99.t.99'
                    destination_type = trans.players[0].transaction_data.destination_type
                    df_trans.loc[len(df_trans)] = (self.year, trans_week, trans_time, trans_type,
                                                   trans_id, trans_status, player_id,
                                                   player_name, '', '', source, source_key,
                                                   destination, destination_key, waiver_check, '')

                elif trans_type == 'add/drop':
                    try:
                        faab_bid = trans.faab_bid
                    except:
                        faab_bid = np.nan

                    add = trans.players[0]
                    player_id = add.player_key
                    player_name = add.name.full
                    # add portion
                    destination = add.transaction_data.destination_team_name
                    destination_key = add.transaction_data.destination_team_key
                    destination_type = add.transaction_data.destination_type
                    source = 'Free Agency'
                    source_key = '99.l.99.t.99'
                    source_type = add.transaction_data.source_type
                    waiver_check = 'YES' if source_type == 'waivers' else 'NO'
                    trans_type = 'add'
                    df_trans.loc[len(df_trans)] = (self.year, trans_week, trans_time, trans_type,
                                                   trans_id, trans_status, player_id,
                                                   player_name, '', faab_bid, source, source_key,
                                                   destination, destination_key, waiver_check, '')

                    # drop portion
                    drop = trans.players[1]
                    player_id = drop.player_key
                    player_name = drop.name.full

                    source = drop.transaction_data.source_team_name
                    source_key = drop.transaction_data.source_team_key
                    source_type = drop.transaction_data.source_type
                    waiver_check = 'YES' if source_type == 'waivers' else 'NO'
                    destination = 'Free Agency'
                    destination_key = '99.l.99.t.99'
                    destination_type = drop.transaction_data.destination_type
                    trans_type = 'drop'
                    df_trans.loc[len(df_trans)] = (self.year, trans_week, trans_time, trans_type,
                                                   trans_id, trans_status, player_id,
                                                   player_name, '', '', source, source_key,
                                                   destination, destination_key, waiver_check, '')

                elif trans_type == 'trade':
                    if len(trans.picks) > 0:
                        # There will always be an even amount of picks
                        for pck in range(0, len(trans.picks)):
                            pick = trans.picks[pck]
                            player_id = '999.p.9999'
                            og_team_name = pick.original_team_name
                            og_gm_name = df_teams[df_teams['Team_Name'] == og_team_name]['GM_Name'].values[0]

                            # Special case for 2021 where some GM names were changed across the year - this is more for the trades that involved the previous year's draft pick and thus their name
                            if self.year == 2021:
                                if og_gm_name == 'Kristofer':
                                    og_gm_name = 'Mack'
                                if og_gm_name == 'Cole':
                                    og_gm_name = 'Taylor'
                                if og_gm_name == 'Tingle':
                                    og_gm_name = 'Nigel'
                           # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

                            player_name = str(self.year + 1) + " " + og_gm_name + " " + "Round " + str(pick.round) + " Draft Pick"
                            draft_round = pick.round
                            source = pick.source_team_name
                            source_key = pick.source_team_key
                            destination = pick.destination_team_name
                            destination_key = pick.destination_team_key
                            waiver_check = 'NO'
                            df_trans.loc[len(df_trans)] = (self.year, trans_week, trans_time, trans_type,
                                                           trans_id, trans_status, player_id,
                                                           player_name, draft_round, '', source, source_key,
                                                           destination, destination_key, waiver_check, '')

                    if trans.players == None:
                        continue  # this is likely a stupid 17 for 17 trade
                    elif len(trans.players) == 0:
                        continue  # this is likely a stupid 17 for 17 trade
                    elif len(trans.players) == 1:
                        player = trans.players[0]  # ['player']
                        player_id = player.player_key
                        player_name = player.name.full
                        destination = player.transaction_data.destination_team_name
                        destination_key = player.transaction_data.destination_team_key
                        destination_type = player.transaction_data.destination_type
                        source = player.transaction_data.source_team_name
                        source_key = player.transaction_data.source_team_key
                        source_type = player.transaction_data.source_type
                        waiver_check = 'YES' if source_type == 'waivers' else 'NO'
                        df_trans.loc[len(df_trans)] = (self.year, trans_week, trans_time, trans_type,
                                                       trans_id, trans_status, player_id,
                                                       player_name, '', '', source, source_key,
                                                       destination, destination_key, waiver_check, '')
                    else:
                        for plr in range(0, len(trans.players)):
                            player = trans.players[plr]  # ['player']
                            player_id = player.player_key
                            player_name = player.name.full

                            destination = player.transaction_data.destination_team_name
                            destination_key = player.transaction_data.destination_team_key
                            destination_type = player.transaction_data.destination_type
                            source = player.transaction_data.source_team_name
                            source_key = player.transaction_data.source_team_key
                            source_type = player.transaction_data.source_type
                            waiver_check = 'YES' if source_type == 'waivers' else 'NO'
                            df_trans.loc[len(df_trans)] = (self.year, trans_week, trans_time, trans_type,
                                                           trans_id, trans_status, player_id,
                                                           player_name, '', '', source, source_key,
                                                           destination, destination_key, waiver_check, '')
                else:  # this is commish -> i don't know what to do with these
                    trans_status = trans.status
                    trans_time = datetime.fromtimestamp(trans.timestamp)
                    trans_id = str(self.year) + "_" + str(trans_week) + "_" + str(trans.transaction_id)




            print(f'>>>> [Rundate: {time.ctime()}] Deriving draft for {self.year}:{self.league_id}:{self.game_id}')
            draft = self.query.get_league_draft_results()
            for drft in range(0, len(draft)):
                draft_pick = draft[drft]  # ['draft_result']
                draft_time = self.control_file['keepers'][str(self.year)]['draft_date']
                draft_type = 'draft'
                pick_number = draft_pick.pick
                pick_round = draft_pick.round
                player_id = draft_pick.player_key
                player_name = df_players[df_players['Player_Key'] == player_id]['Name'].values[0]
                if player_name == 'Sebastian Aho':
                    player_name = 'Sebastian Antero Aho'
                destination_key = draft_pick.team_key
                destination = df_teams[df_teams['Team_Key'] == destination_key]['Team_Name'].values[0]
                source_key = '99.l.99.t.99'
                source = 'Free Agency'
                draft_id = str(self.year) + "_0_Draft_" + str(pick_number)
                draft_status = 'successful'
                waiver_check = ''
                try:
                    keeper_check = 'KEEPER' if player_name in self.control_file['keepers'][str(self.year)][destination_key] else 'NO'
                except:
                    # for years 2017 and back
                    keeper_check = 'NO'
                df_trans.loc[len(df_trans)] = (self.year, 0, draft_time, draft_type,
                                               draft_id, draft_status, player_id,
                                               player_name, pick_round, '', source, source_key,
                                               destination, destination_key, waiver_check, keeper_check)

            df_trans = df_trans.merge(df_teams[['Team_Name', 'GM_Name']], how='left', left_on='destination',
                                      right_on='Team_Name')
            df_trans = df_trans.merge(df_teams[['Team_Name', 'GM_Name']], how='left', left_on='source',
                                      right_on='Team_Name', suffixes=('_destination', '_source'))
            df_trans.drop(['Team_Name_destination', 'Team_Name_source'], axis=1, inplace=True)
            df_trans.to_csv(f'{self.current_directory}/TRANSACTIONS/{self.year}_transactions.csv', index=False)

    def METADATA_MATCHUPS(self):

        df_matchups = pd.DataFrame(columns=['Year', 'Week', 'Matchup ID', 'Team_Label','Team','Team_Key', 'Division', 'Playoffs'])
        try:
            df_weeks = pd.read_csv(f'{self.current_directory}/NHL_Schedules/{self.year}_NHL_Schedule.csv')
        except FileNotFoundError:
            print(
                f'>>>> [Rundate: {time.ctime()}] No schedule found for {self.year}. Skipping transactions parsing - make sure the data is available.')
            return
        try:
            df_teams = pd.read_csv(f'{self.current_directory}/TEAMS_METADATA/{self.year}_teams.csv')
        except FileNotFoundError:
            print(
                f'>>>> [Rundate: {time.ctime()}] No teams metadata found for {self.year}. Skipping transactions parsing - make sure the data is available.')
            return

        for week in df_weeks['week_number'].unique():
            if np.isnan(week):
                continue
            try:
                matchups = self.query.get_league_matchups_by_week(str(int(week)))
                print(f'>>>> [Rundate: {time.ctime()}] Parsing matchups for {self.year} week {week} - found {len(matchups)} matchups')
            except:
                print(f'>>>> [Rundate: {time.ctime()}] Parsing matchups for {self.year} week {week} - no matchups found (league over?)')
                continue
            for matchup_id in range(0, len(matchups)):
                matchup_data = matchups[matchup_id]
                team_a_data = matchup_data.teams[0]
                team_a_name = team_a_data.name.decode('utf-8')
                team_a_key = str(team_a_data.team_key)
                team_a_division = team_a_data.division_id
                is_playoffs = matchup_data.is_playoffs
                df_matchups.loc[len(df_matchups)] = (self.year, week, matchup_id, 'A',team_a_name, team_a_key, team_a_division, is_playoffs)

                team_b_data = matchup_data.teams[1]
                team_b_name = team_b_data.name.decode('utf-8')
                team_b_key = str(team_b_data.team_key)
                team_b_division = team_b_data.division_id
                df_matchups.loc[len(df_matchups)] = (self.year, week, matchup_id, 'B',team_b_name, team_b_key, team_b_division, is_playoffs)


        df_matchups = df_matchups.merge(df_teams[['Team_Key','GM_Name', 'Photo_URL']], how='left', left_on='Team_Key',right_on='Team_Key')
        df_matchups.to_csv(f'{self.current_directory}/MATCHUPS_METADATA/{self.year}_matchups_metadata.csv', index=False)
        print(f'>>>> [Rundate: {time.ctime()}] Successfully parsed {self.year} matchups metadata')



    def ONLINE_DATA_PARSER_HOCKEYREFERENCE(self, dates_to_check):

        df_schedule = pd.read_csv(f'{self.current_directory}/NHL_Schedules/{self.year}_NHL_Schedule.csv')
        ref_list = pd.read_csv(f'{self.current_directory}/MANUAL_DATA/Hockey_Team_Codes.csv')

        for date in dates_to_check:
            games_in_day_df = pd.DataFrame()
            # this is the same code snippet I've been using for many years to pull the hockey-reference data
            # Could use a bit of scrubbing BUT it works so leaving it for now
            url_list = df_schedule[df_schedule['date']==date]['urlcode'].unique()
            for url in url_list:
                urlGame = f'https://www.hockey-reference.com/boxscores/{url}.html'
                page = requests.get(urlGame)
                if page.status_code == 429:
                    print(f'>>>> [Rundate: {time.ctime()}] Rate limit hit at HR parser! Break out.')
                    return

                soup = BeautifulSoup(page.content, 'html.parser')
                try:
                    url_date = datetime.strptime(url[:8], '%Y%m%d').strftime('%Y-%m-%d')
                except:
                    print(f'>>>> [Rundate: {time.ctime()}] No game found for {urlGame}. Skipping this date.')
                    continue
                test = soup.find('div', id=["inner_nav"])
                list_metadata = list(test)
                list_metadata = list_metadata[1].text.split('\n')
                list_metadata = [x for x in list_metadata if x != '']
                list_metadata = [x for x in list_metadata if x != ' ']
                home_team = list_metadata[-1].split('Schedule/Results')[0].strip()
                home_code = url[9:]
                away_team = list_metadata[-2].split('Schedule/Results')[0].strip()
                away_code = ref_list['Code'][ref_list['Team_Name'] == away_team].values[0].strip()
                # skater data has 17 colsfor 2014 onwards
                # but only 13 for 2013 and before
                resetCounter = 17 if self.year > 2013 else 14
                ###########################################################
                # Team 1 are away. Team 2 are home
                team_1_skaters = soup.find('table', id=[f"{away_code}_skaters"])
                team_2_skaters = soup.find('table', id=[f"{home_code}_skaters"])
                team_1_goalies = soup.find('table', id=[f"{away_code}_goalies"])
                team_2_goalies = soup.find('table', id=[f"{home_code}_goalies"])
                team_1_adv_stats = soup.find('table', id=[f"{away_code}_adv_ALLAll"])
                team_2_adv_stats = soup.find('table', id=[f"{home_code}_adv_ALLAll"])
                ##############################################################
                skater_data = []
                total_skater_data = []
                headers = list(team_1_skaters)[5].get_text().split('\n')
                headers = [x for x in headers if x != '']
                colDropInd = 4 if self.year > 2013 else 3 # differences in the years thereafter
                headers = headers[colDropInd:]  # drop the first few dressing columns
                for i in range(0, len(list(list(team_1_skaters)[7].find_all('td')))):
                    if (i % resetCounter == 0) and (i != 0):
                        total_skater_data.append(skater_data)
                        skater_data = []
                    value = list(list(team_1_skaters)[7].find_all('td'))[i].text
                    skater_data.append(value)

                total_skater_data.append(skater_data)  # to get the goalie, for concat purposes
                awaySkaterDf = pd.DataFrame(data=total_skater_data, columns=headers)
                skater_data = []
                total_skater_data = []
                for i in range(0, len(list(list(team_2_skaters)[7].find_all('td')))):
                    if (i % resetCounter == 0) and (i != 0):
                        total_skater_data.append(skater_data)
                        skater_data = []
                    value = list(list(team_2_skaters)[7].find_all('td'))[i].text
                    skater_data.append(value)
                total_skater_data.append(skater_data)  # to get the goalie, for concat purposes

                awaySkaterDf['Date'] = date
                awaySkaterDf['Team'] = away_team
                awaySkaterDf['Team_Code'] = away_code
                homeSkaterDf = pd.DataFrame(data=total_skater_data, columns=headers)
                homeSkaterDf['Date'] = date
                homeSkaterDf['Team'] = home_team
                home_code = 'VGK' if home_code == 'VEG' else home_code
                homeSkaterDf['Team_Code'] = home_code

                total_skater_df = pd.concat([homeSkaterDf, awaySkaterDf])

                ######################################
                goalie_data = []
                total_goalie_data = []

                headers = list(team_1_goalies)[5].get_text().split('\n')

                headers = [x for x in headers if x != '']
                headers = headers[2:]
                for i in range(0, len(list(list(team_1_goalies)[7].find_all('td')))):
                    value = list(list(team_1_goalies)[7].find_all('td'))[i].text
                    goalie_data.append(value)
                    if (i % 8 == 0) and (i != 0):
                        total_goalie_data.append(goalie_data)
                        goalie_data = []
                awaygoalieDf = pd.DataFrame(data=total_goalie_data, columns=headers)
                awaygoalieDf['Date'] = date
                awaygoalieDf['Team'] = away_team
                away_code = 'VGK' if away_code == 'VEG' else away_code
                awaygoalieDf['Team_Code'] = away_code
                goalie_data = []
                total_goalie_data = []
                for i in range(0, len(list(list(team_2_goalies)[7].find_all('td')))):
                    value = list(list(team_2_goalies)[7].find_all('td'))[i].text
                    goalie_data.append(value)
                    if (i % 8 == 0) and (i != 0):
                        total_goalie_data.append(goalie_data)
                        goalie_data = []
                homeGoalieDf = pd.DataFrame(data=total_goalie_data, columns=headers)
                homeGoalieDf['Date'] = date
                homeGoalieDf['Team'] = home_team
                homeGoalieDf['Team_Code'] = home_code
                total_goalie_df = pd.concat([awaygoalieDf, homeGoalieDf])
                total_goalie_df.drop(['PIM', 'TOI'], inplace=True,
                                     axis=1)  # we will concat this to the total df, which already contains the data

                #################################################################

                adv_skater_data = []
                adv_total_skater_data = []
                headers = list(team_1_adv_stats)[5].get_text().split(' ')
                headers = [x for x in headers if x != '']
                headers = headers[1:]
                for i in range(0, len(list(list(team_1_adv_stats)[7].find_all('td')))):

                    if (i % 10 == 0) and (i != 0):
                        adv_total_skater_data.append(adv_skater_data)
                        adv_skater_data = []
                    value = list(list(team_1_adv_stats)[7].find_all('td'))[i].text
                    adv_skater_data.append(value)
                adv_total_skater_data.append(adv_skater_data)  # for the last skater

                adv_skater_data = []
                for i in range(0, len(list(list(team_2_adv_stats)[7].find_all('td')))):
                    if (i % 10 == 0) and (i != 0):
                        adv_total_skater_data.append(adv_skater_data)
                        adv_skater_data = []
                    value = list(list(team_2_adv_stats)[7].find_all('td'))[i].text
                    adv_skater_data.append(value)
                adv_total_skater_data.append(adv_skater_data)  # for the last skater

                total_adv_skater_df = pd.DataFrame(data=adv_total_skater_data, columns=headers)

                # the adv stats table has the names in th for some reason
                plyrList = []
                for i in range(0, len(list(list(team_1_adv_stats)[7].find_all('th')))):
                    plyrList.append(list(list(team_1_adv_stats)[7].find_all('th'))[i].text)
                for i in range(0, len(list(list(team_2_adv_stats)[7].find_all('th')))):
                    plyrList.append(list(list(team_2_adv_stats)[7].find_all('th'))[i].text)

                total_adv_skater_df['Player'] = plyrList

                concat_df1 = total_skater_df.merge(total_adv_skater_df, on=["Player"], how='left')
                final_hr_df = concat_df1.merge(total_goalie_df, on=["Player", "Date", "Team"], how='left')
                final_hr_df.drop(['Team_Code_y'], axis=1, inplace=True)
                games_in_day_df = pd.concat([games_in_day_df, final_hr_df])
            games_in_day_df.to_csv(f'{self.current_directory}/ONLINE_PARSED_DATA/HR/{self.year}/HR_Stats_{date}.csv', index=False)
            print(f'>>>> [Rundate: {time.ctime()}] Successfully parsed HR data for {url}.')

            time.sleep(30)  # to avoid hitting the rate limit on hockey-reference.com


    def ONLINE_DATA_PARSER_NATURALSTATTRICK(self, dates_to_check):

        df_schedule = pd.read_csv(f'{self.current_directory}/NHL_Schedules/{self.year}_NHL_Schedule.csv')
        ref_list = pd.read_csv(f'{self.current_directory}/MANUAL_DATA/Hockey_Team_Codes.csv')

        for date in dates_to_check:

            year = date.split('-')[0]
            month = date.split('-')[1]
            day = date.split('-')[2]
            url_year = str(self.year)+str(self.year+1)
            # SKATER PART
            urlPart1 = f'https://www.naturalstattrick.com/playerteams.php?fromseason={url_year}&thruseason={url_year}'
            urlPart2 = f'&stype=2&sit=all&score=all&stdoi=std&rate=n&team=ALL&pos=S&loc=B&toi=0&gpfilt=gpdate&fd={year}-{month}-{day}&td={year}-{month}-{day}&tgp=410&lines=single&draftteam=ALL'
            url = urlPart1 + urlPart2
            print(url)
            page = requests.get(url)
            if page.status_code == 429:
                print(f'>>>> [Rundate: {time.ctime()}] Rate limit hit at NST parser! Break out.')
                return
            soup = BeautifulSoup(page.content, 'html.parser')
            top = list(soup.children)
            # The .text method will extract the text within the tags.
            # We’ll pass the columns text into a list to use later.
            body = list(top[1].children)[14]
            columns = [item.text for item in body.find_all('th')]
            # Now we’ll find all the ‘td’ tags where the data is contained
            # And we’ll pass the text of the data into a list
            data = [e.text for e in body.find_all('td')]
            # Construct the pandas dataframe that will contain all the pulled data
            start = 0
            table = []
            # loop through entire data
            while start + len(columns) <= len(data):
                player = []
                # use length of columns as iteration stop point to get list of info for 1 player
                for i in range(start, start + len(columns)):
                    player.append(data[i])
                # add player row to list
                table.append(player)
                # start at next player
                start += len(columns)

            df_skater = pd.DataFrame(table, columns=columns).set_index('')
            # GOALIE PART
            urlPart1 = f'https://www.naturalstattrick.com/playerteams.php?fromseason={url_year}&thruseason={url_year}'
            urlPart2 = f'&stype=2&sit=all&score=all&stdoi=g&rate=n&team=ALL&pos=S&loc=B&toi=0&gpfilt=gpdate&fd={year}-{month}-{day}&td={year}-{month}-{day}&tgp=410&lines=single&draftteam=ALL'
            url = urlPart1 + urlPart2
            page = requests.get(url)
            # print(page.status_code)
            soup = BeautifulSoup(page.content, 'html.parser')
            top = list(soup.children)
            # The .text method will extract the text within the tags.
            # We’ll pass the columns text into a list to use later.
            body = list(top[1].children)[14]
            columns = [item.text for item in body.find_all('th')]
            # Now we’ll find all the ‘td’ tags where the data is contained
            # And we’ll pass the text of the data into a list
            data = [e.text for e in body.find_all('td')]
            # Construct the pandas dataframe that will contain all the pulled data
            start = 0
            table = []
            # loop through entire data
            while start + len(columns) <= len(data):
                player = []
                # use length of columns as iteration stop point to get list of info for 1 player
                for i in range(start, start + len(columns)):
                    player.append(data[i])
                # add player row to list
                table.append(player)
                # start at next player
                start += len(columns)

            df_goalies = pd.DataFrame(table, columns=columns).set_index('')
            df_data = pd.concat([df_skater, df_goalies])
            df_data['Date'] = date
            df_data['Code'] = df_data['Team'].map(ref_list.set_index('Code_nst')['Code_alt'])

            df_data.to_csv(f'{self.current_directory}/ONLINE_PARSED_DATA/NST/{self.year}/NST_Stats_{date}.csv', index=False)

            time.sleep(30) # to avoid hitting the rate limit on naturalstattrick.com
