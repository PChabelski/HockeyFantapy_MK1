
import numpy as np
import pandas as pd
pd.options.display.float_format = '{:,}'.format
pd.set_option('mode.chained_assignment', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)
np.seterr(divide='ignore')
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings('error')
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import logging
logging.getLogger("yfpy.query").setLevel(level=logging.INFO)
import glob
pd.options.display.float_format = '{:,}'.format
import time
from datetime import datetime
from datetime import timedelta
from bs4 import BeautifulSoup, Comment
import requests
from urllib.request import urlopen
import yfpy
pd.set_option('mode.chained_assignment', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)
np.seterr(divide='ignore')
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings('error')
import os

# Define globals
root_dir = os.getcwd()+'/'




#########################################################################
def scheduleParser(year, control_file):
    if not control_file['NHL Schedule Parser']:
        print(f'>>>> [Rundate: {time.ctime()}] Not running schedule scraper')
        return

    print(f'>>>> [Rundate: {time.ctime()}] Parsing schedules from Hockey-Reference.com for {year}')
    weeks_df = pd.read_csv(f'{root_dir}WEEKS_DATA/{year}_week_data.csv')
    year_plus = year + 1
    games_page = f'https://www.hockey-reference.com/leagues/NHL_{year_plus}_games.html'
    #page = requests.get(games_page)
    page_content = urlopen(games_page).read()
    #soup = BeautifulSoup(page.content, 'html.parser')
    soup = BeautifulSoup(page_content, 'html.parser')

    items = soup.find(id="all_games").find_all(class_="left")
    schedule_data = []

    for i in range(5, len(items), 5):
        try:
            date_str = items[i].find("a").getText()
        except:
            date_str = datetime.strptime(items[i].getText(), "%Y-%m-%d")
        date = datetime.strptime(date_str, "%Y-%m-%d")
        weekStat = next(
            (week for week in weeks_df['week'].unique()
             if datetime.strptime(weeks_df[weeks_df['week'] == week]['start'].values[0], '%Y-%m-%d') <= date <
                datetime.strptime(weeks_df[weeks_df['week'] == week]['end'].values[0], '%Y-%m-%d') + timedelta(1)),
            0
        )

        teamA = items[i + 2].find("a").getText()
        teamB = items[i + 3].find("a").getText()
        schedule_data.append({'date': date.strftime("%Y-%m-%d"), 'away': teamA, 'home': teamB, 'week': weekStat})

    print(f'>>>> [Rundate: {time.ctime()}] Got all the data')
    df_sched = pd.DataFrame(schedule_data)

    # Calculate game counts using groupby and cumcount
    df_sched['home_count'] = df_sched.groupby('home').cumcount() + 1
    df_sched['away_count'] = df_sched.groupby('away').cumcount() + 1

    # Parse the page and extract game links, then add URLCODE to the schedule
    list_of_games = [
        a['href'][11:-5] for a in soup.find_all('a', href=True)
        if a['href'].startswith('/boxscores/') and len(a['href'][11:-5]) > 11
    ]
    print(f'>>>> [Rundate: {time.ctime()}] Got list of games')

    # Add URLCODE to the schedule DataFrame and save
    df_sched['URLCODE'] = df_sched['date'].map(
        lambda d: next((g for g in list_of_games if datetime.strptime(g[:8], '%Y%m%d').strftime('%Y-%m-%d') == d), None)
    )

    print(f'>>>> [Rundate: {time.ctime()}] URLCODE added via mapper')
    df_sched.to_csv(f'{root_dir}NHL_Schedules/{year}_NHL_Schedule.csv', index=False)
    print(f'--> [Rundate: {time.ctime()}] {year} schedule successfully parsed')

def weeks_parser(year, control_file):
    # really this should be renamed the week metadata file
    if control_file['Weeks Metadata']:
        print(f'METADATA {year}: Parsing weeks metadata from yahoo')

        league_id = control_file['Years'][str(year)]['league_id']
        game_id = control_file['Years'][str(year)]['game_id']
        query = yfpy.YahooFantasySportsQuery(auth_dir='C:/Users/16472/PycharmProjects/Hockey_FantaPy/private',
                                             league_id=str(league_id),
                                             game_id=str(game_id),
                                             game_code='nhl',
                                             all_output_as_json_str=False
                                             )

        gameWeeks = query.get_game_weeks_by_game_id(game_id)
        tempDict = {}
        league_season_info = {}
        weeks_df = pd.DataFrame(
            columns=['year', 'week', 'start', 'end'])

        for i in range(0, len(gameWeeks)):
            week_number = gameWeeks[i].week
            start = gameWeeks[i].start
            end = gameWeeks[i].end
            weeks_df.loc[len(weeks_df)] = (year, week_number, start, end)

        weeks_df.to_csv(f'{root_dir}WEEKS_DATA/{year}_week_data.csv', index=False)
        print(f'>>>> [Rundate: {time.ctime()}] Season metadata (league, teams, week, and player info) parsed')

        return league_season_info
    else:
        print(f'>>>> [Rundate: {time.ctime()}] Not running {year} season metadata parser')


def online_data_stitcher(status):
    if status:
        glued_online_data = pd.DataFrame()
        for file_name in glob.glob(f'{root_dir}MegaRepo/' + '*.csv'):
            x = pd.read_csv(root_dir+file_name, low_memory=False)
            glued_online_data = pd.concat([glued_online_data, x], axis=0)
        glued_online_data.to_csv(f'{root_dir}Parkdale_Fantasy_Hockey_Mega_Stats_data.csv', index=False)
        print(f'>>>> [Rundate: {time.ctime()}] Online stats done')
    else:
        print(f'>>>> [Rundate: {time.ctime()}] Not stitching NST-HR-Yahoo data')


def natStatTrick_parser(gameRegisterDf, urlYear):
    df_team_codes = pd.read_csv(f'{root_dir}Hockey_Team_Codes.csv', low_memory=False)

    dateResetCounter = 0
    for iter_date in gameRegisterDf['Date']:
        timeA = time.time()
        count = 0

        year = iter_date.split('-')[0]
        month = iter_date.split('-')[1]
        day = iter_date.split('-')[2]
        # SKATER PART
        urlPart1 = f'https://www.naturalstattrick.com/playerteams.php?fromseason={urlYear}&thruseason={urlYear}'
        urlPart2 = f'&stype=2&sit=all&score=all&stdoi=std&rate=n&team=ALL&pos=S&loc=B&toi=0&gpfilt=gpdate&fd={year}-{month}-{day}&td={year}-{month}-{day}&tgp=410&lines=single&draftteam=ALL'
        url = urlPart1 + urlPart2
        page = requests.get(url)
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
        urlPart1 = f'https://www.naturalstattrick.com/playerteams.php?fromseason={urlYear}&thruseason={urlYear}'
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
        df_data['Date'] = iter_date
        df_data['Code'] = df_data['Team'].map(df_team_codes.set_index('Code_nst')['Code_alt'])

        colList = df_data.columns.to_list()
        colListUpdated = [s + '_NST' for s in colList]
        df_data.columns = colListUpdated

        time.sleep(20)
        return df_data

def chrono_trigger(year, runType, control_file):
    # New module to control dates of processing!
    ymdformat = '%Y-%m-%d'
    all_dates = []
    schedule_df = pd.read_csv(f'{root_dir}NHL_Schedules/{year}_NHL_Schedule.csv')
    weeks_metadata = pd.read_csv(f'{root_dir}WEEKS_DATA/{year}_week_data.csv')
    season_start = weeks_metadata['start'].iloc[0]
    season_end = weeks_metadata[weeks_metadata['week'] == weeks_metadata['week'].max()]['end'].values[0]
    nhl_season_start = schedule_df['date'].iloc[0]
    nhl_season_end = schedule_df['date'].iloc[-1]
    print(
        f'{year} Yahoo Season goes from {season_start} to {season_end} || NHL Season goes from {nhl_season_start} to {nhl_season_end}')
    if runType == 'Yahoo_full':
        startDate = datetime.strptime(season_start, ymdformat)
        endDate = datetime.strptime(season_end, ymdformat)
        dateArray = pd.date_range(start=startDate, end=endDate).to_list()
    elif runType == 'Start_to_now':
        startDate = datetime.strptime(season_start, ymdformat)
        endDate = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
        dateArray = pd.date_range(start=startDate, end=endDate).to_list()
        if len(dateArray) > 365:
            print(
                f'Careful! You might want to change the season pull, there are {len(dateArray)} dates to analyze for this start-to-season analysis')
            return None
        else:
            pass
    elif runType == 'Today':
        startDate = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
        endDate = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
        dateArray = pd.date_range(start=startDate, end=endDate).to_list()
    elif runType == 'NHL':
        startDate = schedule_df['date'].iloc[0]
        endDate = schedule_df['date'].iloc[-1]
        dateArray = pd.date_range(start=startDate, end=endDate).to_list()
    elif runType == 'Custom':
        # pull the specific range
        startDate = control_file['custom_start']
        endDate = control_file['custom_end']
        dateArray = pd.date_range(start=startDate, end=endDate).to_list()
    iter_date_array = [x.to_pydatetime().strftime("%Y-%m-%d") for x in dateArray]
    all_dates.extend(iter_date_array)
    print(f'Total number of dates to analyze: {len(all_dates)} for {year} season')
    return all_dates


def hockeyReference_parser(gameRegisterDf, year):
    date_df = pd.DataFrame()
    df_team_codes = pd.read_csv(f'{root_dir}Hockey_Team_Codes.csv', low_memory=False)

    for j in range(0, len(gameRegisterDf)):
        date = gameRegisterDf['Date'].iloc[j]
        url = gameRegisterDf['URLCODE'].iloc[j]
        urlGame = f'https://www.hockey-reference.com/boxscores/{url}.html'
        # page = urlopen(url)
        page = requests.get(urlGame)
        soup = BeautifulSoup(page.content, 'html.parser')

        ###########################################################
        ref_list = pd.read_csv(f'{root_dir}Hockey_Team_Codes.csv')
        date = url[:8]
        date = datetime.strptime(url[:8], '%Y%m%d').strftime('%Y-%m-%d')

        test = soup.find('div', id=["inner_nav"])
        list_metadata = list(test)
        list_metadata = list_metadata[1].text.split('\n')
        list_metadata = [x for x in list_metadata if x != '']
        list_metadata = [x for x in list_metadata if x != ' ']

        home_team = list_metadata[-1].split('Schedule/Results')[0].strip()
        home_code = url[9:]
        away_team = list_metadata[-2].split('Schedule/Results')[0].strip()
        away_code = ref_list['Code'][ref_list['Team_Name'] == away_team].values[0].strip()
        # print(f'{away_team} ({away_code}) at {home_team} ({home_code}) on {date}')

        # skater data has 17 colsfor 2014 onwards
        # but only 13 for 2013 and before
        resetCounter = 17 if year > 2013 else 14

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
        # colDropInd = 4
        colDropInd = 4 if year > 2013 else 3

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
        colList = final_hr_df.columns.to_list()
        colListUpdated = [s + '_HR' for s in colList]
        final_hr_df.columns = colListUpdated
        # final_hr_df['UID'] = final_hr_df['Player_HR'].astype(str) + "_" + final_hr_df['Team_Code_HR'].astype(str) + "_" + final_hr_df['Date_HR'].astype(str)
        # final_hr_df['UID'] = final_hr_df['Player_HR'].astype(str) + "_" + final_hr_df['Date_HR'].astype(str)
        date_df = pd.concat([date_df, final_hr_df])

        if (len(gameRegisterDf) == 1) or (j == len(gameRegisterDf) - 1):
            date_df.drop(['Team_Code_y_HR'], inplace=True,
                         axis=1)
            date_df.replace({'Team_Code_x_HR': 'Team_Code_HR'}, inplace=True)
            return date_df
            date_df = pd.DataFrame()
        time.sleep(8)


def rosterStatsQuery(season, iter_date, query, teamID):
    team_split = teamID.split('.')[4]
    result = query.get_team_roster_player_info_by_date(team_id=team_split, chosen_date=iter_date)

    rosterDict = {}
    for plyr in range(0, len(result)):
        # pylrDict = {plyr:result[plyr]['player'].clean_data_dict()}
        pylrDict = {plyr: result[plyr].clean_data_dict()}
        rosterDict.update(pylrDict)

    return rosterDict


# pass in the date and year, grab all the relevant json info for that datetime, stitch them together with other info
def yahoo_data_collection(season, iter_date, year, schedule, control_file, query):
    df_team_codes = pd.read_csv(f'{root_dir}Hockey_Team_Codes.csv', low_memory=False)
    teams_data = pd.read_csv(f'{root_dir}TEAMS_METADATA/{season}_teams.csv')
    count = 0
    df_rosterstats = pd.DataFrame()
    str_error = None
    for team in teams_data['Team_Name'].unique():
        teamName = team
        teamID = teams_data[teams_data['Team_Name'] == team]['Team_Key'].values[0]
        while True:
            try:
                teamRosterData = rosterStatsQuery(season, iter_date, query, teamID)
                str_error = None
            except Exception as e:
                str_error = str(e)
                print(str_error)
                print(f'{e} - wait before trying again for date: {iter_date}')
                print('Sleeping...')
                time.sleep(60)
                continue
            break

        for plrnum in range(0, len(teamRosterData)):
            plrData = teamRosterData[plrnum]
            firstName = plrData['name']['first']
            lastName = plrData['name']['last']
            fullName = firstName + " " + lastName
            player_key = plrData["player_key"]
            nhlName = plrData['editorial_team_full_name']
            selectedPosition = plrData['selected_position']['position']
            elligiblePositions = str(plrData['eligible_positions'])
            # df_rosterstats.loc[count,'UID'] =str(iter_date)+'-'+str(count)
            df_rosterstats.loc[count, 'Date'] = iter_date
            df_rosterstats.loc[count, 'Team'] = teamName
            df_rosterstats.loc[count, 'Name'] = fullName
            df_rosterstats.loc[count, 'Selected Position'] = selectedPosition
            df_rosterstats.loc[count, 'Elligible Positions'] = elligiblePositions
            df_rosterstats.loc[count, 'Player Key'] = player_key
            df_rosterstats.loc[count, 'Year'] = year
            df_rosterstats.loc[count, 'Week'] = schedule[schedule['date'] == iter_date]['week'].iloc[0]
            if int(year) < 2024:
                df_rosterstats.loc[count, 'Percentage Owned'] = 100
                df_rosterstats.loc[count, 'Percentage Owned Delta'] = 0

            else:
                try: # sometimes the value doesn't exist? so default it to 0 in these cases
                    df_rosterstats.loc[count, 'Percentage Owned'] = plrData['percent_owned']['value']#.iloc[0]
                    df_rosterstats.loc[count, 'Percentage Owned Delta'] = plrData['percent_owned']['delta']#.iloc[0]
                except:
                    print(f"{fullName} had no ownership value!?")
                    df_rosterstats.loc[count, 'Percentage Owned'] = 0
                    df_rosterstats.loc[count, 'Percentage Owned Delta'] = 0
            count += 1

    return df_rosterstats


def online_data_parser(year, dates_of_interest, control_file):
    schedule_df = pd.read_csv(f'{root_dir}NHL_Schedules/{year}_NHL_Schedule.csv')
    if control_file["Online Parser Status"]['overall_status']:
        print(f'>>>> [Rundate: {time.ctime()}] Parsing online data for year: {year}')
        # Retrieve league and game IDs
        league_id = control_file['Years'][str(year)]['league_id']
        game_id = control_file['Years'][str(year)]['game_id']
        # Initialize YahooFantasySportsQuery
        query = yfpy.YahooFantasySportsQuery(
            auth_dir='C:/Users/16472/PycharmProjects/Hockey_FantaPy/private',
            league_id=str(league_id),
            game_id=str(game_id),
            game_code='nhl',
            all_output_as_json_str=False
        )

        for date_item in dates_of_interest:
            print(f'Online parser date item: {date_item}')
            game_register_filtered = schedule_df[schedule_df['date'] == date_item]
            try:
                week_number = schedule_df.loc[schedule_df['date'] == date_item, 'week'].values[0]
            except IndexError:
                continue  # Skip if no games on this date
            ##########################################################################
            if control_file["Online Parser Status"]['yahoo_parse']:
                yahoo_df = yahoo_data_collection(year, date_item, year, schedule_df, control_file, query)
                os.makedirs(f'{root_dir}ONLINE_PARSED_DATA/YH_ROSTERS/{year}/', exist_ok=True)
                yahoo_df.to_csv(f'{root_dir}ONLINE_PARSED_DATA/YH_ROSTERS/{year}/YH_stats_{date_item}_percentage_added.csv', index=False)

            ##########################################################################
            if control_file["Online Parser Status"]['hr_parse']:
                hr_df = hockeyReference_parser(game_register_filtered, year)
                hr_df['Week'] = week_number
                hr_df['WINS_HR'] = hr_df['DEC_HR'].copy().replace('W', 1).replace('L', '').replace('O', '')
                hr_df['LOSSES_HR'] = hr_df['DEC_HR'].copy().replace('L', 1).replace('W', '').replace('O', '')
                os.makedirs(f'{root_dir}ONLINE_PARSED_DATA/HR/{year}/', exist_ok=True)
                hr_df.to_csv(f'{root_dir}ONLINE_PARSED_DATA/HR/{year}/HR_stats_{date_item}.csv', index=False)
            if control_file["Online Parser Status"]['nst_parse']:
                nst_df = natStatTrick_parser(game_register_filtered, urlYear)
                nst_df['TOI_NST'] = nst_df["TOI_NST"].astype(float)
                nst_df["Position_NST"] = nst_df["Position_NST"].fillna('G')
                goalie_mask = nst_df["Position_NST"] == 'G'
                nst_df['GOALIE_TOI'] = np.nan
                nst_df.loc[goalie_mask, "GOALIE_TOI"] = nst_df.loc[goalie_mask, "TOI_NST"]
                os.makedirs(f'{root_dir}ONLINE_PARSED_DATA/NST/{year}/', exist_ok=True)
                nst_df.to_csv(f'{root_dir}ONLINE_PARSED_DATA/NST/{year}/NST_stats_{date_item}.csv', index=False)
            print(
                f'>>>> [Rundate: {time.ctime()}] Players NST: {len(nst_df)} Players HR: {len(hr_df)} YH_Rosters: {len(yahoo_df)} [{date_item}]')
    else:
        print(f'>>>> [Rundate: {time.ctime()}] Not running online roster parser')


# def matchup_metadata(year, control_file):
#     if control_file['Matchup Metadata']:
#
#         print(f'>>>> Parsing matchup metadata for {year}')
#
#         # with open('control.json', 'r') as f:
#         #     control_file = json.loads(f.read())
#         # load weeks df
#         weeks = pd.read_csv(f'{root_dir}WEEKS_DATA/{year}_week_data.csv')
#
#         league_id = control_file['Years'][str(year)]['league_id']
#         game_id = control_file['Years'][str(year)]['game_id']
#         query = yfpy.YahooFantasySportsQuery(auth_dir='C:/Users/16472/PycharmProjects/Hockey_FantaPy/private',
#                                              league_id=str(league_id),
#                                              game_id=str(game_id),
#                                              game_code='nhl'
#                                              ##all_output_as_json_str =True
#                                              )
#
#         df_matchups = pd.DataFrame(columns=['Year', 'Week', 'Matchup ID', 'Team_Label',
#                                             'Team', 'GM', 'Image',
#                                             'Team_Key', 'Division', 'Playoffs'])
#         print(f'Grabbing matchups from week 1 to week {weeks["week"].max()}')
#         for week_num in range(1, weeks['week'].max()+1):
#             if (year == 2012):  # 2012 starts at week 12
#                 print(f'Skipping {year} - this one does not work for some reason')
#                 return
#             try:
#                 print(f'Trying {year} week {week_num}...')
#                 matchups = query.get_league_matchups_by_week(str(week_num))
#                 print('Matchups parsed')
#                 for matchup_id in range(0, len(matchups)):
#                     matchup_data = matchups[matchup_id]
#
#                     team_a_data = matchup_data.teams[0]
#                     team_a_name = team_a_data.name.decode('utf-8')
#                     if team_a_name == "Vintage'tingle'Boar":
#                         team_a_nickname = 'Tingle'
#                     elif team_a_name == "The Nerve":
#                         team_a_nickname = 'Ira'
#                     elif (team_a_name == "#G") | (team_a_name == "Grampa Jarzabek"):
#                         team_a_nickname = 'A'
#                     else:
#                         team_a_nickname = str(team_a_data.managers[0].nickname)
#                     if team_a_nickname == 'Doctor Kocktapus':
#                         team_a_nickname = 'Peter'
#                     if team_a_nickname == 't':
#                         team_a_nickname = 'Taylor'
#                     if team_a_nickname == 'Master':
#                         team_a_nickname = 'Yusko'
#                     if team_a_nickname == 'Thomson McKnight':
#                         team_a_nickname = 'Thomson'
#                     if team_a_nickname == 'garrett':
#                         team_a_nickname = 'Garrett'
#                     if team_a_nickname == 'george':
#                         team_a_nickname = 'George'
#                     team_a_image = str(team_a_data.team_logos[0].url)
#                     team_a_key = str(team_a_data.team_key)
#                     team_a_division = team_a_data.division_id
#                     is_playoffs = matchup_data.is_playoffs
#                     df_matchups.loc[len(df_matchups)] = (
#                         year, week_num, matchup_id, 'A',
#                         team_a_name, team_a_nickname, team_a_image, team_a_key, team_a_division, is_playoffs)
#
#                     team_b_data = matchup_data.teams[1]
#                     team_b_name = team_b_data.name.decode('utf-8')
#                     if team_b_name == "Vintage'tingle'Boar":
#                         team_b_nickname = 'Tingle'
#                     elif team_b_name == "The Nerve":
#                         team_b_nickname = 'Ira'
#                     elif (team_b_name == "#G") | (team_b_name == "Grampa Jarzabek"):
#                         team_b_nickname = 'A'
#                     else:
#                         team_b_nickname = str(team_b_data.managers[0].nickname)
#                     if team_b_nickname == 'Doctor Kocktapus':
#                         team_b_nickname = 'Peter'
#                     if team_b_nickname == 't':
#                         team_b_nickname = 'Taylor'
#                     if team_b_nickname == 'Thomson McKnight':
#                         team_b_nickname = 'Thomson'
#                     if team_b_nickname == 'garrett':
#                         team_b_nickname = 'Garrett'
#                     if team_b_nickname == 'george':
#                         team_b_nickname = 'George'
#                     if team_b_nickname == 'Master':
#                         team_b_nickname = 'Yusko'
#                     team_b_image = str(team_b_data.team_logos[0].url)
#
#                     team_b_key = str(team_b_data.team_key)
#                     team_b_division = team_b_data.division_id
#
#                     df_matchups.loc[len(df_matchups)] = (
#                         year, week_num, matchup_id, 'B',
#                         team_b_name, team_b_nickname, team_b_image, team_b_key, team_b_division, is_playoffs)
#
#             except Exception as e:
#                 print(f'Something went wrong: {e}')
#                 pass
#         df_matchups.to_csv(f'{root_dir}MATCHUPS_METADATA/{year}_matchups_metadata.csv', index=False)
#         print(f'>>>> [Rundate: {time.ctime()}] Successfully parsed {year} matchups metadata')
#     else:
#         print(f'>>>> [Rundate: {time.ctime()}] Not running matchup metadata parser')
#
#
# def teams_metadata(year, control_file):
#     if control_file['Teams Metadata']:
#
#         print(f'>>>> Parsing team info for {year}')
#         if year == 2012:
#             print('YFPY has an issue with the json decode for team metada in year 2012...')
#             print('...so we manually created the file and will skip this year')
#             return
#
#         league_id = control_file['Years'][str(year)]['league_id']
#         game_id = control_file['Years'][str(year)]['game_id']
#         query = yfpy.YahooFantasySportsQuery(auth_dir='C:/Users/16472/PycharmProjects/Hockey_FantaPy/private',
#                                              league_id=str(league_id),
#                                              game_id=str(game_id),
#                                              game_code='nhl'
#                                              # all_output_as_json=False
#                                              )
#
#         teams_metadata = query.get_league_teams()
#         df_teams = pd.DataFrame(columns=['Season', 'Team_Name', 'Team_Key', 'GM_Name', 'Division', 'Photo_URL'])
#         for i in range(0, len(teams_metadata)):
#             team_data = teams_metadata[i]
#             team_name = team_data.name.decode('utf-8')
#             team_key = team_data.team_key
#             division = team_data.division_id
#             if team_name == "Vintage'tingle'Boar":
#                 print('Jordan to Tingle replacement')
#                 nickname = 'Tingle'
#             elif (team_name == "The Nerve"):
#                 print('The Nerve to Ira')
#                 nickname = 'Ira'
#             elif (team_name == "#G") | (team_name == "Grampa Jarzabek"):
#                 print('Hidden to A')
#                 nickname = 'A'
#             else:
#                 nickname = team_data.managers[0].nickname
#             if nickname == 'Doctor Kocktapus':
#                 nickname = 'Peter'
#             if nickname == 't':
#                 nickname = 'Taylor'
#             if nickname == 'Thomson McKnight':
#                 nickname = 'Thomson'
#             if nickname == 'garrett':
#                 nickname = 'Garrett'
#             if nickname == 'george':
#                 nickname = 'George'
#             if nickname == 'Master':
#                 nickname = 'Yusko'
#             team_photo_url = team_data.team_logos[0].url
#             df_teams.loc[len(df_teams)] = (year, team_name, team_key, nickname, division, team_photo_url)
#         df_teams.to_csv(f'{root_dir}TEAMS_METADATA/{year}_teams.csv', index=False)
#         print(f'>>>> [Rundate: {time.ctime()}] Successfully parsed {year} teams metadata')
#     else:
#         print(f'>>>> [Rundate: {time.ctime()}] Not running teams metadata parser')




# def player_parser(query, players_unpacked):
#     retry = True  # just for the initialization
#     while retry:
#         try:
#             print(
#                 f' ..... Attempting to parse player data from year: {year} || || League ID: {league_id} || Game ID: {game_id} ||')
#             players = query.get_league_players()
#             retry = False
#             print(
#                 f' ^^^^^^ Parsed player data from year: {year} || || League ID: {league_id} || Game ID: {game_id} || Retry status is {retry}')
#             time.sleep(300)
#
#         except Exception as e:
#             print(f'!!!!!! Could not parse player data for {year}')
#             retry = True
#             print(f'{e}... sleeping and retrying. Keep going status is {retry}')
#             time.sleep(300)
#     for i in range(len(players)):
#         player_dict = {}
#         player_row = players[i]
#         player_dict['name'] = player_row.name.full
#         player_dict['player_yhid'] = player_row.player_id
#         player_dict['player_key'] = player_row.player_key
#         player_dict['team_code'] = player_row.editorial_team_abbr
#         player_dict['team'] = player_row.editorial_team_full_name
#         player_dict['primary_position'] = player_row.primary_position
#
#         players_unpacked.append(player_dict)
#
#     player_df = pd.DataFrame(players_unpacked)
#     # this is used by the draft / transaction df
#     player_df.to_csv(f'{root_dir}YAHOO_PLAYER_METADATA/Yahoo_Players_{year}.csv', index=False)
#
#
# def close_matchup_wrapper(x, df_data, strtype):
#     try:
#         best_match = difflib.get_close_matches(x, df_data, cutoff=0.75)[0]
#         # print(f'{strtype} found {best_match} for {x}')
#         return best_match
#     except Exception as e:
#         print(f'Could not find a good match for: {x} on {strtype}')
#         return ''
#
#
# def special_case_name_scrubber(df, name_col, filter_col, db_type):
#     # Add special case names that need scrubbing
#     # Either names that cannot easily be matched in fuzzymatching,
#     # or names that are identical (Seb Aho, Elias Petersen)
#     if 'Michael Matheson' in df[name_col].unique() or 'Mike Matheson' in df[name_col].unique():
#         df.loc[(df[name_col] == 'Michael Matheson') | (df[name_col] == 'Mike Matheson'), name_col] = 'Michael Matheson'
#
#     if 'JJ Peterka' in df[name_col].unique() or 'John-Jason Peterka' in df[name_col].unique():
#         df.loc[(df[name_col] == 'JJ Peterka') | (df[name_col] == 'John-Jason Peterka'), name_col] = 'John-Jason Peterka'
#
#     if 'Tommy Novak' in df[name_col].unique() or 'Thomas Novak' in df[name_col].unique():
#         df.loc[(df[name_col] == 'Tommy Novak') | (df[name_col] == 'Thomas Novak'), name_col] = 'Thomas Novak'
#
#     if 'Nicholas Paul' in df[name_col].unique() or 'Nick Paul' in df[name_col].unique():
#         df.loc[(df[name_col] == 'Nick Paul') | (df[name_col] == 'Nicholas Paul'), name_col] = 'Nicholas Nick Paul'
#
#     if 'Alex Wennberg' in df[name_col].unique() or 'Alexander Wennberg' in df[name_col].unique():
#         df.loc[(df[name_col] == 'Alex Wennberg') | (
#                     df[name_col] == 'Alexander Wennberg'), name_col] = 'Alexander Wennberg'
#
#     if 'Alexander Kerfoot' in df[name_col].unique() or 'Alex Kerfoot' in df[name_col].unique():
#         df.loc[(df[name_col] == 'Alex Kerfoot') | (
#                     df[name_col] == 'Alexander Kerfoot'), name_col] = 'Alexander Kerfoot'
#
#     if 'Benoit-Olivier Groulx' in df[name_col].unique() or 'Bo Groulx' in df[name_col].unique():
#         df.loc[(df[name_col] == 'Benoit-Olivier Groulx') | (
#                     df[name_col] == 'Bo Groulx'), name_col] = 'Benoit-Olivier Groulx'
#
#     if 'Sebastian Aho' in df[name_col].unique():
#         df.loc[((df[name_col] == "Sebastian Aho") &
#                 ((df[filter_col] == 'Carolina Hurricanes') |
#                  (df[filter_col] == 'CAR') |
#                  (df[filter_col].str.contains('C')))), name_col] = 'Sebastian Antero Aho'
#
#         df.loc[((df[name_col] == "Sebastian Aho") &
#                 ((df[filter_col] == 'New York Islanders') |
#                  (df[filter_col] == 'NYI') |
#                  (df[filter_col].str.contains('D')))), name_col] = 'Sebastian Johannes Aho'
#
#     # after all the specials are scrubbed, do a little more general scrubbing
#     df['DB'] = db_type
#     df[f'clean_name_{db_type}'] = df[name_col].apply(lambda x: unidecode(x))
#     df[f'clean_name_{db_type}'] = df[f'clean_name_{db_type}'].apply(lambda x: re.sub(r'[^a-zA-Z0-9]', '', x))
#     df[f'clean_name_{db_type}'] = df[f'clean_name_{db_type}'].str.strip()
#     df[f'clean_name_{db_type}'] = df[f'clean_name_{db_type}'].str.lower()
#     return df
#
#
# def clean_player_name_parser(status, yearsToCheck):
#     if status:
#         import os
#         yh_stackframe = pd.DataFrame()
#         nst_stackframe = pd.DataFrame()
#         hr_stackframe = pd.DataFrame()
#         for year in yearsToCheck:
#
#             yh_filelist = os.listdir(f'ONLINE_PARSED_DATA/YH_ROSTERS/{year}')
#             print(f'YH {year} NAMES')
#             for file in yh_filelist:
#                 df = pd.read_csv(f'{root_dir}ONLINE_PARSED_DATA/YH_ROSTERS/{year}/{file}')
#                 df = special_case_name_scrubber(df, 'Name', 'Elligible Positions', 'yh')
#                 df = df.rename(columns={'clean_name_yh': 'Name_clean'})
#                 yh_stackframe = pd.concat([yh_stackframe, df])
#                 yh_stackframe.drop_duplicates('Name', inplace=True)
#                 yh_stackframe = yh_stackframe[['Name', 'DB']]
#             nst_filelist = os.listdir(f'ONLINE_PARSED_DATA/NST/{year}')
#             print(f'NST {year} NAMES')
#             for file in nst_filelist:
#                 df = pd.read_csv(f'{root_dir}ONLINE_PARSED_DATA/NST/{year}/{file}')
#                 df = special_case_name_scrubber(df, 'Player_NST', 'Team_NST', 'nst')
#                 df = df.rename(columns={'Player_NST': 'Name', 'clean_name_nst': 'Name_clean'})
#                 nst_stackframe = pd.concat([nst_stackframe, df])
#                 nst_stackframe.drop_duplicates('Name', inplace=True)
#                 nst_stackframe = nst_stackframe[['Name', 'DB']]
#             hr_filelist = os.listdir(f'ONLINE_PARSED_DATA/HR/{year}')
#             print(f'HR {year} NAMES')
#             for file in hr_filelist:
#                 df = pd.read_csv(f'{root_dir}ONLINE_PARSED_DATA/HR/{year}/{file}')
#                 df = special_case_name_scrubber(df, 'Player_HR', 'Team_HR', 'hr')
#                 df = df.rename(columns={'Player_HR': 'Name', 'clean_name_hr': 'Name_clean'})
#                 hr_stackframe = pd.concat([hr_stackframe, df])
#                 hr_stackframe.drop_duplicates('Name', inplace=True)
#                 hr_stackframe = hr_stackframe[['Name', 'DB']]
#
#         mega_stacked_df = pd.concat([hr_stackframe, nst_stackframe, yh_stackframe])
#         mega_stacked_df['Name_clean'] = mega_stacked_df['Name'].str.lower()
#         mega_stacked_df['Name_clean'] = mega_stacked_df['Name_clean'].apply(lambda x: unidecode(x))
#         mega_stacked_df['Name_clean'] = mega_stacked_df['Name_clean'].apply(lambda x: re.sub(r'[^a-zA-Z0-9]', '', x))
#         mega_stacked_df.sort_values('Name_clean', inplace=True)
#         mega_stacked_df.to_csv(f'{root_dir}Player_Clean_Names.csv', index=False)
#
#
# def parsed_data_stitcher(year, dates_of_interest, control_file):
#     mega_stacked_df = pd.read_csv(f'{root_dir}Player_Clean_Names.csv')
#     if control_file['Stitcher Status']:
#
#         schedule = pd.read_csv(f'{root_dir}NHL_Schedules/{year}_NHL_Schedule.csv')
#         weeks_metadata = pd.read_csv(f'{root_dir}WEEKS_DATA/{year}_week_data.csv')
#         season_start = weeks_metadata['start'].iloc[0]
#         season_end = weeks_metadata[weeks_metadata['week'] == weeks_metadata['week'].max()]['end'].values[0]
#         for date in dates_of_interest:
#             for file_name in glob.glob(f'ONLINE_PARSED_DATA/HR/{year}/' + f'*{date}.csv'):
#                 hr_df = pd.read_csv(root_dir+file_name, low_memory=False)
#                 # date = file_name.split('_')[-1].split('.')[0]
#                 try:
#                     week = schedule[schedule['date'] == date]['week'].values[0]
#                 except Exception as e:
#                     print(f'({e}) Week outside of Yahoo boundary - just take the stats')
#                     week = np.nan
#
#                 hr_df = special_case_name_scrubber(hr_df, name_col='Player_HR', filter_col='Team_HR', db_type='hr')
#                 nst_df = pd.read_csv(f'{root_dir}ONLINE_PARSED_DATA/NST/{year}/NST_stats_{date}.csv')
#                 nst_df = special_case_name_scrubber(nst_df, name_col='Player_NST', filter_col='Team_NST', db_type='nst')
#                 yh_df = pd.read_csv(f'{root_dir}ONLINE_PARSED_DATA/YH_ROSTERS/{year}/YH_stats_{date}_percentage_added.csv')
#                 yh_df = special_case_name_scrubber(yh_df, name_col='Name', filter_col='Elligible Positions',
#                                                    db_type='yh')
#
#                 nst_df['clean_name_nst_converted'] = nst_df['clean_name_nst'].apply(
#                     lambda x: close_matchup_wrapper(x, hr_df['clean_name_hr'], 'NST_TO_HR'))
#
#                 hr_nst_df = hr_df.merge(nst_df, left_on='clean_name_hr', right_on='clean_name_nst_converted')
#                 yh_df['clean_name_yh_converted'] = ''
#                 yh_df['clean_name_yh_converted'] = yh_df['clean_name_yh'].apply(
#                     lambda x: close_matchup_wrapper(x, mega_stacked_df['Name_clean'], 'YAHOO_TO_FULL'))
#
#                 hr_nst_yh_df = hr_nst_df.merge(yh_df, how='left', left_on='clean_name_nst',
#                                                right_on='clean_name_yh_converted')
#                 # data quality stuff here
#
#                 if year < 2014:
#                     hr_nst_yh_df.columns = ['Player_HR', 'G_HR', 'A_HR', 'PTS_HR', '+/-_HR', 'PIM_HR', 'EV_HR', 'PP_HR',
#                                             'SH_HR', 'GW_HR', 'S_HR', 'S%_HR', 'SHFT_HR',
#                                             'TOI_HR', 'Date_HR', 'Team_HR', 'Team_Code_HR', 'iCF_HR', 'SAT‑F_HR',
#                                             'SAT‑A_HR', 'CF%_HR', 'CRel%_HR', 'ZSO_HR', 'ZSD_HR', 'oZS%_HR', 'HIT_HR',
#                                             'BLK_HR', 'DEC_HR', 'GA_HR', 'SA_HR', 'SV_HR', 'SV%_HR', 'SO_HR', 'Week_x',
#                                             'WINS_HR', 'LOSSES_HR', 'DB_x', 'clean_name_hr', 'Player_NST', 'Team_NST',
#                                             'Position_NST', 'GP_NST', 'TOI_NST', 'Goals_NST', 'Total Assists_NST',
#                                             'First Assists_NST', 'Second Assists_NST', 'Total Points_NST', 'IPP_NST',
#                                             'Shots_NST', 'SH%_NST', 'ixG_NST', 'iCF_NST', 'iFF_NST', 'iSCF_NST',
#                                             'iHDCF_NST', 'Rush Attempts_NST', 'Rebounds Created_NST', 'PIM_NST',
#                                             'Total Penalties_NST', 'Minor_NST', 'Major_NST', 'Misconduct_NST',
#                                             'Penalties Drawn_NST', 'Giveaways_NST', 'Takeaways_NST', 'Hits_NST',
#                                             'Hits Taken_NST', 'Shots Blocked_NST', 'Faceoffs Won_NST',
#                                             'Faceoffs Lost_NST',
#                                             'Faceoffs %_NST', 'Shots Against_NST', 'Saves_NST', 'Goals Against_NST',
#                                             'SV%_NST', 'GAA_NST', 'GSAA_NST', 'xG Against_NST', 'HD Shots Against_NST',
#                                             'HD Saves_NST', 'HD Goals Against_NST', 'HDSV%_NST', 'HDGAA_NST',
#                                             'HDGSAA_NST',
#                                             'MD Shots Against_NST', 'MD Saves_NST', 'MD Goals Against_NST', 'MDSV%_NST',
#                                             'MDGAA_NST', 'MDGSAA_NST', 'LD Shots Against_NST', 'LD Saves_NST',
#                                             'LD Goals Against_NST', 'LDSV%_NST', 'LDGAA_NST', 'LDGSAA_NST',
#                                             'Rush Attempts Against_NST', 'Rebound Attempts Against_NST',
#                                             'Avg. Shot Distance_NST', 'Avg. Goal Distance_NST', 'Date_NST', 'Code_NST',
#                                             'GOALIE_TOI', 'DB_y', 'clean_name_nst', 'clean_name_nst_converted', 'Date',
#                                             'Team', 'Name', 'Selected Position', 'Elligible Positions', 'Player Key',
#                                             'Year', 'Week_y','Percentage Owned','Percentage Owned Delta', 'DB', 'clean_name_yh', 'clean_name_yh_converted']
#                     hr_nst_yh_df['PPP_HR'] = hr_nst_yh_df['PP_HR']  # .astype(int)
#                     hr_nst_yh_df['SHP_HR'] = hr_nst_yh_df['SH_HR']  # .astype(int)
#                     hr_nst_yh_df['EVP_HR'] = hr_nst_yh_df['EV_HR']  # astype(int)
#                     # 2013 and earlier has fewer columns in HR... so just drop them
#                     hr_nst_yh_df.drop_duplicates(subset=['Player_HR', 'Player_NST', 'Name'], inplace=True)
#                     # drop the useless / redundant columns
#                     hr_nst_yh_df.drop(['PP_HR',
#
#                                        'SH_HR',
#
#                                        'EV_HR',
#
#                                        'DB',
#                                        'clean_name_yh',
#                                        'clean_name_yh_converted',
#                                        'Week_y',
#                                        'DB_y',
#                                        'clean_name_nst',
#                                        'clean_name_nst_converted',
#                                        'DB_x',
#                                        'clean_name_hr',
#                                        'IPP_NST',
#                                        'GP_NST',
#                                        'Date',
#                                        'Date_HR'
#                                        ], axis=1, inplace=True)
#                 else:
#                     hr_nst_yh_df.columns = ['Player_HR', 'G_HR', 'A_HR', 'PTS_HR', '+/-_HR', 'PIM_HR', 'EV_HR', 'PP_HR',
#                                             'SH_HR', 'GW_HR', 'EV_HR.1', 'PP_HR.1', 'SH_HR.1', 'S_HR', 'S%_HR',
#                                             'SHFT_HR',
#                                             'TOI_HR', 'Date_HR', 'Team_HR', 'Team_Code_HR', 'iCF_HR', 'SAT‑F_HR',
#                                             'SAT‑A_HR', 'CF%_HR', 'CRel%_HR', 'ZSO_HR', 'ZSD_HR', 'oZS%_HR', 'HIT_HR',
#                                             'BLK_HR', 'DEC_HR', 'GA_HR', 'SA_HR', 'SV_HR', 'SV%_HR', 'SO_HR', 'Week_x',
#                                             'WINS_HR', 'LOSSES_HR', 'DB_x', 'clean_name_hr', 'Player_NST', 'Team_NST',
#                                             'Position_NST', 'GP_NST', 'TOI_NST', 'Goals_NST', 'Total Assists_NST',
#                                             'First Assists_NST', 'Second Assists_NST', 'Total Points_NST', 'IPP_NST',
#                                             'Shots_NST', 'SH%_NST', 'ixG_NST', 'iCF_NST', 'iFF_NST', 'iSCF_NST',
#                                             'iHDCF_NST', 'Rush Attempts_NST', 'Rebounds Created_NST', 'PIM_NST',
#                                             'Total Penalties_NST', 'Minor_NST', 'Major_NST', 'Misconduct_NST',
#                                             'Penalties Drawn_NST', 'Giveaways_NST', 'Takeaways_NST', 'Hits_NST',
#                                             'Hits Taken_NST', 'Shots Blocked_NST', 'Faceoffs Won_NST',
#                                             'Faceoffs Lost_NST',
#                                             'Faceoffs %_NST', 'Shots Against_NST', 'Saves_NST', 'Goals Against_NST',
#                                             'SV%_NST', 'GAA_NST', 'GSAA_NST', 'xG Against_NST', 'HD Shots Against_NST',
#                                             'HD Saves_NST', 'HD Goals Against_NST', 'HDSV%_NST', 'HDGAA_NST',
#                                             'HDGSAA_NST',
#                                             'MD Shots Against_NST', 'MD Saves_NST', 'MD Goals Against_NST', 'MDSV%_NST',
#                                             'MDGAA_NST', 'MDGSAA_NST', 'LD Shots Against_NST', 'LD Saves_NST',
#                                             'LD Goals Against_NST', 'LDSV%_NST', 'LDGAA_NST', 'LDGSAA_NST',
#                                             'Rush Attempts Against_NST', 'Rebound Attempts Against_NST',
#                                             'Avg. Shot Distance_NST', 'Avg. Goal Distance_NST', 'Date_NST', 'Code_NST',
#                                             'GOALIE_TOI', 'DB_y', 'clean_name_nst', 'clean_name_nst_converted', 'Date',
#                                             'Team', 'Name', 'Selected Position', 'Elligible Positions', 'Player Key',
#                                             'Year', 'Week_y','Percentage Owned','Percentage Owned Delta', 'DB', 'clean_name_yh', 'clean_name_yh_converted']
#
#                     hr_nst_yh_df['PPP_HR'] = hr_nst_yh_df['PP_HR'].astype(int) + hr_nst_yh_df['PP_HR.1'].astype(int)
#                     hr_nst_yh_df['SHP_HR'] = hr_nst_yh_df['SH_HR'].astype(int) + hr_nst_yh_df['SH_HR.1'].astype(int)
#                     hr_nst_yh_df['EVP_HR'] = hr_nst_yh_df['EV_HR'].astype(int) + hr_nst_yh_df['EV_HR.1'].astype(int)
#                     hr_nst_yh_df.drop_duplicates(subset=['Player_HR', 'Player_NST', 'Name'], inplace=True)
#                     # drop the useless / redundant columns
#                     hr_nst_yh_df.drop(['PP_HR',
#                                        'PP_HR.1',
#                                        'SH_HR',
#                                        'SH_HR.1',
#                                        'EV_HR',
#                                        'EV_HR.1',
#                                        'DB',
#                                        'clean_name_yh',
#                                        'clean_name_yh_converted',
#                                        'Week_y',
#                                        'DB_y',
#                                        'clean_name_nst',
#                                        'clean_name_nst_converted',
#                                        'DB_x',
#                                        'clean_name_hr',
#                                        'IPP_NST',
#                                        'GP_NST',
#                                        'Date',
#                                        'Date_HR'
#                                        ], axis=1, inplace=True)
#
#                 hr_nst_yh_df.rename(columns={'Week_x': "Week", "Team": "Team_Yahoo", "Name": "Player_Yahoo"},
#                                     inplace=True)
#                 # reorganize columns to be more sensical
#                 hr_nst_yh_df['Game_Number'] = np.nan
#                 for row in range(0, len(hr_nst_yh_df)):
#                     team_hr = hr_nst_yh_df.iloc[row]['Team_HR']
#                     date_nst = hr_nst_yh_df.iloc[row]['Date_NST']
#
#                     if team_hr in schedule[(schedule['date'] == date_nst) & ((schedule['home'] == team_hr))][
#                         'home'].unique():
#                         hr_nst_yh_df.loc[row, 'Game_Number'] = \
#                             schedule[(schedule['date'] == date_nst) & ((schedule['home'] == team_hr))][
#                                 'home_count'].values[
#                                 0]
#                     elif team_hr in schedule[(schedule['date'] == date_nst) & ((schedule['away'] == team_hr))][
#                         'away'].unique():
#                         hr_nst_yh_df.loc[row, 'Game_Number'] = \
#                             schedule[(schedule['date'] == date_nst) & ((schedule['away'] == team_hr))][
#                                 'away_count'].values[
#                                 0]
#                     else:
#                         print(f'could not find game day')
#                 hr_nst_yh_df = hr_nst_yh_df[
#                     [
#                         'Year',
#                         'Week',
#                         'Date_NST',
#                         'Player_HR',
#                         'Player_NST',
#                         'Player_Yahoo',
#                         'Team_HR',
#                         'Team_NST',
#                         'Team_Code_HR',
#                         'Code_NST',
#                         'Game_Number',
#                         'Position_NST',
#                         'Team_Yahoo',
#                         'Selected Position',
#                         'Elligible Positions',
#                         'Percentage Owned',
#                         'Percentage Owned Delta',
#                         'Player Key',
#                         'TOI_NST',
#                         'TOI_HR',
#                         'GOALIE_TOI',
#                         'SHFT_HR',
#                         'G_HR',
#                         'Goals_NST',
#                         'A_HR',
#                         'Total Assists_NST',
#                         'First Assists_NST',
#                         'Second Assists_NST',
#                         'PTS_HR',
#                         'Total Points_NST',
#                         'PPP_HR',
#                         'SHP_HR',
#                         'EVP_HR',
#                         '+/-_HR',
#                         'PIM_HR',
#                         'PIM_NST',
#                         'Total Penalties_NST',
#                         'Minor_NST',
#                         'Major_NST',
#                         'Misconduct_NST',
#                         'Penalties Drawn_NST',
#                         'GW_HR',
#                         'S_HR',
#                         'Shots_NST',
#                         'S%_HR',
#                         'SH%_NST',
#                         'Giveaways_NST',
#                         'Takeaways_NST',
#                         'Faceoffs Won_NST',
#                         'Faceoffs Lost_NST',
#                         'Faceoffs %_NST',
#                         'Rush Attempts_NST',
#                         'Rebounds Created_NST',
#                         'HIT_HR',
#                         'Hits_NST',
#                         'Hits Taken_NST',
#                         'BLK_HR',
#                         'Shots Blocked_NST',
#                         'ixG_NST',
#                         'iCF_NST',
#                         'iCF_HR',
#                         'CF%_HR',
#                         'CRel%_HR',
#                         'SAT‑F_HR',
#                         'SAT‑A_HR',
#                         'iFF_NST',
#                         'iSCF_NST',
#                         'iHDCF_NST',
#                         'ZSO_HR',
#                         'ZSD_HR',
#                         'oZS%_HR',
#                         'DEC_HR',
#                         'WINS_HR',
#                         'LOSSES_HR',
#                         'GA_HR',
#                         'Goals Against_NST',
#                         'SA_HR',
#                         'Shots Against_NST',
#                         'SV_HR',
#                         'Saves_NST',
#                         'SV%_HR',
#                         'SV%_NST',
#                         'SO_HR',
#                         'GAA_NST',
#                         'GSAA_NST',
#                         'xG Against_NST',
#                         'HD Shots Against_NST',
#                         'HD Saves_NST',
#                         'HD Goals Against_NST',
#                         'HDSV%_NST',
#                         'HDGAA_NST',
#                         'HDGSAA_NST',
#                         'MD Shots Against_NST',
#                         'MD Saves_NST',
#                         'MD Goals Against_NST',
#                         'MDSV%_NST',
#                         'MDGAA_NST',
#                         'MDGSAA_NST',
#                         'LD Shots Against_NST',
#                         'LD Saves_NST',
#                         'LD Goals Against_NST',
#                         'LDSV%_NST',
#                         'LDGAA_NST',
#                         'LDGSAA_NST',
#                         'Rush Attempts Against_NST',
#                         'Rebound Attempts Against_NST',
#                         'Avg. Shot Distance_NST',
#                         'Avg. Goal Distance_NST'
#                     ]]
#                 # add a name deduplicator here
#                 hr_nst_yh_df.fillna({'Year': year}, inplace=True)
#                 hr_nst_yh_df.fillna({'Team_Yahoo': 'Free Agency'}, inplace=True)
#                 print(
#                     f'>>>> [Rundate: {time.ctime()}] Players NST: {len(nst_df)} Players HR: {len(hr_df)} Total Stitched: {len(hr_nst_yh_df)} [{date}]')
#                 hr_nst_yh_df.to_csv(f'{root_dir}STITCHED_PARSED_DATA/HRNSTYH_{year}_{week}_{date}.csv', index=False)
#
#
# def fp_calculator(year, dates_of_interest, control_file):
#     stat_data = control_file['Fantasy Points']['fp_calcs']
#     schedule = pd.read_csv(f'{root_dir}NHL_Schedules/{year}_NHL_Schedule.csv')
#     if control_file['Fantasy Points']['fp_calculator_status']:
#
#         print(f'>>>> [Rundate: {time.ctime()}] Calculating roster-day FPs for {year} season files')
#         # stitched_file_list = os.listdir(f'STITCHED_PARSED_DATA')
#         for date in dates_of_interest:
#
#             try:
#                 week = schedule[schedule['date'] == date]['week'].values[0]
#             except Exception as e:
#                 # no games on this date - skip
#                 continue
#
#             df = pd.read_csv(f'{root_dir}STITCHED_PARSED_DATA/HRNSTYH_{year}_{week}_{date}.csv')
#             if df['DEC_HR'].str.contains('\\', regex=False).any():
#                 print('We have a / goalie pull')
#                 df['DEC_HR'] = df['DEC_HR'].str.replace(r'\\', '', regex=True).replace(r'^\s*$', np.nan, regex=True)
#                 df['WINS_HR'] = df['WINS_HR'].str.replace(r'\\', '', regex=True).replace(r'^\s*$', np.nan, regex=True)
#                 df['LOSSES_HR'] = df['LOSSES_HR'].str.replace(r'\\', '', regex=True).replace(r'^\s*$', np.nan,
#                                                                                              regex=True)
#             fp_total = 0
#             for stat in stat_data.keys():
#                 value = df[stat].fillna(0).astype(float) * stat_data[stat]  # fill in any blanks
#                 df[f'FP_{stat}'] = value
#                 fp_total += value
#             df['FP_TOTAL'] = fp_total
#             # Tag the starts for aggregation later
#             df['SKATER_START'] = 0
#             df.loc[((df['Selected Position'] == 'LW') |
#                     (df['Selected Position'] == 'RW') |
#                     (df['Selected Position'] == 'D') |
#                     (df['Selected Position'] == 'C') |
#                     (df['Selected Position'] == 'Util')), 'SKATER_START'] = 1
#
#             df['GOALIE_START'] = 0
#             df.loc[((df['Selected Position'] == 'G')), 'GOALIE_START'] = 1
#
#             df['BENCHED_START'] = 0
#             df.loc[((df['Selected Position'] == 'BN') | (df['Selected Position'] == 'IR+')), 'BENCHED_START'] = 1
#
#             df['FORGOTTEN_START'] = np.nan
#             # Check for missed starts
#             teams_df = pd.read_csv(f'{root_dir}TEAMS_METADATA/{year}_teams.csv')
#             schedule_df = pd.read_csv(f'{root_dir}MATCHUPS_METADATA/{year}_matchups_metadata.csv')
#             for team in df['Team_Yahoo'].unique():
#
#                 if team == 'Free Agency':
#                     pass
#                 else:
#                     #                             team_gm = teams_df[teams_df['Team_Name']==team]['GM_Name'].values[0]
#                     #                             df['GM'] = ''
#                     #                             df.loc[(df['Team_Yahoo'] == team), 'GM'] = team_gm
#                     team_bench_df = df[(df['Team_Yahoo'] == team) & (df['Selected Position'] == 'BN')]
#                     c_bench = (len(df[(df['Team_Yahoo'] == team) & (df['Selected Position'] == 'BN') & (
#                         df['Elligible Positions'].str.contains('C'))]) > 0)
#                     d_bench = (len(df[(df['Team_Yahoo'] == team) & (df['Selected Position'] == 'BN') & (
#                         df['Elligible Positions'].str.contains('D'))]) > 0)
#                     rw_bench = (len(df[(df['Team_Yahoo'] == team) & (df['Selected Position'] == 'BN') & (
#                         df['Elligible Positions'].str.contains('RW'))]) > 0)
#                     lw_bench = (len(df[(df['Team_Yahoo'] == team) & (df['Selected Position'] == 'BN') & (
#                         df['Elligible Positions'].str.contains('LW'))]) > 0)
#                     g_bench = (len(df[(df['Team_Yahoo'] == team) & (df['Selected Position'] == 'BN') & (
#                         df['Elligible Positions'].str.contains('G'))]) > 0)
#                     c_open = (len(df[(df['Team_Yahoo'] == team) & (df['Selected Position'] == 'C')]) < 2)
#                     g_open = (len(df[(df['Team_Yahoo'] == team) & (df['Selected Position'] == 'G')]) < 2)
#                     lw_open = (len(df[(df['Team_Yahoo'] == team) & (df['Selected Position'] == 'LW')]) < 2)
#                     rw_open = (len(df[(df['Team_Yahoo'] == team) & (df['Selected Position'] == 'RW')]) < 2)
#                     d_open = (len(df[(df['Team_Yahoo'] == team) & (df['Selected Position'] == 'D')]) < 4)
#                     util_open = (len(df[(df['Team_Yahoo'] == team) & (df['Selected Position'] == 'Util')]) < 1)
#                     date = df['Date_NST'].iloc[0]
#                     # print(f"{date} {team}: Cbench {c_bench} c_open {c_open} dbench {d_bench} d_open {d_open} rwbench {rw_bench} rw_open {rw_open} lwbench { lw_bench} lw_open {lw_open} gbench {g_bench} g_open {g_open}")
#                     if len(team_bench_df) > 0:
#                         if c_bench and (
#                                 c_open or util_open):  # ie, we have a centre on the bench and there was a slot open in C or UTIL
#                             df.loc[(df['Team_Yahoo'] == team) & (df['Selected Position'] == 'BN') & (
#                                 df['Elligible Positions'].str.contains('C')), 'FORGOTTEN_START'] = 1
#
#                         if lw_bench and (
#                                 lw_open or util_open):  # ie, we have a lw on the bench and there was a slot open in LW or UTIL
#                             df.loc[(df['Team_Yahoo'] == team) & (df['Selected Position'] == 'BN') & (
#                                 df['Elligible Positions'].str.contains('LW')), 'FORGOTTEN_START'] = 1
#
#                         if rw_bench and (
#                                 rw_open or util_open):  # ie, we have a rw on the bench and there was a slot open in rw or UTIL
#                             df.loc[(df['Team_Yahoo'] == team) & (df['Selected Position'] == 'BN') & (
#                                 df['Elligible Positions'].str.contains('RW')), 'FORGOTTEN_START'] = 1
#
#                         if d_bench and (
#                                 d_open or util_open):  # ie, we have a d on the bench and there was a slot open in D or UTIL
#                             df.loc[(df['Team_Yahoo'] == team) & (df['Selected Position'] == 'BN') & (
#                                 df['Elligible Positions'].str.contains('D')), 'FORGOTTEN_START'] = 1
#
#             df = df.merge(teams_df[['Team_Name', 'GM_Name']], how='left', left_on='Team_Yahoo', right_on='Team_Name')
#             df = df.merge(schedule_df[['Year', 'Week', 'Team', 'Playoffs']], how='left',
#                           left_on=['Year', 'Week', 'Team_Name'], right_on=['Year', 'Week', 'Team'])
#             df.to_csv(f'{root_dir}CALCULATED_DATA/CALCS_{year}_{week}_{date}.csv', index=False)
#             print(
#                 f'>>>> [Rundate: {time.ctime()}] Successfully wrote out CALCS_{year}_{week}_{date}.csv file')
#
#
# def check_if_date_in_week(trans_time, df_weeks):
#     for week_row in range(0, len(df_weeks)):
#
#         start_date = datetime.strptime(df_weeks['start'].iloc[week_row], '%Y-%m-%d')
#         end_date = datetime.strptime(df_weeks['end'].iloc[week_row], '%Y-%m-%d') + timedelta(hours=23, minutes=59,
#                                                                                              seconds=59)
#
#         if ((trans_time >= start_date) & (trans_time <= end_date)):
#             week = df_weeks.iloc[week_row]['week']
#
#             return week
#         else:
#             continue
#     return 0
#
#
# def trans_and_draft(year, control_file):
#     df_weeks = pd.read_csv(f'{root_dir}WEEKS_DATA/{year}_week_data.csv')
#     df_teams = pd.read_csv(f'{root_dir}TEAMS_METADATA/{year}_teams.csv')
#     df_players = pd.read_csv(f'{root_dir}YAHOO_PLAYER_METADATA/Yahoo_Players_{year}.csv')
#     league_id = control_file['Years'][str(year)]['league_id']
#     game_id = control_file['Years'][str(year)]['game_id']
#
#     query = yfpy.YahooFantasySportsQuery(auth_dir='C:/Users/16472/PycharmProjects/Hockey_FantaPy/private',
#                                          league_id=str(league_id),
#                                          game_id=str(game_id),
#                                          game_code='nhl'
#                                          # all_output_as_json=False
#                                          )
#     week = 0
#
#     if control_file['Transaction Parser Status']:
#         print(f'>>>> [Rundate: {time.ctime()}] Deriving transactions for {year}:{league_id}:{game_id}')
#         df_trans = pd.DataFrame(columns=['season',
#                                          'week',
#                                          'transaction_date',
#                                          'transaction_type',
#                                          'transaction_id',
#                                          'status',
#                                          'player_id',
#                                          'name',
#                                          'draft_round',
#                                          'faab_bid',
#                                          'source',
#                                          'source_key',
#                                          'destination',
#                                          'destination_key',
#                                          'waiver',
#                                          'keeper'])
#
#         transactions = query.get_league_transactions()
#
#         for i in range(0, len(transactions)):
#
#             trans = transactions[i]  # ['transaction']
#             trans_type = trans.type
#             try:
#                 faab_bid = trans.faab_bid
#             except:
#                 faab_bid = np.nan
#             if trans_type == 'add':
#                 trans_status = trans.status
#                 trans_time = datetime.fromtimestamp(trans.timestamp)
#                 week = check_if_date_in_week(trans_time, df_weeks)
#
#                 trans_id = str(year) + "_" + str(week) + "_Trans_" + str(trans.transaction_id)
#                 player_id = trans.players[0].player_key
#                 player_name = trans.players[0].name.full
#
#                 destination = trans.players[0].transaction_data.destination_team_name
#                 destination_key = trans.players[0].transaction_data.destination_team_key
#                 destination_type = trans.players[0].transaction_data.destination_type
#                 source = 'Free Agency'
#                 source_key = '99.l.99.t.99'
#                 source_type = trans.players[0].transaction_data.source_type
#                 waiver_check = 'YES' if source_type == 'waivers' else 'NO'
#                 df_trans.loc[len(df_trans)] = (year, week, trans_time, trans_type,
#                                                trans_id, trans_status, player_id,
#                                                player_name, '', faab_bid, source, source_key,
#                                                destination, destination_key, waiver_check, '')
#
#
#             elif trans_type == 'drop':
#                 trans_status = trans.status
#                 trans_time = datetime.fromtimestamp(trans.timestamp)
#                 week = check_if_date_in_week(trans_time, df_weeks)
#                 trans_id = str(year) + "_" + str(week) + "_Trans_" + str(trans.transaction_id)
#                 player_id = trans.players[0].player_key
#                 player_name = trans.players[0].name.full
#
#                 source = trans.players[0].transaction_data.source_team_name
#                 source_key = trans.players[0].transaction_data.source_team_key
#                 source_type = trans.players[0].transaction_data.source_type
#                 waiver_check = 'YES' if source_type == 'waivers' else 'NO'
#                 destination = 'Free Agency'
#                 destination_key = '99.l.99.t.99'
#                 destination_type = trans.players[0].transaction_data.destination_type
#                 df_trans.loc[len(df_trans)] = (year, week, trans_time, trans_type,
#                                                trans_id, trans_status, player_id,
#                                                player_name, '', '', source, source_key,
#                                                destination, destination_key, waiver_check, '')
#
#
#
#             elif trans_type == 'add/drop':
#                 try:
#                     faab_bid = trans.faab_bid
#                 except:
#                     faab_bid = np.nan
#                 trans_status = trans.status
#                 trans_time = datetime.fromtimestamp(trans.timestamp)
#                 week = check_if_date_in_week(trans_time, df_weeks)
#                 trans_id = str(year) + "_" + str(week) + "_Trans_" + str(trans.transaction_id)
#
#                 add = trans.players[0]
#                 player_id = add.player_key
#                 player_name = add.name.full
#                 # add portion
#                 destination = add.transaction_data.destination_team_name
#                 destination_key = add.transaction_data.destination_team_key
#                 destination_type = add.transaction_data.destination_type
#                 source = 'Free Agency'
#                 source_key = '99.l.99.t.99'
#                 source_type = add.transaction_data.source_type
#                 waiver_check = 'YES' if source_type == 'waivers' else 'NO'
#                 trans_type = 'add'
#                 df_trans.loc[len(df_trans)] = (year, week, trans_time, trans_type,
#                                                trans_id, trans_status, player_id,
#                                                player_name, '', faab_bid, source, source_key,
#                                                destination, destination_key, waiver_check, '')
#
#                 # drop portion
#                 drop = trans.players[1]
#                 player_id = drop.player_key
#                 player_name = drop.name.full
#
#                 source = drop.transaction_data.source_team_name
#                 source_key = drop.transaction_data.source_team_key
#                 source_type = drop.transaction_data.source_type
#                 waiver_check = 'YES' if source_type == 'waivers' else 'NO'
#                 destination = 'Free Agency'
#                 destination_key = '99.l.99.t.99'
#                 destination_type = drop.transaction_data.destination_type
#                 trans_type = 'drop'
#                 df_trans.loc[len(df_trans)] = (year, week, trans_time, trans_type,
#                                                trans_id, trans_status, player_id,
#                                                player_name, '', '', source, source_key,
#                                                destination, destination_key, waiver_check, '')
#
#
#
#
#             elif trans_type == 'trade':
#                 trans_status = trans.status
#                 trans_time = datetime.fromtimestamp(trans.timestamp)
#                 week = check_if_date_in_week(trans_time, df_weeks)
#                 trans_id = str(year) + "_" + str(week) + "_Trans_" + str(trans.transaction_id)
#                 if len(trans.picks) > 0:
#                     # There will always be an even amount of picks
#                     for pck in range(0, len(trans.picks)):
#                         pick = trans.picks[pck]
#                         player_id = '999.p.9999'
#                         og_team_name = pick.original_team_name
#                         og_gm_name = df_teams[df_teams['Team_Name'] == og_team_name]['GM_Name'].values[0]
#                         if year == 2021:
#                             if og_gm_name == 'Kristofer':
#                                 og_gm_name = 'Mack'
#                             if og_gm_name == 'Cole':
#                                 og_gm_name = 'Taylor'
#                             if og_gm_name == 'Tingle':
#                                 og_gm_name = 'Nigel'
#                         player_name = str(year + 1) + " " + og_gm_name + " " + "Round " + str(
#                             pick.round) + " Draft Pick"
#                         draft_round = pick.round
#                         source = pick.source_team_name
#                         source_key = pick.source_team_key
#                         destination = pick.destination_team_name
#                         destination_key = pick.destination_team_key
#                         waiver_check = 'NO'
#                         df_trans.loc[len(df_trans)] = (year, week, trans_time, trans_type,
#                                                        trans_id, trans_status, player_id,
#                                                        player_name, draft_round, '', source, source_key,
#                                                        destination, destination_key, waiver_check, '')
#
#                 if trans.players == None:
#                     continue  # this is likely a stupid 17 for 17 trade
#                 elif len(trans.players) == 0:
#                     continue  # this is likely a stupid 17 for 17 trade
#                 elif len(trans.players) == 1:
#                     player = trans.players[0]  # ['player']
#                     player_id = player.player_key
#                     player_name = player.name.full
#                     destination = player.transaction_data.destination_team_name
#                     destination_key = player.transaction_data.destination_team_key
#                     destination_type = player.transaction_data.destination_type
#                     source = player.transaction_data.source_team_name
#                     source_key = player.transaction_data.source_team_key
#                     source_type = player.transaction_data.source_type
#                     waiver_check = 'YES' if source_type == 'waivers' else 'NO'
#                     df_trans.loc[len(df_trans)] = (year, week, trans_time, trans_type,
#                                                    trans_id, trans_status, player_id,
#                                                    player_name, '', '', source, source_key,
#                                                    destination, destination_key, waiver_check, '')
#                 else:
#                     for plr in range(0, len(trans.players)):
#                         player = trans.players[plr]  # ['player']
#                         player_id = player.player_key
#                         player_name = player.name.full
#
#                         destination = player.transaction_data.destination_team_name
#                         destination_key = player.transaction_data.destination_team_key
#                         destination_type = player.transaction_data.destination_type
#                         source = player.transaction_data.source_team_name
#                         source_key = player.transaction_data.source_team_key
#                         source_type = player.transaction_data.source_type
#                         waiver_check = 'YES' if source_type == 'waivers' else 'NO'
#                         df_trans.loc[len(df_trans)] = (year, week, trans_time, trans_type,
#                                                        trans_id, trans_status, player_id,
#                                                        player_name, '', '', source, source_key,
#                                                        destination, destination_key, waiver_check, '')
#             else:  # this is commish -> i don't know what to do with these
#                 trans_status = trans.status
#                 trans_time = datetime.fromtimestamp(trans.timestamp)
#
#                 trans_id = str(year) + "_" + str(week) + "_" + str(trans.transaction_id)
#         teams_df = pd.read_csv(f'{root_dir}TEAMS_METADATA/{year}_teams.csv')
#         df_trans = df_trans.merge(teams_df[['Team_Name', 'GM_Name']], how='left', left_on='destination',
#                                   right_on='Team_Name')
#         df_trans = df_trans.merge(teams_df[['Team_Name', 'GM_Name']], how='left', left_on='source',
#                                   right_on='Team_Name', suffixes=('_destination', '_source'))
#         df_trans.drop(['Team_Name_destination', 'Team_Name_source'], axis=1, inplace=True)
#
#         df_trans.to_csv(f'{root_dir}TRANSACTIONS/{year}_transactions.csv', index=False)
#
#     if control_file['Draft Parser Status']:
#         df_draft = pd.DataFrame(columns=['season',
#                                          'week',
#                                          'transaction_date',
#                                          'transaction_type',
#                                          'transaction_id',
#                                          'status',
#                                          'player_id',
#                                          'name',
#                                          'draft_round',
#                                          'faab_bid',
#                                          'source',
#                                          'source_key',
#                                          'destination',
#                                          'destination_key',
#                                          'waiver',
#                                          'keeper'])
#
#         print(f'>>>> [Rundate: {time.ctime()}] Deriving draft for {year}:{league_id}:{game_id}')
#         draft = query.get_league_draft_results()
#         for drft in range(0, len(draft)):
#             draft_pick = draft[drft]  # ['draft_result']
#             draft_time = control_file['keepers'][str(year)]['draft_date']
#             draft_type = 'draft'
#             pick_number = draft_pick.pick
#             pick_round = draft_pick.round
#             player_id = draft_pick.player_key
#             player_name = df_players[df_players['Player_Key'] == player_id]['Name'].values[0]
#             if player_name == 'Sebastian Aho':
#                 player_name = 'Sebastian Antero Aho'
#             destination_key = draft_pick.team_key
#             destination = df_teams[df_teams['Team_Key'] == destination_key]['Team_Name'].values[0]
#             source_key = '99.l.99.t.99'
#             source = 'Free Agency'
#             draft_id = str(year) + "_0_Draft_" + str(pick_number)
#             draft_status = 'successful'
#             waiver_check = ''
#             print(f"Checking if {player_id} is in {control_file['keepers'][str(year)][destination_key]}")
#             try:
#
#                 keeper_check = 'KEEPER' if player_name in control_file['keepers'][str(year)][destination_key] else 'NO'
#             except:
#                 # for years 2017 and back
#                 keeper_check = 'NO'
#             df_draft.loc[len(df_draft)] = (year, week, draft_time, draft_type,
#                                            draft_id, draft_status, player_id,
#                                            player_name, pick_round, '', source, source_key,
#                                            destination, destination_key, waiver_check, keeper_check)
#
#             # Diagnostic
#         # print(df_draft[df_draft['keeper']=='KEEPER'].groupby(['destination'])['keeper'].count())
#         teams_df = pd.read_csv(f'{root_dir}TEAMS_METADATA/{year}_teams.csv')
#         df_draft = df_draft.merge(teams_df[['Team_Name', 'GM_Name']], how='left', left_on='destination',
#                                   right_on='Team_Name')
#         df_draft = df_draft.merge(teams_df[['Team_Name', 'GM_Name']], how='left', left_on='source',
#                                   right_on='Team_Name', suffixes=('_destination', '_source'))
#         df_draft.drop(['Team_Name_destination', 'Team_Name_source'], axis=1, inplace=True)
#
#         df_draft.to_csv(f'{root_dir}DRAFT/{year}_draft.csv', index=False)
#
#
# def player_metadata_parser(year, control_file):
#     if control_file['Yahoo Player Metadata Status']:
#
#         players_unpacked = []
#         df_weeks = pd.read_csv(f'{root_dir}WEEKS_DATA/{year}_week_data.csv')
#         df_teams = pd.read_csv(f'{root_dir}TEAMS_METADATA/{year}_teams.csv')
#
#         league_id = control_file['Years'][str(year)]['league_id']
#         game_id = control_file['Years'][str(year)]['game_id']
#         print(f'Deriving yahoo player metadata for {year}:{league_id}:{game_id}')
#         query = yfpy.YahooFantasySportsQuery(auth_dir='C:/Users/16472/PycharmProjects/Hockey_FantaPy/private',
#                                              league_id=str(league_id),
#                                              game_id=str(game_id),
#                                              game_code='nhl'
#                                              # all_output_as_json=False
#                                              )
#
#         retry = True  # just for the initialization
#         while retry:
#             try:
#                 print(
#                     f' ..... Attempting to parse player data from year: {year} || || League ID: {league_id} || Game ID: {game_id} ||')
#                 players = query.get_league_players()
#                 retry = False
#                 print(
#                     f' ^^^^^^ Parsed player data from year: {year} || || League ID: {league_id} || Game ID: {game_id} || Retry status is {retry}')
#                 # time.sleep(300)
#
#             except Exception as e:
#                 print(f'!!!!!! Could not parse player data for {year}')
#                 retry = True
#                 print(f'{e}... sleeping and retrying. Keep going status is {retry}')
#                 time.sleep(300)
#         print(f'Successfully pulled yahoo player metadata for {year}:{league_id}:{game_id}')
#         player_df = pd.DataFrame(columns=['Name', 'Player_Key', 'NHL_Team', 'Primary_Position', 'Headshot_URL'])
#         for i in range(len(players)):
#             try:
#                 player_row = players[i]
#                 name = player_row.name.full
#                 player_yhid = player_row.player_id
#                 player_key = player_row.player_key
#                 team_code = player_row.editorial_team_abbr
#                 team = player_row.editorial_team_full_name
#                 primary_position = player_row.primary_position
#                 headshot_url = player_row.image_url
#             except:
#                 player_row = players[i]['player']
#                 name = player_row.name.full
#                 player_yhid = player_row.player_id
#                 player_key = player_row.player_key
#                 team_code = player_row.editorial_team_abbr
#                 team = player_row.editorial_team_full_name
#                 primary_position = player_row.primary_position
#                 headshot_url = player_row.image_url
#             player_df.loc[len(player_df)] = (name, player_key, team, primary_position, headshot_url)
#
#         print(f'Successfully dumped yahoo player metadata for {year}:{league_id}:{game_id}')
#
#         # this is used by the draft / transaction df
#         player_df.to_csv(f'{root_dir}YAHOO_PLAYER_METADATA/Yahoo_Players_{year}.csv', index=False)
#
#
# def fake_sql_database_creator(year, dates_of_interest, control_file):
#     # this function consolidates all the calculated files, transaction + draft files,  into "databases"
#     # god forgive me for the sins I am about to commit
#     if control_file['Fake Database Status']:
#         schedule = pd.read_csv(f'{root_dir}NHL_Schedules/{year}_NHL_Schedule.csv')
#         print(f'>>>> [Rundate: {time.ctime()}] Beginning {year} Roster-Stat database update...')
#         try:
#             original_df = pd.read_csv(f'{root_dir}DATABASES/Parkdale_Fantasy_Hockey_Rosters_and_Calcs.csv', low_memory=False)
#         except:
#             original_df = pd.DataFrame()
#
#         # original_df = pd.DataFrame()
#         # for chunk in pd.read_csv(f'{root_dir}DATABASES/Parkdale_Fantasy_Hockey_Rosters_and_Calcs.csv', low_memory=False):
#         #     original_df = pd.concat([original_df, chunk], ignore_index=True)
#
#         # go for the calculated first
#         glued_calc_data = pd.DataFrame()
#         print(f'>>>> [Rundate: {time.ctime()}] Pre drop: {len(original_df)}')
#         print(f'We are going to truncate all results from: {dates_of_interest[0]} to {dates_of_interest[-1]}')
#         for date in dates_of_interest:
#             # drop the existing rows of data
#             if len(original_df) > 0:
#                 original_df = original_df[original_df['Date_NST'] != date]
#             else:
#                 pass
#             try:
#                 week = schedule[schedule['date'] == date]['week'].values[0]
#             except Exception as e:
#                 # no games on this date - skip
#                 continue
#             print(f'looking for: CALCS_{year}_{week}_{date}.csv')
#             date_df = pd.read_csv(f'{root_dir}CALCULATED_DATA/CALCS_{year}_{week}_{date}.csv', low_memory=False)
#             glued_calc_data = pd.concat([glued_calc_data, date_df], axis=0)
#
#             glued_calc_data.sort_values('Date_NST', inplace=True)
#         print(f'>>>> [Rundate: {time.ctime()}] Post drop: {len(original_df)}')
#         print(f'>>>> [Rundate: {time.ctime()}] Total number of new rows to add: {len(glued_calc_data)}')
#         original_df = pd.concat([original_df, glued_calc_data], axis=0)
#         print(f'>>>> [Rundate: {time.ctime()}] Total number of rows after adding: {len(original_df)}')
#         original_df.to_csv(f'{root_dir}DATABASES/Parkdale_Fantasy_Hockey_Rosters_and_Calcs.csv', index=False)
#         print(f'>>>> [Rundate: {time.ctime()}] Roster-Stat database update complete!')
#
#         glued_moves_data = pd.DataFrame()
#         for file_name in glob.glob(f'{root_dir}TRANSACTIONS/' + '*.csv'):
#             x = pd.read_csv(file_name, low_memory=False)
#             glued_moves_data = pd.concat([glued_moves_data, x], axis=0)
#         for file_name in glob.glob(f'{root_dir}DRAFT/' + '*.csv'):
#             x = pd.read_csv(file_name, low_memory=False)
#             glued_moves_data = pd.concat([glued_moves_data, x], axis=0)
#         print(glued_moves_data.columns.to_list())
#         glued_moves_data.sort_values('transaction_date', inplace=True)
#         glued_moves_data.to_csv(f'{root_dir}DATABASES/Parkdale_Fantasy_Hockey_Transactions_and_Drafts.csv', index=False)
#         time_c = time.time()
#         print(f'>>>> [Rundate: {time.ctime()}] Other database updates complete!')
#
#     else:
#         print(f'>>>> [Rundate: {time.ctime()}] Not updating the "databases"')




#
# # Draft Analytics
# def draft_analytics(year, control_file):
#     if control_file['draft_analytics_status']:
#         print(f'>>>> [Rundate: {time.ctime()}] Beginning draft analytics for {year}...')
#
#         df_rosters_full = pd.read_csv(f'{root_dir}DATABASES/Parkdale_Fantasy_Hockey_Rosters_and_Calcs.csv', low_memory=False)
#         df_rosters_full = df_rosters_full[(df_rosters_full['Selected Position'] != 'BN') &
#                                           (df_rosters_full['Selected Position'] != 'IR+') &
#                                           (df_rosters_full['Selected Position'] != 'IR') &
#                                           (df_rosters_full['Playoffs'] == 0)]
#         df_all_transactions = pd.read_csv(f'{root_dir}DATABASES/Parkdale_Fantasy_Hockey_Transactions_and_Drafts.csv',
#                                           low_memory=False)
#         df_draft_full = df_all_transactions[df_all_transactions['transaction_type'] == 'draft']
#         try:
#             df_draft_og = pd.read_csv(f'{root_dir}DATABASES/Draft_Analytics.csv', low_memory=False)
#             df_draft_res = df_draft_og[df_draft_og['season'] != year]
#             print(f'>>>> [Rundate: {time.ctime()}] Truncated {len(df_draft_og) - len(df_draft_res)} draft entries...')
#
#         except:
#             df_draft_res = pd.DataFrame()
#         df_current_draft = df_draft_full[(df_draft_full['season'] == year)]
#         df_rosters = df_rosters_full[df_rosters_full['Year'] == year]
#         df_current_draft['pick_number'] = df_current_draft['transaction_id'].str.split('_', expand=True)[3]
#         # df_full = df_current_draft.merge(df_rosters, how = 'left',right_on='Player_Yahoo', left_on='name')[
#         #     ['season', 'Player_Yahoo', 'Team_Yahoo', 'FP_TOTAL', 'GM_Name', 'draft_round', 'pick_number', 'destination',
#         #      'GM_Name_destination', 'keeper']]
#         df_full = df_rosters.merge(df_current_draft, how = 'outer',left_on='Player_Yahoo', right_on='name')[
#             ['season', 'Player_Yahoo', 'Team_Yahoo', 'FP_TOTAL', 'GM_Name', 'draft_round', 'pick_number', 'destination',
#              'GM_Name_destination', 'keeper']]
#         df_full = df_full[df_full['GM_Name'] == df_full['GM_Name_destination']]
#         df_full['Game_count'] = 1
#
#         df_grouped = df_full.groupby(['season',
#                                       'Player_Yahoo', 'Team_Yahoo',
#                                       'GM_Name', 'draft_round', 'pick_number',
#                                       'destination', 'GM_Name_destination', 'keeper'], as_index=False)[
#             ['FP_TOTAL', 'Game_count']].sum()
#         df_draft_res = pd.concat([df_draft_res, df_grouped])
#
#         df_draft_res.to_csv(f'{root_dir}DATABASES/Draft_Analytics.csv', index=False)
#
#
# # FAAB Analytics
# def faab_analytics(year, control_file):
#     if control_file['faab_analytics_status']:
#         print(f'>>>> [Rundate: {time.ctime()}] Beginning FAAB analytics for {year}...')
#         try:
#             df_rosters_full = pd.read_csv(f'{root_dir}DATABASES/Parkdale_Fantasy_Hockey_Rosters_and_Calcs.csv', low_memory=False)
#             df_rosters_full = df_rosters_full[(df_rosters_full['Selected Position'] != 'BN') &
#                                               (df_rosters_full['Selected Position'] != 'IR+')]
#             df_rosters = df_rosters_full[df_rosters_full['Year'] == year]
#             df_rosters['Game_count'] = 1
#         except:
#             df_rosters = pd.DataFrame()
#
#         df_trans = pd.read_csv(f'{root_dir}DATABASES/Parkdale_Fantasy_Hockey_Transactions_and_Drafts.csv', low_memory=False)
#         try:
#             df_faab_og = pd.read_csv(f'{root_dir}DATABASES/FAAB_Analytics.csv', low_memory=False)
#             df_faab_res = df_faab_og[df_faab_og['season'] != year]
#             print(f'>>>> [Rundate: {time.ctime()}] Truncated {len(df_faab_og) - len(df_faab_res)} FAAB entries...')
#         except:
#             df_faab_res = pd.DataFrame()
#             print(f'>>>> [Rundate: {time.ctime()}] FAAB data does not exist yet...')
#         df_faab = df_trans[(df_trans['faab_bid'].notnull()) & (df_trans['season'] == year)]
#
#
#         df_faab_stats = df_faab.merge(df_rosters,
#                                       left_on=['season',
#                                                'name',
#                                                'GM_Name_destination'],
#                                       right_on=['Year',
#                                                 'Player_Yahoo',
#                                                 'GM_Name'])[
#             ['season', 'week', 'transaction_date', 'Player_Yahoo', 'Team_Yahoo', 'faab_bid', 'FP_TOTAL', 'GM_Name',
#              'destination', 'GM_Name_destination', 'Game_count']]
#
#         df_faab_stats = df_faab_stats.groupby(
#             ['season', 'week', 'transaction_date', 'Player_Yahoo', 'Team_Yahoo', 'GM_Name', 'destination',
#              'GM_Name_destination', 'faab_bid'], as_index=False)[['FP_TOTAL', 'Game_count']].sum()
#
#         df_faab_res = pd.concat([df_faab_res, df_faab_stats])
#         df_faab_res.to_csv(f'{root_dir}DATABASES/FAAB_Analytics.csv', index=False)
#
#
# def bench_analytics(year, control_file):
#     if control_file['bench_analytics_status']:
#         print(f'>>>> [Rundate: {time.ctime()}] Beginning Bench analytics for {year}...')
#
#         df_rosters_full = pd.read_csv(f'{root_dir}DATABASES/Parkdale_Fantasy_Hockey_Rosters_and_Calcs.csv', low_memory=False)
#         df_rosters_full = df_rosters_full[
#             (df_rosters_full['Playoffs'] == 0)]
#         try:
#             df_bench_og = pd.read_csv(f'{root_dir}DATABASES/Bench_Analytics.csv', low_memory=False)
#             df_bench_res = df_bench_og[df_bench_og['Year'] != year]
#             print(f'>>>> [Rundate: {time.ctime()}] Truncated {len(df_bench_og) - len(df_bench_res)} Bench entries...')
#         except:
#             df_bench_res = pd.DataFrame()
#             print(f'>>>> [Rundate: {time.ctime()}] Bench data does not exist yet...')
#         df_bench_stats = df_rosters_full[df_rosters_full['Year'] == year]
#         df_bench_stats['FP_TOTAL'] = df_bench_stats.apply(
#             lambda x: 0 if (x['Selected Position'] != 'BN') else x['FP_TOTAL'], axis=1)
#         df_bench_stats = df_bench_stats.groupby(['Year', 'Week',
#                                                  'Team_Name', 'GM_Name'
#                                                  ], as_index=False)[['FP_TOTAL',
#                                                                      'SKATER_START',
#                                                                      'GOALIE_START',
#                                                                      'BENCHED_START']].sum()
#         df_bench_stats['TOTAL_STARTS'] = df_bench_stats['SKATER_START'] + df_bench_stats['GOALIE_START'] + \
#                                          df_bench_stats['BENCHED_START']
#         df_bench_stats['BENCH RATIO'] = df_bench_stats['BENCHED_START'] / df_bench_stats['TOTAL_STARTS']
#         df_bench_res = pd.concat([df_bench_res, df_bench_stats])
#         df_bench_res.to_csv(f'{root_dir}DATABASES/Bench_Analytics.csv', index=False)
#
#
# def forgotten_start_analytics(year, control_file):
#     if control_file['forgotten_start_analytics_status']:
#         print(f'>>>> [Rundate: {time.ctime()}] Beginning Forgotten Start analytics for {year}...')
#         df_teams = pd.read_csv(f'{root_dir}TEAMS_METADATA/{year}_teams.csv')
#         df_rosters_full = pd.read_csv(f'{root_dir}DATABASES/Parkdale_Fantasy_Hockey_Rosters_and_Calcs.csv', low_memory=False)
#         df_rosters_full = df_rosters_full[
#             (df_rosters_full['Playoffs'] == 0)]
#         try:
#             df_frgt_og = pd.read_csv(f'{root_dir}DATABASES/Forgotten_Analytics.csv', low_memory=False)
#             df_frgt_res = df_frgt_og[df_frgt_og['Year'] != year]
#             print(f'>>>> [Rundate: {time.ctime()}] Truncated {len(df_frgt_og) - len(df_frgt_res)} Forgotten entries...')
#         except:
#             df_frgt_res = pd.DataFrame()
#             print(f'>>>> [Rundate: {time.ctime()}] Forgotten data does not exist yet...')
#         df_frgt_stats = df_rosters_full[(df_rosters_full['Year'] == year)
#                                         & (df_rosters_full['FORGOTTEN_START'] == 1)]
#         # df_frgt_stats['FP_TOTAL'] = df_frgt_stats.apply(lambda x: 0 if (x['Selected Position']!=1) else x['FP_TOTAL'], axis=1)
#         df_frgt_stats = df_frgt_stats.groupby(['Year', 'Week', 'Date_NST',
#                                                'Team_Name', 'Player_Yahoo', 'GM_Name', 'Team_HR',
#                                                ], as_index=False)[['FP_TOTAL',
#                                                                    'FORGOTTEN_START']].sum()
#         for gm in df_teams['GM_Name'].unique():
#             if gm in df_frgt_stats['GM_Name'].unique():
#                 pass
#             else:
#                 df_frgt_stats.loc[len(df_frgt_stats)] = (year,
#                                                          0,
#                                                          '',
#                                                          '',
#                                                          '',
#                                                          gm,
#                                                          '',
#                                                          0,
#                                                          0)
#
#         df_frgt_res = pd.concat([df_frgt_res, df_frgt_stats])
#         df_frgt_res.to_csv(f'{root_dir}DATABASES/Forgotten_Analytics.csv', index=False)
#
#
# # Keeper analytics
# def keeper_analytics(year, control_file):
#     if year < 2019:
#         print(f'>>>> [Rundate: {time.ctime()}] No keepers in {year}...')
#         return  # no keepers prior to 2019
#     if control_file['keeper_analytics_status']:
#         print(f'>>>> [Rundate: {time.ctime()}] Beginning keeper analytics for {year}...')
#
#         df_rosters_full = pd.read_csv(f'{root_dir}DATABASES/Parkdale_Fantasy_Hockey_Rosters_and_Calcs.csv', low_memory=False)
#         df_rosters_full = df_rosters_full[(df_rosters_full['Selected Position'] != 'BN') &
#                                           (df_rosters_full['Selected Position'] != 'IR+') &
#                                           (df_rosters_full['Playoffs'] == 0)]
#         df_keeps_full = pd.read_csv(f'{root_dir}DATABASES/Parkdale_Fantasy_Hockey_Transactions_and_Drafts.csv', low_memory=False)
#         try:
#             df_keep_og = pd.read_csv(f'{root_dir}DATABASES/Keeper_Analytics.csv', low_memory=False)
#             df_keep_res = df_keep_og[df_keep_og['season'] != year]
#             print(f'>>>> [Rundate: {time.ctime()}] Truncated {len(df_keep_og) - len(df_keep_res)} Keeper entries...')
#         except:
#             df_keep_res = pd.DataFrame()
#             print(f'>>>> [Rundate: {time.ctime()}] Keeper data does not exist yet...')
#         df_keep = df_keeps_full[(df_keeps_full['transaction_type'] == 'draft')
#                                 & (df_keeps_full['season'] == year)
#                                 & (df_keeps_full['keeper'] == 'KEEPER')]
#         df_rosters = df_rosters_full[df_rosters_full['Year'] == year]
#         df_full = df_rosters.merge(df_keep, left_on='Player_Yahoo', right_on='name')[
#             ['season', 'Player_Yahoo', 'Team_Yahoo', 'FP_TOTAL', 'GM_Name', 'draft_round', 'destination',
#              'GM_Name_destination', 'keeper']]
#         df_full = df_full[df_full['GM_Name'] == df_full['GM_Name_destination']]
#         df_full['Game_count'] = 1
#
#         df_grouped = df_full.groupby(['season',
#                                       'Player_Yahoo', 'Team_Yahoo',
#                                       'GM_Name', 'draft_round',
#                                       'destination', 'GM_Name_destination', 'keeper'], as_index=False)[
#             ['FP_TOTAL', 'Game_count']].sum()
#
#         df_keep_res = pd.concat([df_grouped, df_keep_res])
#
#         df_keep_res.to_csv(f'{root_dir}DATABASES/Keeper_Analytics.csv', index=False)
#
#
# # streamer analytics
# def streamer_analytics(year, control_file):
#     if control_file['streamer_analytics_status']:
#         print(f'>>>> [Rundate: {time.ctime()}] Beginning streamer analytics for {year}...')
#
#         df_rosters_full = pd.read_csv(f'{root_dir}DATABASES/Parkdale_Fantasy_Hockey_Rosters_and_Calcs.csv', low_memory=False)
#         df_trans = pd.read_csv(f'{root_dir}DATABASES/Parkdale_Fantasy_Hockey_Transactions_and_Drafts.csv', low_memory=False)
#         df_teams = pd.read_csv(f'{root_dir}TEAMS_METADATA/{year}_teams.csv')
#         try:
#             df_streamer_og = pd.read_csv(f'{root_dir}DATABASES/Streamer_Analytics.csv', low_memory=False)
#
#             df_streamer_res = df_streamer_og[df_streamer_og['season'] != year]
#             print(
#                 f'>>>> [Rundate: {time.ctime()}] Truncated {len(df_streamer_og) - len(df_streamer_res)} Streamer entries...')
#         except:
#             df_streamer_res = pd.DataFrame()
#         df_streamers = df_trans[(df_trans['transaction_type'] == 'add')
#                                 & (df_trans['season'] == year)]
#         df_streamers = df_streamers.drop_duplicates(
#             subset=['season', 'week', 'transaction_type', 'name', 'destination'], keep='first')
#         df_streamers['Streamer_Games'] = 1
#         df_rosters = df_rosters_full[df_rosters_full['Year'] == year]
#         df_full = df_rosters.merge(df_streamers[['season',
#                                                  'week',
#                                                  'name',
#                                                  'GM_Name_destination', 'Streamer_Games']],
#                                    left_on=['Year',
#                                             'Week',
#                                             'Player_Yahoo',
#                                             'GM_Name'],
#                                    right_on=['season',
#                                              'week',
#                                              'name',
#                                              'GM_Name_destination'])
#         df_full = df_full[(df_full['Team_Yahoo'] != 'Free Agency') &
#                           (df_full['Selected Position'] != 'BN') &
#                           (df_full['Selected Position'] != 'IR+')]
#         df_grouped = df_full.groupby(['season', 'Week', 'Player_Yahoo',
#                                       'Team_Yahoo',
#                                       'GM_Name'], as_index=False)[['FP_TOTAL',
#                                                                    'Streamer_Games',
#                                                                    'Goals_NST',
#                                                                    'Total Assists_NST',
#                                                                    '+/-_HR',
#                                                                    'PIM_NST',
#                                                                    'PPP_HR',
#                                                                    'SHP_HR',
#                                                                    'GW_HR',
#                                                                    'S_HR',
#                                                                    'Hits_NST',
#                                                                    'Shots Blocked_NST',
#                                                                    'WINS_HR',
#                                                                    'LOSSES_HR',
#                                                                    'GA_HR',
#                                                                    'SA_HR',
#                                                                    'SV_HR',
#                                                                    'SO_HR',
#                                                                    'GOALIE_TOI']].sum()
#         df_grouped['S%'] = df_grouped['Goals_NST'] / df_grouped['S_HR']
#         df_grouped['SV%'] = df_grouped['SV_HR'] / df_grouped['SA_HR']
#         df_grouped['GAA'] = (df_grouped['GA_HR'] / df_grouped['GOALIE_TOI']) * 60
#
#         # check that all GMs are in
#         for gm in df_teams['GM_Name'].unique():
#             if gm in df_grouped['GM_Name'].unique():
#                 pass
#             else:
#                 df_grouped.loc[len(df_grouped)] = (year,
#                                                    0,
#                                                    '',
#                                                    '',
#                                                    gm,
#                                                    np.nan,
#                                                    np.nan,
#                                                    np.nan,
#                                                    np.nan,
#                                                    np.nan,
#                                                    np.nan,
#                                                    np.nan,
#                                                    np.nan,
#                                                    np.nan,
#                                                    np.nan,
#                                                    np.nan,
#                                                    np.nan,
#                                                    np.nan,
#                                                    np.nan,
#                                                    np.nan,
#                                                    np.nan,
#                                                    np.nan,
#                                                    np.nan,
#                                                    np.nan,
#                                                    np.nan,
#                                                    np.nan,
#                                                    np.nan)
#         df_streamer_res = pd.concat([df_streamer_res, df_grouped])
#
#         df_streamer_res.to_csv(f'{root_dir}DATABASES/Streamer_Analytics.csv', index=False)
#
#
# def player_week_agger(year, control_file, total_df):
#     yahoo_cols = list(control_file['yahoo_columns'].keys())
#     extra_sum_columns = ['FP_TOTAL', 'FORGOTTEN_START']
#
#     list_players = total_df[(total_df['Selected Position'] != 'BN') & (total_df['Selected Position'] != 'IR+')][[
#                                                                                                                     'Year',
#                                                                                                                     'Matchup ID',
#                                                                                                                     'Team_Yahoo',
#                                                                                                                     'GM',
#                                                                                                                     'Player_HR',
#                                                                                                                     'Week',
#                                                                                                                     'Date_NST',
#                                                                                                                     'Selected Position',
#                                                                                                                 ] + yahoo_cols + extra_sum_columns]
#     grouped_list_players = \
#     list_players.groupby(['Year', 'Week', 'Matchup ID', 'Team_Yahoo', 'GM', 'Player_HR'], as_index=False)[
#         yahoo_cols + extra_sum_columns
#         ].sum()
#     # Replace the % stats with properly calculated versions
#     grouped_list_players['S%_HR'] = grouped_list_players['Goals_NST'] / grouped_list_players['S_HR']
#     grouped_list_players['SV%_HR'] = grouped_list_players['SV_HR'] / grouped_list_players['SA_HR']
#     grouped_list_players['GAA_CALC'] = (grouped_list_players['GA_HR'] / grouped_list_players['GOALIE_TOI']) * 60
#     # generate columns for each of the potential rank stats
#
#     return grouped_list_players
#
#
# def matchup_data_cruncher(year, control_file, position, total_df):
#     # these are the week-based analytics
#     yahoo_cols = list(control_file['yahoo_columns'].keys())
#     extra_sum_columns = ['FP_TOTAL', 'FORGOTTEN_START', 'SKATER_START', 'GOALIE_START', 'BENCHED_START']
#     if position == 'BN':
#         yahoo_stats_no_bench = total_df[(total_df['Selected Position'] == 'BN') |
#                                         (total_df['Selected Position'] == 'IR+')][[
#                                                                                       'Year',
#                                                                                       'Matchup ID',
#                                                                                       'Team_Yahoo',
#                                                                                       'GM',
#                                                                                       'Playoffs',
#                                                                                       'Image',
#                                                                                       'Player_HR',
#                                                                                       'Week',
#                                                                                       'Date_NST',
#                                                                                       'Selected Position',
#                                                                                   ] + yahoo_cols + extra_sum_columns]
#
#     elif position == 'ACTIVE':
#         yahoo_stats_no_bench = \
#         total_df[(total_df['Selected Position'] != 'BN') & (total_df['Selected Position'] != 'IR+')][[
#                                                                                                          'Year',
#                                                                                                          'Matchup ID',
#                                                                                                          'Team_Yahoo',
#                                                                                                          'GM',
#                                                                                                          'Playoffs',
#                                                                                                          'Image',
#                                                                                                          'Player_HR',
#                                                                                                          'Week',
#                                                                                                          'Date_NST',
#                                                                                                          'Selected Position',
#                                                                                                      ] + yahoo_cols + extra_sum_columns]
#
#     else:
#         print('Not sure what you are looking for ')
#
#     grouped_yahoo_stats_no_bench = yahoo_stats_no_bench.groupby(['Year',
#                                                                  'Week',
#                                                                  'Matchup ID',
#                                                                  'Team_Yahoo',
#                                                                  'GM',
#                                                                  'Playoffs',
#                                                                  'Image'], as_index=False)[
#         yahoo_cols + extra_sum_columns
#         ].sum()
#
#     # FAILED GOALIE REQUIREMENT FORCER
#     grouped_yahoo_stats_no_bench['FAILED_GOALIE_REQUIREMENT'] = 0
#     if position == 'ACTIVE':
#         # FUCKING YAHOO AND THEIR BULLSHIT ARBITRARY NONSENSE
#         # WEEK 12, YEAR 2024 HAD NO REQUIREMENTS
#         grouped_yahoo_stats_no_bench.loc[((grouped_yahoo_stats_no_bench['GOALIE_START'] < 3)
#                                          & ((grouped_yahoo_stats_no_bench['Year'].astype(str) != '2,024.0') # dumb dumb stupid
#                                         & (grouped_yahoo_stats_no_bench['Week'].astype(str) != '12.0')) # stinky dumb
#                                           ),
#                                          'FAILED_GOALIE_REQUIREMENT'] = 1
#         grouped_yahoo_stats_no_bench.loc[((grouped_yahoo_stats_no_bench['FAILED_GOALIE_REQUIREMENT'] == 1)),
#                                          'WINS_HR'] = -1
#         grouped_yahoo_stats_no_bench.loc[((grouped_yahoo_stats_no_bench['FAILED_GOALIE_REQUIREMENT'] == 1)),
#                                          'LOSSES_HR'] = 10
#         grouped_yahoo_stats_no_bench.loc[((grouped_yahoo_stats_no_bench['FAILED_GOALIE_REQUIREMENT'] == 1)),
#                                          'GA_HR'] = 100
#         grouped_yahoo_stats_no_bench.loc[((grouped_yahoo_stats_no_bench['FAILED_GOALIE_REQUIREMENT'] == 1)),
#                                          'SA_HR'] = 10
#         grouped_yahoo_stats_no_bench.loc[((grouped_yahoo_stats_no_bench['FAILED_GOALIE_REQUIREMENT'] == 1)),
#                                          'SV_HR'] = 7
#         grouped_yahoo_stats_no_bench.loc[((grouped_yahoo_stats_no_bench['FAILED_GOALIE_REQUIREMENT'] == 1)),
#                                          'SO_HR'] = -1
#         grouped_yahoo_stats_no_bench.loc[((grouped_yahoo_stats_no_bench['FAILED_GOALIE_REQUIREMENT'] == 1)),
#                                          'GOALIE_TOI'] = 600
#
#         # Replace the % stats with properly calculated versions
#     grouped_yahoo_stats_no_bench['S%_HR'] = grouped_yahoo_stats_no_bench['Goals_NST'] / grouped_yahoo_stats_no_bench[
#         'S_HR']
#     grouped_yahoo_stats_no_bench['SV%_HR'] = grouped_yahoo_stats_no_bench['SV_HR'] / grouped_yahoo_stats_no_bench[
#         'SA_HR']
#     grouped_yahoo_stats_no_bench['GAA_CALC'] = (grouped_yahoo_stats_no_bench['GA_HR'] / grouped_yahoo_stats_no_bench[
#         'GOALIE_TOI']) * 60
#     # generate columns for each of the potential rank stats
#     for key in yahoo_cols:
#         if key == 'GAA_NST':
#             grouped_yahoo_stats_no_bench['GAA_CALC_RANK'] = np.nan
#         else:
#             grouped_yahoo_stats_no_bench[key + '_RANK'] = np.nan
#     # read it back to a dataframe to get the metadata cols
#     data = grouped_yahoo_stats_no_bench.copy()
#     stats_with_rank = pd.DataFrame()
#
#     for week in data['Week'].unique():
#         week_matchup = data[data['Week'] == week]
#         week_matchup['Quality_Score'] = 0
#         week_matchup['Offense_QS'] = 0
#         week_matchup['Peripheral_QS'] = 0
#         week_matchup['Goaltending_QS'] = 0
#         if position == 'ACTIVE':
#             off_count = 0
#             per_count = 0
#             gl_count = 0
#             for key in yahoo_cols:
#                 if year in control_file['yahoo_columns'][key]['years_applicable']:
#                     if control_file['yahoo_columns'][key]['type'] == 'Offense':
#                         off_count += 1
#                         week_matchup[key + '_RANK'] = week_matchup[key].rank(pct=True, ascending=True)
#                         week_matchup['Offense_QS'] += week_matchup[key + '_RANK']
#                     elif control_file['yahoo_columns'][key]['type'] == 'Peripheral':
#                         per_count += 1
#                         week_matchup[key + '_RANK'] = week_matchup[key].rank(pct=True, ascending=True)
#                         week_matchup['Peripheral_QS'] += week_matchup[key + '_RANK']
#                     else:  # goaltending
#                         gl_count += 1
#                         if key == 'LOSSES_HR':
#                             week_matchup[key + '_RANK'] = week_matchup[key].rank(pct=True, ascending=False)
#                             week_matchup['Goaltending_QS'] += week_matchup[key + '_RANK']
#                         elif key == 'GAA_NST':
#                             week_matchup['GAA_CALC_RANK'] = week_matchup['GAA_CALC'].rank(pct=True, ascending=False)
#                             week_matchup['Goaltending_QS'] += week_matchup['GAA_CALC_RANK']
#                         else:
#                             week_matchup[key + '_RANK'] = week_matchup[key].rank(pct=True, ascending=True)
#                             week_matchup['Goaltending_QS'] += week_matchup[key + '_RANK']
#
#             week_matchup['Goaltending_QS'] = (week_matchup['Goaltending_QS'] / gl_count) * 5
#             week_matchup['Peripheral_QS'] = (week_matchup['Peripheral_QS'] / per_count) * 5
#             week_matchup['Offense_QS'] = (week_matchup['Offense_QS'] / off_count) * 5
#             week_matchup['Quality_Score'] = week_matchup['Goaltending_QS'] + week_matchup['Peripheral_QS'] + \
#                                             week_matchup['Offense_QS']
#
#             week_matchup['FP_Rank'] = week_matchup['FP_TOTAL'].rank(pct=True, ascending=True)
#             week_matchup['Starts_Rank'] = (week_matchup['SKATER_START'] + week_matchup['GOALIE_START']).rank(pct=True,
#                                                                                                              ascending=True)
#
#         # what the hell was I doing here????
#         # This is the opponent roundup
#         for matchup in week_matchup['Matchup ID'].unique():
#
#             try:
#                 matchup_df_p1 = week_matchup[week_matchup['Matchup ID'] == matchup].iloc[[0]].reset_index()
#                 matchup_df_p1_opp = matchup_df_p1.copy().add_suffix('_opp')
#             except:
#                 matchup_df_p1 = pd.DataFrame()
#                 matchup_df_p1_opp = pd.DataFrame()
#             try:
#                 matchup_df_p2 = week_matchup[week_matchup['Matchup ID'] == matchup].iloc[[1]].reset_index()
#                 matchup_df_p2_opp = matchup_df_p2.copy().add_suffix('_opp')
#             except:
#                 matchup_df_p2 = pd.DataFrame()
#                 matchup_df_p2_opp = pd.DataFrame()
#
#             result1 = pd.concat([matchup_df_p1, matchup_df_p2_opp], axis=1)
#             result2 = pd.concat([matchup_df_p2, matchup_df_p1_opp], axis=1)
#             matchup_final = pd.concat([result1, result2])
#
#             stats_with_rank = pd.concat([stats_with_rank, matchup_final])
#             # Like it works but what???
#             stats_with_rank['STATUS'] = position if 'ACTIVE' else 'BENCH'
#
#     return stats_with_rank
#
#
# #############################################################################
# def closeness(val, col):
#     if (col == 'Goals_NST' or
#             col == 'Total Assists_NST' or
#             col == 'PIM_NST' or
#             col == 'PPP_HR' or
#             col == '+/-_HR' or
#             col == 'S_HR' or
#             col == 'Hits_NST' or
#             col == 'Shots Blocked_NST' or
#             col == 'SV_HR'):
#         if val == 0:
#             return 1
#         elif val == 1:
#             return 0.25
#         else:
#             return 0
#
#     elif (col == 'WINS_HR' or
#           col == 'LOSSES_HR'):
#         if round(val) == 0:
#             return 1
#         else:
#             return 0
#
#     elif (col == 'SV%_HR' or
#           col == 'S%_HR'):
#         if math.isnan(val):
#             return 0
#         elif round(1000 * val) == 0.0:
#             return 1
#         elif val == 1.0:
#             return 0.25
#         else:
#             return 0
#
#     else:
#         return 0
#         # shp, gwg, shutouts are so few and far between that it doesn't matter...
#
#
# ###################################################################################
# def yahoo_win_losser(df, cf, year):
#     df['Yahoo_Score'] = 0
#     df['Closeness_Score'] = 0
#     for col in cf['yahoo_columns']:
#         if col == 'LOSSES_HR':
#             df[f'LOSSES_HR_win'] = df.apply(
#                 lambda x: 1 if (x['LOSSES_HR'] < x['LOSSES_HR_opp']) and x['STATUS'] == 'ACTIVE' else 0, axis=1)
#             df[f'LOSSES_HR_win_cat'] = df.apply(lambda x: "WIN" if
#             (x['LOSSES_HR'] < x['LOSSES_HR_opp']) else "TIE" if
#             (x['LOSSES_HR'] == x['LOSSES_HR_opp']) else "LOSS", axis=1)
#
#         elif col == 'GAA_NST':
#             df[f'GAA_CALC_win'] = df.apply(lambda x: 1 if x['GAA_CALC'] < x['GAA_CALC_opp'] else 0, axis=1)
#             df[f'GAA_CALC_win_cat'] = df.apply(lambda x: "WIN" if
#             (x['GAA_CALC'] < x['GAA_CALC_opp']) else "TIE" if
#             (x['GAA_CALC'] == x['GAA_CALC_opp']) else "LOSS", axis=1)
#         else:
#             df[f'{col}_win'] = df.apply(lambda x: 1 if x[col] > x[col + '_opp'] else 0, axis=1)
#             df[f'{col}_win_cat'] = df.apply(lambda x: "WIN" if
#             (x[col] > x[col + '_opp']) else "TIE" if
#             (x[col] == x[col + '_opp']) else "LOSS", axis=1)
#         if year in cf['yahoo_columns'][col]['years_applicable']:
#             if col == 'GAA_NST':
#                 df['Yahoo_Score'] = df['Yahoo_Score'] + df[f'GAA_CALC_win']
#             else:
#                 df['Yahoo_Score'] = df['Yahoo_Score'] + df[f'{col}_win']
#
#     df['UID'] = df['Year'].astype(str) + df['Week'].astype(str) + df['Matchup ID'].astype(str)
#     df['Yahoo_Win'] = ''
#     df['Yahoo_Points'] = np.nan
#     df['Victory_Margin'] = np.nan
#     df['Closeness_Score'] = 0.0
#
#     for uid in df['UID'].unique():
#         df_uid = df[(df['UID'] == uid) & (df['STATUS'] == 'ACTIVE')]
#         if len(df_uid) < 2:
#             print(f"{uid} {df_uid['Team_Yahoo'].iloc[0]} had a bye week")
#             continue
#         team_a = df_uid['Team_Yahoo'].iloc[0]
#         team_b = df_uid['Team_Yahoo'].iloc[1]
#         df.loc[(df['UID'] == uid) & (df['Team_Yahoo'] == team_a) & (df['STATUS'] == 'ACTIVE'), 'Victory_Margin'] = (
#                     df_uid['Yahoo_Score'].iloc[0] - df_uid['Yahoo_Score'].iloc[1])
#         df.loc[(df['UID'] == uid) & (df['Team_Yahoo'] == team_b) & (df['STATUS'] == 'ACTIVE'), 'Victory_Margin'] = (
#                     df_uid['Yahoo_Score'].iloc[1] - df_uid['Yahoo_Score'].iloc[0])
#         if df_uid['Yahoo_Score'].iloc[0] > df_uid['Yahoo_Score'].iloc[1]:
#             df.loc[(df['UID'] == uid) & (df['Team_Yahoo'] == team_a) & (df['STATUS'] == 'ACTIVE'), 'Yahoo_Win'] = 'WIN'
#             df.loc[(df['UID'] == uid) & (df['Team_Yahoo'] == team_b) & (df['STATUS'] == 'ACTIVE'), 'Yahoo_Win'] = 'LOSS'
#             df.loc[(df['UID'] == uid) & (df['Team_Yahoo'] == team_a) & (df['STATUS'] == 'ACTIVE'), 'Yahoo_Points'] = 2
#             df.loc[(df['UID'] == uid) & (df['Team_Yahoo'] == team_b) & (df['STATUS'] == 'ACTIVE'), 'Yahoo_Points'] = 0
#         elif df_uid['Yahoo_Score'].iloc[0] < df_uid['Yahoo_Score'].iloc[1]:
#             df.loc[(df['UID'] == uid) & (df['Team_Yahoo'] == team_b) & (df['STATUS'] == 'ACTIVE'), 'Yahoo_Win'] = 'WIN'
#             df.loc[(df['UID'] == uid) & (df['Team_Yahoo'] == team_a) & (df['STATUS'] == 'ACTIVE'), 'Yahoo_Win'] = 'LOSS'
#             df.loc[(df['UID'] == uid) & (df['Team_Yahoo'] == team_a) & (df['STATUS'] == 'ACTIVE'), 'Yahoo_Points'] = 0
#             df.loc[(df['UID'] == uid) & (df['Team_Yahoo'] == team_b) & (df['STATUS'] == 'ACTIVE'), 'Yahoo_Points'] = 2
#         else:
#             df.loc[(df['UID'] == uid) & (df['Team_Yahoo'] == team_a) & (df['STATUS'] == 'ACTIVE'), 'Yahoo_Win'] = 'TIE'
#             df.loc[(df['UID'] == uid) & (df['Team_Yahoo'] == team_b) & (df['STATUS'] == 'ACTIVE'), 'Yahoo_Win'] = 'TIE'
#             df.loc[(df['UID'] == uid) & (df['Team_Yahoo'] == team_a) & (df['STATUS'] == 'ACTIVE'), 'Yahoo_Points'] = 1
#             df.loc[(df['UID'] == uid) & (df['Team_Yahoo'] == team_b) & (df['STATUS'] == 'ACTIVE'), 'Yahoo_Points'] = 1
#         for col in ['Goals_NST', 'Total Assists_NST',
#                     '+/-_HR', 'PIM_NST', 'PPP_HR', 'SHP_HR', 'S_HR', 'S%_HR',
#                     'Hits_NST', 'Shots Blocked_NST', 'WINS_HR', 'LOSSES_HR',
#                     'GAA_CALC', 'SA_HR', 'SV_HR', 'SV%_HR', 'SO_HR']:
#             col_delta = abs(df_uid[col].iloc[0] - df_uid[col].iloc[1])
#             closeness_value = closeness(col_delta, col)
#             df.loc[(df['UID'] == uid) & (df['STATUS'] == 'ACTIVE'), 'Closeness_Score'] += closeness_value
#             # print(f"For {col}: abs {df_uid[col].iloc[0]}-{df_uid[col].iloc[1]} is {col_delta}, increasing closeness to {df[(df['UID'] == uid)]['Closeness_Score'].iloc[1]}  ")
#
#     return df
#
#
# ###################################################################################
# def trending_analytics(all_df, cf):
#     # These are the cross-week trending/analytics
#
#     df = all_df.copy()
#     df_processed = pd.DataFrame()
#     df['AVERAGE_QS'] = 0
#     df['AVERAGE_OPP_SCORE'] = 0
#     df['AVERAGE_POWER_SCORE'] = 0
#     df['WEEK_POWER_SCORE'] = 0
#     df['AVERAGE_POWER_SCORE'] = 0
#     df['TOTAL_POWER_SCORE'] = 0
#     df['POWER_RANK'] = 0
#     injured_df = df[df['STATUS'] == 'IR+']
#     benched_df = df[df['STATUS'] == 'BN']
#     for team in df['Team_Yahoo'].unique():
#         team_df = df[(df['Team_Yahoo'] == team) & (df['STATUS'] == 'ACTIVE')]
#         team_df.sort_values('Week', ascending=True, inplace=True)
#         processed_team_df = pd.DataFrame()
#         historic_component = 0
#         avg_qs = 0
#         avg_opp_qs = 0
#
#         for week in team_df['Week']:
#             team_week_df = team_df[team_df['Week'] == week]
#             team_week_df['WEEK_POWER_SCORE'] = (cf['matchup_weights']['week_val'] * team_week_df['Quality_Score'] +
#                                                 cf['matchup_weights']['opp_val'] * team_week_df['Quality_Score_opp'] +
#                                                 (1 / 15) * team_week_df['Victory_Margin'] +
#                                                 (0.75) * team_week_df['FP_Rank'] +
#                                                 (0.75) * team_week_df['Starts_Rank'] +
#                                                 (-1) * team_week_df['FAILED_GOALIE_REQUIREMENT'] +
#                                                 (1 / 6) * team_week_df['Closeness_Score']
#                                                 )
#             historic_component += team_week_df['WEEK_POWER_SCORE'].values[0]
#             avg_qs += team_week_df['Quality_Score'].values[0]
#             avg_opp_qs += team_week_df['Quality_Score_opp'].values[0]
#             team_week_df['ROLLING_AVERAGE_QS'] = (avg_qs) / week
#             team_week_df['ROLLING_AVERAGE_OPP_SCORE'] = (avg_opp_qs) / week
#             team_week_df['ROLLING_AVERAGE_POWER_SCORE'] = (
#                         cf['matchup_weights']['week_val'] * team_week_df['ROLLING_AVERAGE_QS'] +
#                         cf['matchup_weights']['opp_val'] * team_week_df['ROLLING_AVERAGE_OPP_SCORE'])
#             team_week_df['TOTAL_POWER_SCORE'] = (
#                         cf['matchup_weights']['historic'] * team_week_df['ROLLING_AVERAGE_POWER_SCORE'] +
#                         cf['matchup_weights']['current'] * team_week_df['WEEK_POWER_SCORE'])
#
#             processed_team_df = pd.concat([processed_team_df, team_week_df])
#
#         df_processed = pd.concat([df_processed, processed_team_df])
#
#     df_power_rank = pd.DataFrame()
#     for week in df_processed['Week'].unique():
#         week_df = df_processed[df_processed['Week'] == week]
#         week_df['POWER_RANK'] = week_df['TOTAL_POWER_SCORE'].rank(pct=False, ascending=False)
#         # week_df['POWER_RANK'] = week_df['WEEK_POWER_SCORE'].rank(pct=False,ascending =False)
#
#         df_power_rank = pd.concat([df_power_rank, week_df])
#
#     # add in the bench and IR stuff
#     df_power_rank = pd.concat([df_power_rank, injured_df, benched_df])
#     return df_power_rank
#
#
# #################################################################################################
# def matchup_consolidator(year, control_file):
#     if control_file['matchup_consolidator_status']:
#
#         megaframe = pd.read_csv(f'{root_dir}DATABASES/Parkdale_Fantasy_Hockey_Rosters_and_Calcs.csv', low_memory=False)
#
#
#         consolidated_bench_df = pd.DataFrame()
#         consolidated_expanded_df = pd.DataFrame()
#         try:
#             consolidated_matchup_og = pd.read_csv(f'{root_dir}DATABASES/Consolidated_Matchups_Data.csv', low_memory=False)
#             consolidated_matchup_df = consolidated_matchup_og[consolidated_matchup_og['Year'] != year]
#             print(f'>>>> [Rundate: {time.ctime()}] Beginning matchup analytics for {year}...')
#         except:
#             consolidated_matchup_df = pd.DataFrame()
#             print(f'>>>> [Rundate: {time.ctime()}] Matchup data does not exist yet...')
#         season = megaframe[megaframe['Year'] == year]  #
#
#         season['Team_Yahoo'] = season['Team_Yahoo'].fillna('Free Agency')
#         years_matchup = pd.read_csv(f'{root_dir}MATCHUPS_METADATA/{year}_matchups_metadata.csv')
#
#         transactions = pd.read_csv(f'{root_dir}TRANSACTIONS/{year}_transactions.csv')
#
#         total_df = season.merge(years_matchup, how='left', left_on=['Year', 'Week', 'Team_Yahoo'],
#                                 right_on=['Year', 'Week', 'Team'])
#
#         total_df.drop('Playoffs_y', axis=1, inplace=True)
#         total_df.rename({'Playoffs_x': "Playoffs"}, axis=1, inplace=True)
#         matchup_dataframe = pd.DataFrame()
#         player_dataframe = pd.DataFrame()
#         # for debugging
#         df_player_list = player_week_agger(year, control_file, total_df)
#         df_player_list = df_player_list[
#             ['Year', 'Week', 'Matchup ID', 'Team_Yahoo', 'GM', 'Player_HR', 'Goals_NST', 'Total Assists_NST', '+/-_HR',
#              'PIM_NST', 'PPP_HR', 'SHP_HR', 'GW_HR', 'S_HR', 'S%_HR', 'Hits_NST', 'Shots Blocked_NST', 'WINS_HR',
#              'LOSSES_HR', 'GA_HR', 'GAA_CALC', 'SA_HR', 'SV_HR', 'SV%_HR', 'SO_HR', 'GOALIE_TOI', 'FP_TOTAL',
#              'FORGOTTEN_START']]
#         df_player_list = df_player_list.dropna(subset=['Year'])
#         df_player_list.to_csv(f'{root_dir}MATCHUPS_BY_YEAR/{year}_matchup_player_list.csv')
#         print(f'Distinct weeks (total df): {total_df.Week.unique()}')
#
#         # actual results
#         for position in ['ACTIVE', 'BN']:
#             matchup_dataframe = pd.concat(
#                 [matchup_dataframe, matchup_data_cruncher(year, control_file, position, total_df)])
#         print(f'Distinct weeks: {matchup_dataframe.Week.unique()}')
#
#         matchup_dataframe = matchup_dataframe.dropna(subset=['Year'])
#         matchup_dataframe['MATCHUP_SCORE'] = matchup_dataframe['Quality_Score_opp'] + matchup_dataframe['Quality_Score']
#
#         matchup_dataframe = yahoo_win_losser(matchup_dataframe, control_file, year)
#
#         matchup_dataframe = trending_analytics(matchup_dataframe, control_file)
#         matchup_dataframe['INJURED_START'] = ''
#         print(f'Distinct weeks after trending analytics: {matchup_dataframe.Week.unique()}')
#
#         matchup_dataframe_expanded = matchup_dataframe[['UID', 'STATUS', 'Year', 'Week', 'Matchup ID',
#                                                         'Quality_Score', 'Quality_Score_opp',
#                                                         'ROLLING_AVERAGE_QS', 'ROLLING_AVERAGE_OPP_SCORE',
#                                                         'MATCHUP_SCORE',
#                                                         'WEEK_POWER_SCORE',
#                                                         'POWER_RANK', 'Yahoo_Score', 'Victory_Margin',
#                                                         'Yahoo_Win', 'Yahoo_Points', 'Closeness_Score', 'Team_Yahoo',
#                                                         'Goals_NST', 'Total Assists_NST',
#                                                         '+/-_HR', 'PIM_NST', 'PPP_HR', 'SHP_HR', 'GW_HR', 'S_HR',
#                                                         'S%_HR',
#                                                         'Hits_NST', 'Shots Blocked_NST', 'WINS_HR', 'LOSSES_HR',
#                                                         'GA_HR',
#                                                         'GAA_CALC', 'SA_HR', 'SV_HR', 'SV%_HR', 'SO_HR', 'GOALIE_TOI',
#                                                         'FP_TOTAL', 'FP_Rank', 'FAILED_GOALIE_REQUIREMENT',
#                                                         'FORGOTTEN_START',
#                                                         'SKATER_START', 'GOALIE_START', 'Starts_Rank', 'BENCHED_START',
#                                                         'INJURED_START', 'Team_Yahoo_opp',
#                                                         'FP_TOTAL_opp', 'FORGOTTEN_START_opp', 'GM', 'Playoffs',
#                                                         'Image',
#                                                         'Goaltending_QS', 'Offense_QS', 'Peripheral_QS',
#                                                         'Goaltending_QS_opp', 'Offense_QS_opp', 'Peripheral_QS_opp',
#                                                         'Goals_NST_win',
#                                                         'Total Assists_NST_win',
#                                                         '+/-_HR_win',
#                                                         'PIM_NST_win',
#                                                         'PPP_HR_win',
#                                                         'SHP_HR_win',
#                                                         'GW_HR_win',
#                                                         'S_HR_win',
#                                                         'S%_HR_win',
#                                                         'Hits_NST_win',
#                                                         'Shots Blocked_NST_win',
#                                                         'WINS_HR_win',
#                                                         'LOSSES_HR_win',
#                                                         'GA_HR_win',
#                                                         'GAA_CALC_win',
#                                                         'SA_HR_win',
#                                                         'SV_HR_win',
#                                                         'SV%_HR_win',
#                                                         'SO_HR_win',
#                                                         'Goals_NST_win_cat',
#                                                         'Total Assists_NST_win_cat',
#                                                         '+/-_HR_win_cat',
#                                                         'PIM_NST_win_cat',
#                                                         'PPP_HR_win_cat',
#                                                         'SHP_HR_win_cat',
#                                                         'GW_HR_win_cat',
#                                                         'S_HR_win_cat',
#                                                         'S%_HR_win_cat',
#                                                         'Hits_NST_win_cat',
#                                                         'Shots Blocked_NST_win_cat',
#                                                         'WINS_HR_win_cat',
#                                                         'LOSSES_HR_win_cat',
#                                                         'GA_HR_win_cat',
#                                                         'GAA_CALC_win_cat',
#                                                         'SA_HR_win_cat',
#                                                         'SV_HR_win_cat',
#                                                         'SV%_HR_win_cat',
#                                                         'SO_HR_win_cat',
#                                                         'Goals_NST_RANK',
#                                                         'Total Assists_NST_RANK',
#                                                         '+/-_HR_RANK',
#                                                         'PIM_NST_RANK',
#                                                         'PPP_HR_RANK',
#                                                         'SHP_HR_RANK',
#                                                         'GW_HR_RANK',
#                                                         'S_HR_RANK',
#                                                         'S%_HR_RANK',
#                                                         'Hits_NST_RANK',
#                                                         'Shots Blocked_NST_RANK',
#                                                         'WINS_HR_RANK',
#                                                         'LOSSES_HR_RANK',
#                                                         'GA_HR_RANK',
#                                                         'GAA_CALC_RANK',
#                                                         'SA_HR_RANK',
#                                                         'SV_HR_RANK',
#                                                         'SV%_HR_RANK',
#                                                         'SO_HR_RANK'
#
#                                                         ]]  # Trending
#         matchup_dataframe = matchup_dataframe[['UID', 'STATUS', 'Year', 'Week', 'Matchup ID',
#                                                'Quality_Score', 'Quality_Score_opp',
#                                                'ROLLING_AVERAGE_QS', 'ROLLING_AVERAGE_OPP_SCORE', 'MATCHUP_SCORE',
#                                                'WEEK_POWER_SCORE',
#                                                'POWER_RANK', 'Yahoo_Score', 'Victory_Margin',
#                                                'Yahoo_Win', 'Yahoo_Points', 'Closeness_Score', 'Team_Yahoo',
#                                                'Goals_NST', 'Total Assists_NST',
#                                                '+/-_HR', 'PIM_NST', 'PPP_HR', 'SHP_HR', 'GW_HR', 'S_HR', 'S%_HR',
#                                                'Hits_NST', 'Shots Blocked_NST', 'WINS_HR', 'LOSSES_HR', 'GA_HR',
#                                                'GAA_CALC', 'SA_HR', 'SV_HR', 'SV%_HR', 'SO_HR', 'GOALIE_TOI',
#                                                'FP_TOTAL', 'FP_Rank', 'FAILED_GOALIE_REQUIREMENT', 'FORGOTTEN_START',
#                                                'SKATER_START', 'GOALIE_START', 'Starts_Rank', 'BENCHED_START',
#                                                'INJURED_START', 'Team_Yahoo_opp',
#                                                'FP_TOTAL_opp', 'FORGOTTEN_START_opp', 'GM', 'Playoffs', 'Image',
#                                                'Goaltending_QS', 'Offense_QS', 'Peripheral_QS',
#                                                'Goaltending_QS_opp', 'Offense_QS_opp', 'Peripheral_QS_opp'
#
#                                                ]]  # Trending
#
#         matchup_dataframe['Best_Player'] = ''
#         matchup_dataframe['Best_Player_FP_Value'] = ''
#         matchup_dataframe['Week_Complete'] = np.nan
#         for week in matchup_dataframe['Week'].unique():
#             print(f'MATCHUP WEEK {week}')
#             # team_df = matchup_dataframe[matchup_dataframe['Week'] == week]
#             for team in matchup_dataframe['Team_Yahoo'].unique():
#                 team_week_df = df_player_list[['Player_HR', 'FP_TOTAL']][(df_player_list['Week'] == week) &
#                                                                          (df_player_list['Team_Yahoo'] == team)]
#                 best_player_df = team_week_df[['Player_HR', 'FP_TOTAL']][
#                     (team_week_df['FP_TOTAL'] == team_week_df['FP_TOTAL'].max())]
#                 if len(best_player_df) > 0:
#                     best_player = best_player_df['Player_HR'].iloc[0]
#                     fp_max = best_player_df['FP_TOTAL'].iloc[0]
#                 else:
#                     best_player = 'NONE'
#                     fp_max = 0
#                 # print(f'{year} Best player for {team} on week {week} was {best_player} with FP Value {fp_max}')
#                 matchup_dataframe.loc[(matchup_dataframe['Week'] == week) &
#                                       (matchup_dataframe['Team_Yahoo'] == team), 'Best_Player'] = best_player
#                 matchup_dataframe.loc[(matchup_dataframe['Week'] == week) &
#                                       (matchup_dataframe['Team_Yahoo'] == team), 'Best_Player_FP_Value'] = fp_max
#
#         matchup_bench_dataframe = matchup_dataframe[matchup_dataframe['STATUS'] == 'BN']
#         matchup_dataframe = matchup_dataframe[matchup_dataframe['STATUS'] == 'ACTIVE']
#
#         matchup_dataframe_expanded.to_csv(f'{root_dir}MATCHUPS_BY_YEAR/{year}_matchups_data_expanded.csv')
#         matchup_dataframe.to_csv(f'{root_dir}MATCHUPS_BY_YEAR/{year}_matchups_data.csv')
#         matchup_bench_dataframe.to_csv(f'{root_dir}MATCHUPS_BY_YEAR/{year}_bench_data.csv')
#
#         consolidated_matchup_df = pd.concat([consolidated_matchup_df, matchup_dataframe])
#         consolidated_bench_df = pd.concat([consolidated_bench_df, matchup_bench_dataframe])
#         consolidated_expanded_df = pd.concat([consolidated_expanded_df, matchup_dataframe_expanded])
#         consolidated_matchup_df.to_csv(f'{root_dir}DATABASES/Consolidated_Matchups_Data.csv', index=False)
#         # consolidated_bench_df.to_csv('MATCHUPS_BY_YEAR/Consolidated_Bench_Data.csv',index=False)
#         # consolidated_expanded_df.to_csv('MATCHUPS_BY_YEAR/Consolidated_Expanded_Data.csv',index=False)
#
# def player_stats_analytics(year,control_file):
#     if control_file['player_stats_analytics_status']:
#         print(f'>>>> [Rundate: {time.ctime()}] Beginning Player Stats analytics for {year}...')
#         df_rosters_full = pd.read_csv('DATABASES/Parkdale_Fantasy_Hockey_Rosters_and_Calcs.csv',low_memory=False)
#         df_rosters_full = df_rosters_full[
#                                          (df_rosters_full['Playoffs']==0)]
#         try:
#             df_plyr_stats_og = pd.read_csv('DATABASES/Player_Stats_Analytics.csv',low_memory=False)
#             df_plyr_stats_res = df_plyr_stats_og[df_plyr_stats_og['Year']!=year]
#             print(f'>>>> [Rundate: {time.ctime()}] Truncated {len(df_plyr_stats_og)-len(df_plyr_stats_res)} Player Stats entries...')
#         except:
#             df_plyr_stats_res = pd.DataFrame()
#             print(f'>>>> [Rundate: {time.ctime()}] Player Stat data does not exist yet...')
#         df_plyr_stats = df_rosters_full[(df_rosters_full['Year']==year)]
#         df_plyr_stats['GAME_COUNT'] = 1
#         df_plyr_stats = df_plyr_stats.groupby(['Year','Player_NST','Team_HR',
#                                                 ],as_index=False)[['FP_TOTAL',
#                                                                    'GAME_COUNT']].sum()
#         df_plyr_stats['AVG_FP_PER_GP'] = df_plyr_stats['FP_TOTAL'] /df_plyr_stats['GAME_COUNT']
#         df_plyr_stats_res = pd.concat([df_plyr_stats_res,df_plyr_stats])
#         df_plyr_stats_res.to_csv('DATABASES/Player_Stats_Analytics.csv',index=False)
#
# def hospital(year,control_file):
#     if control_file['hospital_status']:
#         print(f'>>>> [Rundate: {time.ctime()}] Beginning Hospital analytics for {year}...')
#
#
#         df_rosters_full = pd.read_csv('DATABASES/Parkdale_Fantasy_Hockey_Rosters_and_Calcs.csv', low_memory=False)
#         df_player_stats = pd.read_csv('DATABASES/Player_Stats_Analytics.csv', low_memory=False)
#         df_teams = pd.read_csv(f'TEAMS_METADATA/{year}_teams.csv')
#         df_player_logs = pd.read_csv('DATABASES/LOGS_PLAYERS.csv', low_memory=False)
#         try:
#             df_hospital_og = pd.read_csv('DATABASES/Hospital_Register.csv',low_memory=False)
#             df_hospital_res = df_hospital_og[df_hospital_og['Year']!=year]
#             print(f'>>>> [Rundate: {time.ctime()}] Truncated {len(df_hospital_og)-len(df_hospital_res)} Hospital Register entries...')
#         except:
#             df_hospital_res = pd.DataFrame()
#             print(f'>>>> [Rundate: {time.ctime()}] Hospital data does not exist yet...')
#         # this is going to be massive
#         rosters_list = pd.DataFrame()
#         df_schedule = pd.read_csv(f'NHL_SCHEDULES/{year}_NHL_Schedule.csv')
#         df_rosters_year = df_rosters_full[df_rosters_full['Year'] == year]
#         for file_name in glob.glob(f'ONLINE_PARSED_DATA/YH_ROSTERS/{year}/*.csv'):
#             #date = file_name.split('\\')[1].split('_')[2].split('.')[0]
#             x = pd.read_csv(file_name, low_memory=False)
#             x = x[(x['Selected Position'] == 'IR') | (x['Selected Position'] == 'IR+')]
#             for name in x['Name'].unique():
#                 games_injured = df_rosters_year[df_rosters_year['Player_NST'] == name]
#             rosters_list = pd.concat([rosters_list, x], axis=0)
#
#         full_data = rosters_list.merge(df_player_stats, how='left', left_on=['Year', 'Name'], right_on=['Year', 'Player_NST'])
#         full_data['GM'] = full_data['Team'].apply(lambda x: df_teams[df_teams['Team_Name']==x]['GM_Name'].iloc[0])
#         df_hospital_res = pd.concat([df_hospital_res,full_data])
#         df_hospital_res.to_csv('DATABASES/Hospital_Register.csv', index=False)
#
#
# def google_sheets_trunc_and_load(control_file):
#     if control_file['cloud_upload_status']:
#         print(f'>>>> [Rundate: {time.ctime()}] Beginning cloud upload ...')
#         # define the scope
#         scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
#         # add credentials to the account
#         creds = ServiceAccountCredentials.from_json_keyfile_name('C:/Users/16472/PycharmProjects/Hockey_FantaPy/hockey-fantasy-409814-27fdf881a0a9.json', scope)
#         # authorize the clientsheet
#         client = gspread.authorize(creds)
#         print(client)
#         table_dict = {
#             'Transactions': 'DATABASES/Parkdale_Fantasy_Hockey_Transactions_and_Drafts.csv',
#             'Draft Analytics': 'DATABASES/Draft_Analytics.csv',
#             'Streamer Analytics': 'DATABASES/Streamer_Analytics.csv',
#             'Keeper Analytics': 'DATABASES/Keeper_Analytics.csv',
#             'FAAB Analytics': 'DATABASES/FAAB_Analytics.csv',
#             'Matchup Data': 'DATABASES/Consolidated_Matchups_Data.csv',
#             'Bench Data': 'DATABASES/Bench_Analytics.csv',
#             'Forgotten Data': 'DATABASES/Forgotten_Analytics.csv',
#             'Player Stats Analytics': 'DATABASES/Player_Stats_Analytics.csv',
#             'Hospital Register': 'DATABASES/Hospital_Register.csv',
#             'Player Logs':'DATABASES/LOGS_PLAYERS.csv',
#             'Team Logs': 'DATABASES/LOGS_TEAMS.csv',
#             'Stat Logs': 'DATABASES/LOGS_STATS.csv',
#             'Trade Analytics':'DATABASES/Trade_Analytics.csv',
#             'Ownership Analytics': 'DATABASES/Ownership_Analytics.csv',
#             'Loyalty Analytics': 'DATABASES/Loyalty_Analytics.csv'
#         }
#
#         for table in table_dict.keys():
#             print(f'>>>> [Rundate: {time.ctime()}] {table} Upload')
#             # get the instance of the Spreadsheet
#             sheet = client.open(table)
#             table_df = pd.read_csv(root_dir+table_dict[table], low_memory=False)
#             # get the first sheet of the Spreadsheet
#             sheet_instance = sheet.get_worksheet(0)
#             print(sheet_instance.col_count)
#             set_with_dataframe(worksheet=sheet_instance, dataframe=table_df, include_index=False,
#                                include_column_header=True, resize=True)
#
#     else:
#         print(f'>>>> [Rundate: {time.ctime()}] Not uploading data to cloud...')
#
#
# def trade_analytics( control_file):
#     if control_file['trade_analytics_status']:
#
#         print(f'>>>> [Rundate: {time.ctime()}] Beginning Trade analytics...')
#
#         df_teams = pd.read_csv(f'TEAMS_METADATA/all_teams.csv')
#         df_rosters_full = pd.read_csv('DATABASES/Parkdale_Fantasy_Hockey_Rosters_and_Calcs.csv', low_memory=False)
#         df_rosters_full = df_rosters_full[(df_rosters_full['Selected Position'] != 'BN') &
#                                           (df_rosters_full['Selected Position'] != 'IR+') &
#                                           (df_rosters_full['Playoffs'] == 0)]
#         df_transactions_full = pd.read_csv('DATABASES/Parkdale_Fantasy_Hockey_Transactions_and_Drafts.csv', low_memory=False)
#         df_trades = df_transactions_full[(df_transactions_full['transaction_type'] == 'trade')]
#         df_draft = df_transactions_full[(df_transactions_full['transaction_type'] == 'draft')]
#         trade_counter = 0
#         for trade_id in df_trades['transaction_id'].unique():
#             trade_counter += 1
#             trade_df = df_trades[df_trades['transaction_id'] == trade_id]
#             year = trade_df.season.iloc[0]
#             week = trade_df.week.iloc[0]
#             date_trade = trade_df.transaction_date.iloc[0]
#             status = trade_df.status.iloc[0]
#             if status == 'vetoed':
#                 continue
#             print(f'Analyzing Trade: {trade_id} for Year {year} Week {week} [{date_trade}] which was: {status}')
#             involved_parties = trade_df['destination'].unique()
#             #print(f'----> Involved parties: {involved_parties}')
#             for c in range(0, len(trade_df)):
#
#                 trade_component = trade_df.iloc[c]
#                 component = trade_component['name']
#                 source = trade_component.source
#                 destination = trade_component.destination
#                 if 'Draft Pick' in component:
#                     # Check if this pick was traded another time this year
#                     if component in df_trades[(df_trades['season'] == year) &
#                                               (df_trades['transaction_id'] != trade_id) &
#                                               (df_trades['transaction_date'] > date_trade)]['name'].unique():
#                         new_trade_id = df_trades[(df_trades['season'] == year) &
#                                                  (df_trades['transaction_id'] != trade_id) &
#                                                  (df_trades['transaction_date'] > date_trade) &
#                                                  (df_trades['name'] == component)]['transaction_id'].unique()
#
#                         #print(f'---->  {component} went from {source} to {destination} but was involved in another trade later! -> {new_trade_id}')
#                     else:
#                         # Let's find the associated draft pick
#                         draft_round = int(component.split(' ')[-3])
#                         next_year = year + 1
#                         draft_id = df_draft[(df_draft['season'] == next_year) &
#                                             (df_draft['draft_round'] == draft_round) &
#                                             (df_draft['destination'] == destination)]['name']
#                         #print(f'---->  {component} went from {source} to {destination}, drafted {draft_id} in round {draft_round}  {next_year} ')
#
#                 else:
#                     player_data = df_rosters_full[(df_rosters_full['Year'] == year) &
#                                                   (df_rosters_full['Player_Yahoo'] == component) &
#                                                   (df_rosters_full['Team_Yahoo'] == source)]
#                     gp = len(player_data)
#                     fp = player_data['FP_TOTAL'].sum()
#                     #print(f'----> {component} went from {source} to {destination} and got {fp} FP in {gp} games in {year}')
#
#         print(f'We have had {trade_counter} trades')
#
# def analytics_logs( control_file):
#     if control_file['logs_status']:
#         print(f'>>>> [Rundate: {time.ctime()}] Beginning Log Slicers...')
#
#
#         df_total = pd.read_csv('DATABASES/Parkdale_Fantasy_Hockey_Rosters_and_Calcs.csv', low_memory=False)
#         df_total.fillna({'GM_Name': 'Free Agency'}, inplace=True)
#         df_total['GP'] = 1
#         df_total['POS'] = df_total['Position_NST'].apply(lambda x: 'F' if (x == 'L' or x == 'R' or x == 'C')
#         else 'D' if x == 'D'
#         else 'G' if x == 'G' else '')
#         df_log = df_total[['Year', 'Player_NST', 'Team_HR', 'POS', 'GM_Name', 'GP', 'FP_TOTAL', 'TOI_NST',
#                            'G_HR', 'Total Assists_NST', 'PPP_HR', 'SHP_HR', '+/-_HR', 'PIM_NST', 'GW_HR',
#                            'S_HR', 'S%_HR', 'HIT_HR', 'BLK_HR', 'WINS_HR', 'LOSSES_HR', 'SV_HR', 'SV%_HR', 'SO_HR',
#                            'GA_HR', 'SA_HR', 'GAA_NST']]
#
#         df_log_groups = df_log.groupby(['Year', 'Player_NST', 'GM_Name', 'POS'], as_index=False)[[
#             'GP',
#             'FP_TOTAL',
#             'TOI_NST',
#             'G_HR',
#             'Total Assists_NST',
#             'PPP_HR',
#             'SHP_HR',
#             '+/-_HR',
#             'PIM_NST',
#             'GW_HR',
#             'S_HR',
#             'HIT_HR',
#             'BLK_HR',
#             'WINS_HR',
#             'LOSSES_HR',
#             'SV_HR',
#             'SO_HR',
#             'GA_HR',
#             'SA_HR',
#         ]].sum()
#
#         df_log_groups['SH%'] = df_log_groups['G_HR'] / df_log_groups['S_HR']
#         df_log_groups['SV%'] = df_log_groups['SV_HR'] / df_log_groups['SA_HR']
#         df_log_groups['GAA'] = df_log_groups['GA_HR'] / df_log_groups['TOI_NST'] * 60
#
#         df_log_groups.to_csv('DATABASES/LOGS_STATS.csv', index=False)
#
#         df_log_teams = df_log.groupby(['Year', 'GM_Name', 'Team_HR'], as_index=False)[[
#             'GP',
#             'FP_TOTAL',
#         ]].sum()
#         df_log_teams = df_log_teams[df_log_teams['GM_Name'] != 'Free Agency']
#         df_log_teams.to_csv('DATABASES/LOGS_TEAMS.csv', index=False)
#
#         df_log_players = df_log.groupby(['Year', 'GM_Name', 'Player_NST'], as_index=False)[[
#             'GP',
#             'FP_TOTAL',
#         ]].sum()
#         df_log_players.head(100)
#         df_log_players = df_log_players[df_log_players['GM_Name'] != 'Free Agency']
#
#         df_log_players.to_csv('DATABASES/LOGS_PLAYERS.csv', index=False)
#
#
# def ownership_analytics(year, control_file):
#     if control_file['ownership_analytics_status']:
#         if int(year) < 2018:
#             print(f'>>>> [Rundate: {time.ctime()}] Not running Ownership analytics for {year}, not ready for it')
#         else:
#             print(f'>>>> [Rundate: {time.ctime()}] Beginning Ownership analytics...')
#
#             try:
#                 df_perc_owned_og = pd.read_csv('DATABASES/Ownership_Analytics.csv', low_memory=False)
#                 df_perc_owned_res = df_perc_owned_og[df_perc_owned_og['Year'].astype(int) != int(year)]
#                 print(
#                     f'>>>> [Rundate: {time.ctime()}] Truncated {len(df_perc_owned_og) - len(df_perc_owned_res)} Ownership Analytics entries...')
#             except:
#                 df_perc_owned_res = pd.DataFrame()
#                 print(f'>>>> [Rundate: {time.ctime()}] Ownership Analytics data does not exist yet...')
#
#             df_all_rosters = pd.DataFrame()
#
#             for file_name in glob.glob(f'ONLINE_PARSED_DATA/YH_ROSTERS/{str(year)}/*.csv'):
#                 df_day_roster = pd.read_csv(file_name)
#                 df_all_rosters = pd.concat([df_all_rosters, df_day_roster])
#
#             df_all_rosters.sort_values(['Name', 'Date'], inplace=True)
#
#             '''
#             I need to do this interpolation for Oct 15th,
#             for whatever reason it didn't run on the 15th so the % owned values are skewed
#             So I am interpolating as needed
#             '''
#             for player in df_all_rosters[df_all_rosters['Date'] == '2024-10-15']['Name'].unique():
#                 try:
#                     perc_val_back = df_all_rosters[(df_all_rosters['Date'] == '2024-10-14') &
#                                                    (df_all_rosters['Name'] == player)]['Percentage Owned'].values[0]
#                 except:
#                     perc_val_back = 0
#                 try:
#                     perc_val_forward = df_all_rosters[(df_all_rosters['Date'] == '2024-10-16') &
#                                                       (df_all_rosters['Name'] == player)]['Percentage Owned'].values[0]
#                 except:
#                     perc_val_forward = 0
#
#                 if perc_val_back == 0 and perc_val_forward == 0:
#                     perc_final = df_all_rosters[(df_all_rosters['Date'] == '2024-10-15') &
#                                                 (df_all_rosters['Name'] == player)]['Percentage Owned'].values[0]
#                 elif perc_val_back == 0 and perc_val_forward != 0:
#                     perc_final = perc_val_forward
#                 elif perc_val_back == 0 and perc_val_forward != 0:
#                     perc_final = perc_val_back
#                 else:
#                     perc_final = (perc_val_forward + perc_val_back) / 2
#                 # print(
#                 #     f'{player} updated to have percentage owned of {perc_final} based on 14th: {perc_val_back} and 16th: {perc_val_forward}')
#                 df_all_rosters.loc[(df_all_rosters['Date'] == '2024-10-15') & (
#                             df_all_rosters['Name'] == player), 'Percentage Owned'] = perc_final
#
#             df_rosters_2 = pd.DataFrame()
#             if int(year) == 2024:
#                 # These are per-day % ownership analytics
#                 for player in df_all_rosters['Name'].unique():
#                     df_player = df_all_rosters[df_all_rosters['Name'] == player]
#                     for gm in df_player['Team'].unique():
#                         df_player_gm = df_player[df_player['Team'] == gm]
#                         df_player_gm['Percentage Owned Delta Alternate'] = df_player_gm['Percentage Owned'].diff()
#                         df_player_gm.fillna({'Percentage Owned Delta Alternate': 0}, inplace=True)
#                         df_player_gm['Cumulative_Ownership_Change']  = df_player_gm['Percentage Owned Delta Alternate'].cumsum().values[-1]
#                         df_player_gm['Absolute_Cumulative_Ownership_Change']  = abs(df_player_gm['Percentage Owned Delta Alternate']).cumsum().values[-1]
#
#
#
#                         df_rosters_2 = pd.concat([df_rosters_2, df_player_gm])
#
#             else:
#                 # These are per-week % ownership analytics
#                 for player in df_all_rosters['Name'].unique():
#                     df_player = df_all_rosters[df_all_rosters['Name'] == player]
#                     for gm in df_player['Team'].unique():
#                         df_player_gm = df_player[df_player['Team'] == gm]
#                         df_player_gm['Percentage Owned Delta Alternate'] = df_player_gm['Percentage Owned'].diff()
#                         df_player_gm.fillna({'Percentage Owned Delta Alternate': 0}, inplace=True)
#                         df_rosters_2 = pd.concat([df_rosters_2, df_player_gm])
#
#
#             # Output!
#             df_perc_owned_res = pd.concat([df_perc_owned_res,df_rosters_2])
#             df_perc_owned_res.to_csv('DATABASES/Ownership_Analytics.csv',index=False)
#
# def loyalty_analytics(year, control_file):
#     if control_file['loyalty_analytics_status']:
#         trans_df = pd.read_csv('DATABASES/Parkdale_Fantasy_Hockey_Transactions_and_Drafts.csv')
#         stats_df = pd.read_csv('DATABASES/Parkdale_Fantasy_Hockey_Rosters_and_Calcs.csv', low_memory=False)
#
#         df_loyalty_res = pd.DataFrame(columns=[
#             'Year',
#             'Player',
#             'GM',
#             'Type',
#             'Date Acquired',
#             'Date Dropped',
#             'GP',
#             'FP',
#             'FP/GP',
#             'GP After',
#             'FP After',
#             'FP/GP After',
#                             'Draft_Picks',
#                             'Keeper_Picks'
#         ])
#         try:
#             df_loyalty_og = pd.read_csv('DATABASES/Loyalty_Analytics.csv', low_memory=False)
#             df_loyalty_og = df_loyalty_og[df_loyalty_og['Year'].astype(int) != int(year)]
#             print(
#                 f'>>>> [Rundate: {time.ctime()}] Truncated {len(df_loyalty_og) - len(df_loyalty_res)} Loyalty Analytics entries...')
#         except:
#
#             df_loyalty_og = pd.DataFrame(columns=[
#                             'Year',
#                             'Player',
#                             'GM',
#                             'Type',
#                             'Date Acquired',
#                             'Date Dropped',
#                             'GP',
#                             'FP',
#                             'FP/GP',
#                             'GP After',
#                             'FP After',
#                             'FP/GP After',
#                             'Draft_Picks',
#                             'Keeper_Picks'
#                         ])
#             print(f'>>>> [Rundate: {time.ctime()}] Loyalty Analytics data does not exist yet...')
#
#         trans_df_year = trans_df[trans_df['season'] == year]
#         stats_df_year = stats_df[stats_df['Year'] == year]
#         number_of_keepers_per = len(trans_df_year[trans_df_year['keeper']=='KEEPER']['draft_round'].unique())
#         number_of_draft_picks_per = len(trans_df_year[trans_df_year['keeper']=='NO']['draft_round'].unique())
#         # Cycle through and figure out if anyone that was drafted / Kept was dropped during the season
#         for player in trans_df_year[trans_df_year['transaction_type'] == 'draft']['name'].unique():
#             gm_drafter = trans_df_year[(trans_df_year['transaction_type'] == 'draft') &
#                                        (trans_df_year['name'] == player)]['destination'].iloc[0]
#             draft_date = trans_df_year[(trans_df_year['transaction_type'] == 'draft') &
#                                        (trans_df_year['name'] == player)]['transaction_date'].iloc[0]
#             draft_round = trans_df_year[(trans_df_year['transaction_type'] == 'draft') &
#                                         (trans_df_year['name'] == player)]['draft_round'].iloc[0]
#             if len(trans_df_year[(trans_df_year['transaction_type'] == 'drop') &
#                                  (trans_df_year['name'] == player) &
#                                  (trans_df_year['source'] == gm_drafter)]) > 0:
#                 drop_date = trans_df_year[(trans_df_year['transaction_type'] == 'drop') &
#                                           (trans_df_year['name'] == player) &
#                                           (trans_df_year['source'] == gm_drafter)]['transaction_date'].iloc[0]
#
#                 stats_df_year_player = stats_df_year[(stats_df_year['Player_Yahoo'] == player) &
#                                                      (stats_df_year['Date_NST'] >= draft_date) &
#                                                      (stats_df_year['Date_NST'] <= drop_date)
#                                                      ]
#                 stats_df_year_player_after = stats_df_year[(stats_df_year['Player_NST'] == player) &
#                                                            (stats_df_year['Date_NST'] > drop_date)
#                                                            ]
#                 gp = len(stats_df_year_player)
#                 fp_earned = round(stats_df_year_player['FP_TOTAL'].sum(), 2)
#                 try:
#                     fp_per_gp = round(fp_earned / gp, 2)
#                 except:
#                     fp_per_gp = np.nan
#                 gp_after = len(stats_df_year_player_after)
#                 fp_earned_after = round(stats_df_year_player_after['FP_TOTAL'].sum(), 2)
#                 try:
#                     fp_per_gp_after = round(fp_earned_after / gp_after, 2)
#                 except:
#                     fp_per_gp_after = np.nan
#                 if trans_df_year[(trans_df_year['transaction_type'] == 'draft') &
#                                  (trans_df_year['name'] == player) &
#                                  (trans_df_year['destination'] == gm_drafter)]['keeper'].iloc[0] == 'KEEPER':
#                     #print(
#                     #    f'!!!! {gm_drafter} dropped KEEPER {player} on {drop_date} - kept for {gp} games, earned {fp_earned} FP [{fp_per_gp} fp/gp] ||| Afterwards, played {gp_after} games & earned {fp_earned_after} FP [{fp_per_gp_after} fp/gp] ')
#                     df_loyalty_res.loc[len(df_loyalty_res)] = (
#                         year,
#                         player,
#                         gm_drafter,
#                         'KEEPER',
#                         draft_date,
#                         drop_date,
#                         gp,
#                         fp_earned,
#                         fp_per_gp,
#                         gp_after,
#                         fp_earned_after,
#                         fp_per_gp_after,
#                         number_of_draft_picks_per,
#                         number_of_keepers_per
#                     )
#                 else:
#                     #print(
#                     #    f'{gm_drafter} dropped draft pick {player} (from round {int(draft_round)}) on {drop_date} - kept for {gp} games, earned {fp_earned} FP [{fp_per_gp} fp/gp] ||| Afterwards, played {gp_after} games & earned {fp_earned_after} FP [{fp_per_gp_after} fp/gp] ')
#                     df_loyalty_res.loc[len(df_loyalty_res)] = (
#                         year,
#                         player,
#                         gm_drafter,
#                         'DRAFT',
#                         draft_date,
#                         drop_date,
#                         gp,
#                         fp_earned,
#                         fp_per_gp,
#                         gp_after,
#                         fp_earned_after,
#                         fp_per_gp_after,
#                         number_of_draft_picks_per,
#                         number_of_keepers_per
#                     )
#         # Output!
#         try: # dang thing depreciated the empty dataframe concatenation
#             df_loyalty_res = pd.concat([df_loyalty_og, df_loyalty_res])
#         except:
#             # pass thru df_loyalty_res as normal
#             pass
#         df_loyalty_res.to_csv('DATABASES/Loyalty_Analytics.csv', index=False)
