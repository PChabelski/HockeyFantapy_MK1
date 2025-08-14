import pandas as pd
import numpy as np
pd.options.display.float_format = '{:,}'.format
pd.set_option('mode.chained_assignment', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)
np.seterr(divide='ignore')
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings('error')
import logging
logging.getLogger("yfpy.query").setLevel(level=logging.INFO)
from libraries import *
import time as time
import os
import json
import yahoo_class

root_dir = os.getcwd()
os.makedirs(root_dir, exist_ok=True)
time_start = time.time()
print("GOOD DAY! FANTASY HOCKEY 2025 VERSION")
with open(f'{root_dir}/MANUAL_DATA/control_daily.json', 'r') as f:
    control_file = json.loads(f.read())


today = (datetime.now()).strftime('%Y-%m-%d')
yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
print(f'Today: {today} >><< Yesterday: {yesterday}')
yearsToCheck = control_file['Years']  # .keys()
run_type = control_file['run_type']
yearsToCheck = [int(x) for x in yearsToCheck.keys() if yearsToCheck[x]['status'] == "RUN"]
# ============================================================================================
test_or_config_run = input("Live system (1) or custom-operation (2)?: ").strip().lower()
if test_or_config_run == '1':
    print("Live system operation. Only run the previous day's worth of data")
    yearsToCheck = [2025]
    date_of_interest = yesterday
    '''
    Define all the code when ready to run the live system.
    This will only run the previous day's worth of data, and will not prompt for any user
    input. It will also not run any of the metadata parsing, as that should be done
    beforehand.
    
    '''

# ============================================================================================
# ============================================================================================

elif test_or_config_run == '2':
    yearsToCheck = input("Enter the years you want to run (comma separated, e.g. 2023,2024): ").strip().split(',')
    print(f"You have selected the following years: {yearsToCheck}")
    yearsToCheck = [int(x.strip()) for x in yearsToCheck if x.strip().isdigit()]
    for year in yearsToCheck:
        # Create the yahoo query object
        print(f'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
        print(f'>> Starting up data parsing for {year}')
        yahoo_instance = yahoo_class.YahooInstance(control_file, root_dir, year)
        dates_to_check = input("Enter the dates you want to run (comma separated, e.g. 2023-01-01,2023-01-02) OR ALL if you want everything in the season: ").strip().split(',')
        if dates_to_check == ['ALL']:
            print('Grabbing all dates in the season...')
            dates_to_check = pd.read_csv(f'{yahoo_instance.current_directory}/NHL_SCHEDULES/{year}_NHL_schedule.csv')['date'].tolist()
        else:
            print('Grabbing specific dates...')
            dates_to_check = [x.strip() for x in dates_to_check if x.strip()]
        print(f'>> Dates to check: {dates_to_check}')
        print(f'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')

        # Baseline metadata parsing - this is the first thing that should be run
        if input("Do you want to run the metadata parsing? (y/n): ").strip().lower() == 'y':
            print("Running metadata parsing...")
            yahoo_instance.METADATA_PARSE_SCHEDULE()
            yahoo_instance.METADATA_YAHOO_TEAMS()
            yahoo_instance.METADATA_PLAYERS()
            yahoo_instance.METADATA_MATCHUPS()
        else:
            print("Skipping metadata parsing.")
        if input("Do you want to run the transactions parsing? (y/n): ").strip().lower() == 'y':
            print("Running transactions parsing...")
            yahoo_instance.TRANSACTIONS()
        else:
            print("Skipping transactions parsing.")

        # Run the online data parser for the specified dates
        if input("Do you want to run the Hockey Reference data parser for the specified dates? (y/n): ").strip().lower() == 'y':
            yahoo_instance.ONLINE_DATA_PARSER_HOCKEYREFERENCE(dates_to_check)
        else:
            print("Skipping Hockey Reference data parser.")

        if input("Do you want to run the Natural Stat Trick data parser for the specified dates? (y/n): ").strip().lower() == 'y':
            yahoo_instance.ONLINE_DATA_PARSER_NATURALSTATTRICK(dates_to_check)
        else:
            print("Skipping Natural Stat Trick data parser.")

    # # yahoo season week date infmo
    # # yahoo team metadata
    # teams_metadata(year, control_file)
    # # NHL website schedule parser (and yahoo week overlay)
    # # Weekly Matchup metadata parser
    # matchup_metadata(year, control_file)
    # # Yahoo Player Metadata parser
    # player_metadata_parser(year, control_file)
    # # transactions and draft info
    # trans_and_draft(year, control_file)
    # # need to find a way to clean this
    # clean_player_name_parser(control_file['Player Name Cleaner'],yearsToCheck)
    # Get the NST, HR, and Yahoo rosters (raw)
    # # stitch the data here if need be
    # parsed_data_stitcher(year, all_dates, control_file)
    # # calculate all the FP stuff on the stitched datafiles
    # fp_calculator(year, all_dates, control_file)
    # fake_sql_database_creator(year, all_dates, control_file)
    # analytics_logs(control_file)
    # draft_analytics(year, control_file)
    # faab_analytics(year, control_file)
    # keeper_analytics(year, control_file)
    # streamer_analytics(year, control_file)
    # bench_analytics(year, control_file)
    # forgotten_start_analytics(year, control_file)
    # player_stats_analytics(year, control_file)
    # hospital(year, control_file)
    # matchup_consolidator(year, control_file)
    # ownership_analytics(year, control_file)
    # loyalty_analytics(year,control_file)
# ============================================================================================
# Consolidation-level stuff (ie, stitch all the stuff together no matter what years are selected)
# trade_analytics( control_file)
# google_sheets_trunc_and_load(control_file)
else:
    print("Invalid input. Please enter 1 or 2.")
    exit(1)

time_end = time.time()
print(f">>>> [Rundate: {time.ctime()}] Finished in {time_end - time_start}s! See ya!")


