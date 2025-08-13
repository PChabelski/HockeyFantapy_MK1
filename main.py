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
print(root_dir)
os.makedirs(root_dir, exist_ok=True)
time_start = time.time()
print("GOOD DAY! FANTASY HOCKEY 2025 VERSION")
with open(f'{root_dir}/control_daily.json', 'r') as f:
# with open(f'{root_dir}control_manual.json', 'r') as f:
    control_file = json.loads(f.read())


today = (datetime.now()).strftime('%Y-%m-%d')
yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
print(f'Today: {today} >><< Yesterday: {yesterday}')
yearsToCheck = control_file['Years']  # .keys()
run_type = control_file['run_type']
yearsToCheck = [int(x) for x in yearsToCheck.keys() if yearsToCheck[x]['status'] == "RUN"]
print(control_file['Years'][str(yearsToCheck[0])]['league_id'])
# ============================================================================================
test_or_config_run = input("Is this a full-bore reprocessing run? (y/n): ").strip().lower()
if test_or_config_run == 'y':
    print("Running in test mode. Years are defined manually.")
    yearsToCheck = [2018,2017,2016,2015,2014,2013,2012,2011]  # Only run for the first year in the list
    #yearsToCheck = [2024,2023,2022,2021,2020,2019,2018,2017,2016,2015,2014,2013,2012,2011]  # Only run for the first year in the list


for year in yearsToCheck:

    # Create the yahoo query object

    print(f'>> Starting up data parsing for {year}')
    yahoo_instance = yahoo_class.YahooInstance(control_file, root_dir, year)
    if test_or_config_run == 'y':
        yahoo_instance.METADATA_PARSE_SCHEDULE()
        yahoo_instance.METADATA_YAHOO_TEAMS()
        yahoo_instance.METADATA_PLAYERS()
        yahoo_instance.TRANSACTIONS()
        time.sleep(300)  # Sleep for 5 minutes to avoid hitting API limits too quickly
    else:
        # Baseline metadata parsing - this is the first thing that should be run
        if input("Do you want to run the metadata parsing? (y/n): ").strip().lower() == 'y':
            print("Running metadata parsing...")
            yahoo_instance.METADATA_PARSE_SCHEDULE()
            yahoo_instance.METADATA_YAHOO_TEAMS()
            yahoo_instance.METADATA_PLAYERS()
        else:
            print("Skipping metadata parsing.")
        if input("Do you want to run the transactions parsing? (y/n): ").strip().lower() == 'y':
            print("Running transactions parsing...")
            yahoo_instance.TRANSACTIONS()
        else:
            print("Skipping transactions parsing.")
        if input("Do you want to run the matchups parsing? (y/n): ").strip().lower() == 'y':
            print("Running matchups parsing...")
            yahoo_instance.METADATA_MATCHUPS()
        else:
            print("Skipping matchups parsing.")

    # supplementary data parsing - will require upstream information to be available
    # yahoo_instance.TRANSACTIONS()

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

time_end = time.time()

print(f">>>> [Rundate: {time.ctime()}] Finished in {time_end - time_start}s! See ya!")


