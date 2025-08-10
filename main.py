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
for year in yearsToCheck:

    print(f'>> Starting up data parsing for {year}')
    all_dates = chrono_trigger(year, run_type, control_file)
    scheduleParser(year, control_file)
    online_data_parser(year, all_dates, control_file)
    weeks_parser(year, control_file)

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
trade_analytics( control_file)
google_sheets_trunc_and_load(control_file)

time_end = time.time()

print(f">>>> [Rundate: {time.ctime()}] Finished in {time_end - time_start}s! See ya!")


