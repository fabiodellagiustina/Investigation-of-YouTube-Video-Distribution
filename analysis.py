#!/usr/bin/env python3

import os
import glob
# from prettytable import PrettyTable
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
# from zipfile import ZipFile
import sqlite3

DB_COLUMNS = ['ID', 'Url', 'CacheUrl', 'CacheServerDelay', 'IP', 'ASNumber', 'PingMin', 'TimeToGetFirstByte', 'RedirectUrl', 'StatusCode']
VANTAGE_POINT_HOSTNAME_LIST = {'us-west-1': 'ip-172-31-14-163', 'us-east-1': 'ip-172-31-85-175', 'eu-central-1': 'ip-172-31-43-135', 'ap-south-1': 'ip-172-31-13-30', 'ap-northeast-1': 'ip-172-31-4-22'}

def retrieve_database(db):
    conn = sqlite3.connect(db)
    # df = pd.read_sql_table('abc', conn)
    table_date = db.split('/')[-1].split('youtube.')[1].split('.pytomo')[0].replace('-', '_').replace('.', '_')
    table_name = 'pytomo_crawl_' + table_date
    print(",".join(DB_COLUMNS))
    df = pd.read_sql_query('SELECT ' + ",".join(DB_COLUMNS) + ' FROM ' + table_name, conn)
    print(df)
    conn.close()
    return df


#### MAIN

# zips_orig-folder = 'new/'
zips_dest_folder = 'new/'
# # retrieve all measurement zips available
# zips_path = glob.glob(zips_orig-folder + 'ip-*.tbz')
# # extract all zip files in dest folder
# for zip_file in zips_path:
#     with ZipFile(zip_file, 'r') as zip_ref:
#         zip_ref.extractall(zips_dest_folder)

# create empty vantage points dataframes
df_us_west_1 = pd.DataFrame(columns=DB_COLUMNS)
df_us_east_1 = pd.DataFrame(columns=DB_COLUMNS)
df_eu_central_1 = pd.DataFrame(columns=DB_COLUMNS)
df_ap_south_1 = pd.DataFrame(columns=DB_COLUMNS)
df_ap_northeast_1 = pd.DataFrame(columns=DB_COLUMNS)

# retrieve all databases available
dbs_path = glob.glob(zips_dest_folder + 'ip-*.db')
print(dbs_path)
for db in dbs_path:
    df = retrieve_database(db).reset_index(drop=True)
    # add retrieved dataframe into appropriate vantage point general dataframe
    vantage_hostname = db.split('/')[-1].split('.youtube')[0]             # original: /../ip-172-31-14-163.youtube.2020-10-28.06_00_01.pytomo_database.db
    for v, hostname in VANTAGE_POINT_HOSTNAME_LIST.items():
        if hostname == vantage_hostname:
            df_select = v
            print(df_select)
            # break
    if df_select == 'us-west-1':
        df_us_west_1 = pd.concat([df_us_west_1, df], axis=0)
    elif df_select == 'us-east-1':
        df_us_east_1 = pd.concat([df_us_east_1, df], axis=0)
    elif df_select == 'eu-central-1':
        df_eu_central_1 = pd.concat([df_eu_central_1, df], axis=0)
    elif df_select == 'ap-south-1':
        df_ap_south_1 = pd.concat([df_ap_south_1, df], axis=0)
    elif df_select == 'ap-northeast-1':
        df_ap_northeast_1 = pd.concat([df_ap_northeast_1, df], axis=0)

print(df_us_west_1)
print(df_us_east_1)
print(df_eu_central_1)
print(df_ap_south_1)
print(df_ap_northeast_1)

# sanitize all vantage point general dataframes
