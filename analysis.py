#!/usr/bin/env python3

import os
import glob
# from prettytable import PrettyTable
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
# from zipfile import ZipFile
import sqlite3
import itertools

DB_COLUMNS = ['ID', 'Url', 'CacheUrl', 'CacheServerDelay', 'IP', 'ASNumber', 'PingMin', 'TimeTogetFirstByte', 'RedirectUrl', 'StatusCode']
VANTAGE_POINT_HOSTNAME_LIST = {'us-west-1': 'ip-172-31-14-163', 'us-east-1': 'ip-172-31-85-175', 'eu-central-1': 'ip-172-31-43-135', 'ap-south-1': 'ip-172-31-13-30', 'ap-northeast-1': 'ip-172-31-4-22'}
URL_UPLOADED_VIDEO = 'https://youtu.be/GDJs8iMzNSc'

def retrieve_database(db):
    conn = sqlite3.connect(db)
    # df = pd.read_sql_table('abc', conn)
    table_date = db.split('/')[-1].split('youtube.')[1].split('.pytomo')[0].replace('-', '_').replace('.', '_')
    table_name = 'pytomo_crawl_' + table_date
    df = pd.read_sql_query('SELECT ' + ",".join(DB_COLUMNS) + ' FROM ' + table_name + ' WHERE TimeTogetFirstByte NOTNULL OR RedirectUrl NOTNULL', conn)
    # print(df)
    conn.close()
    return df

def element_to_row(origin, element):
    origin['CacheUrl'].append(element['CacheUrl'])
    origin['IP'].append(element['IP'])
    origin['ASNumber'].append(element['ASNumber'])
    origin['PingMin'].append(element['PingMin'])
    return origin


def group_dataframe(df):
    temp_df = pd.DataFrame(columns=DB_COLUMNS)
    grouped = df.groupby('Url')
    for name, group in grouped:
        # print(name)
        new_row = dict(zip(DB_COLUMNS, [None, None, [], None, [], [], [], None, None, None]))
        # print(group.head(1)['ID'])
        new_row['ID'] = group.head(1).iloc[0]['ID']
        new_row['Url'] = group.head(1).iloc[0]['Url']
        new_row['CacheServerDelay'] = group.head(1).iloc[0]['CacheServerDelay']
        new_row['TimeTogetFirstByte'] = group.tail(1).iloc[0]['TimeTogetFirstByte']
        for index, row in group.iterrows():
            new_row = element_to_row(new_row, row)
        temp_df = temp_df.append(new_row, ignore_index=True)
    temp_df.drop(['RedirectUrl', 'StatusCode'], axis=1, inplace=True)
    return temp_df


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
df_us_west_1.drop(['RedirectUrl', 'StatusCode'], axis=1, inplace=True)
df_us_east_1 = pd.DataFrame(columns=DB_COLUMNS)
df_us_east_1.drop(['RedirectUrl', 'StatusCode'], axis=1, inplace=True)
df_eu_central_1 = pd.DataFrame(columns=DB_COLUMNS)
df_eu_central_1.drop(['RedirectUrl', 'StatusCode'], axis=1, inplace=True)
df_ap_south_1 = pd.DataFrame(columns=DB_COLUMNS)
df_ap_south_1.drop(['RedirectUrl', 'StatusCode'], axis=1, inplace=True)
df_ap_northeast_1 = pd.DataFrame(columns=DB_COLUMNS)
df_ap_northeast_1.drop(['RedirectUrl', 'StatusCode'], axis=1, inplace=True)

# retrieve all databases available
dbs_path = glob.glob(zips_dest_folder + 'ip-*.db')
for db in dbs_path:
    df = retrieve_database(db).reset_index(drop=True)
    # groupby 'Url'
    df = group_dataframe(df)
    # print(df)
    # add retrieved dataframe into appropriate vantage point general dataframe
    vantage_hostname = db.split('/')[-1].split('.youtube')[0]             # original: /../ip-172-31-14-163.youtube.2020-10-28.06_00_01.pytomo_database.db
    for v, hostname in VANTAGE_POINT_HOSTNAME_LIST.items():
        if hostname == vantage_hostname:
            df_select = v
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

# # sanitize all vantage point general dataframes
# # df_us_west_1
# print("Prova")
# print(df_us_west_1)
# df_us_west_1.dropna(subset=['StatusCode'], inplace=True)
# print(df_us_west_1)
# print(df_us_west_1['TimeTogetFirstByte'].isnull())
# drop_index = df_us_west_1[(df_us_west_1['TimeTogetFirstByte'].isnull()) & (df_us_west_1['StatusCode'] == 200.0)].index
# print(drop_index)
# print(df_us_west_1.iloc[drop_index])
# df_us_west_1.drop(drop_index , inplace=True)
# # df_us_east_1
# df_us_east_1.dropna(subset=['StatusCode'], inplace=True)
# drop_index = df_us_east_1[(df_us_east_1['TimeTogetFirstByte'].isnull()) & (df_us_east_1['StatusCode'] == 200.0)].index
# print(drop_index)
# df_us_east_1.drop(drop_index , inplace=True)
# # df_eu_central_1
# df_eu_central_1.dropna(subset=['StatusCode'], inplace=True)
# drop_index = df_eu_central_1[(df_eu_central_1['TimeTogetFirstByte'].isnull()) & (df_eu_central_1['StatusCode'] == 200.0)].index
# print(drop_index)
# df_eu_central_1.drop(drop_index , inplace=True)
# # df_ap_south_1
# df_ap_south_1.dropna(subset=['StatusCode'], inplace=True)
# drop_index = df_ap_south_1[(df_ap_south_1['TimeTogetFirstByte'].isnull()) & (df_ap_south_1['StatusCode'] == 200.0)].index
# print(drop_index)
# df_ap_south_1.drop(drop_index , inplace=True)
# # df_ap_northeast_1
# df_ap_northeast_1.dropna(subset=['StatusCode'], inplace=True)
# drop_index = df_ap_northeast_1[(df_ap_northeast_1['TimeTogetFirstByte'].isnull()) & (df_ap_northeast_1['StatusCode'] == 200.0)].index
# print(drop_index)
# df_ap_northeast_1.drop(drop_index , inplace=True)


# extract records for uploaded video and mark vantage point
vantage_point = []
columns_uploaded = DB_COLUMNS
df_uploaded = pd.DataFrame(columns=DB_COLUMNS)
df_uploaded.drop(['RedirectUrl', 'StatusCode'], axis=1, inplace=True)
# df_uploaded = pd.DataFrame(columns=list(itertools.chain(DB_COLUMNS, ['VantagePoint']))
# df_us_west_1
subdf = df_us_west_1[df_us_west_1['Url'] == URL_UPLOADED_VIDEO]
vantage_point.extend([VANTAGE_POINT_HOSTNAME_LIST['us-west-1']]*len(subdf.index))
df_us_west_1.drop(subdf.index, inplace=True)
df_uploaded = pd.concat([df_uploaded, subdf], axis=0, ignore_index=True)
# df_us_east_1
subdf = df_us_east_1[df_us_east_1['Url'] == URL_UPLOADED_VIDEO]
vantage_point.extend([VANTAGE_POINT_HOSTNAME_LIST['us-east-1']]*len(subdf.index))
df_us_east_1.drop(subdf.index, inplace=True)
df_uploaded = pd.concat([df_uploaded, subdf], axis=0, ignore_index=True)
# df_eu_central_1
subdf = df_eu_central_1[df_eu_central_1['Url'] == URL_UPLOADED_VIDEO]
vantage_point.extend([VANTAGE_POINT_HOSTNAME_LIST['eu-central-1']]*len(subdf.index))
df_eu_central_1.drop(subdf.index, inplace=True)
df_uploaded = pd.concat([df_uploaded, subdf], axis=0, ignore_index=True)
# df_ap_south_1
subdf = df_ap_south_1[df_ap_south_1['Url'] == URL_UPLOADED_VIDEO]
vantage_point.extend([VANTAGE_POINT_HOSTNAME_LIST['ap-south-1']]*len(subdf.index))
df_ap_south_1.drop(subdf.index, inplace=True)
df_uploaded = pd.concat([df_uploaded, subdf], axis=0, ignore_index=True)
# df_ap_south_1
subdf = df_ap_northeast_1[df_ap_northeast_1['Url'] == URL_UPLOADED_VIDEO]
vantage_point.extend([VANTAGE_POINT_HOSTNAME_LIST['ap-northeast-1']]*len(subdf.index))
df_ap_northeast_1.drop(subdf.index, inplace=True)
df_uploaded = pd.concat([df_uploaded, subdf], axis=0, ignore_index=True)


df_uploaded['VantagePoint'] = vantage_point

# sorting by ID and reset index
df_us_west_1.sort_values('ID', inplace=True)
df_us_west_1.reset_index(inplace=True, drop=True)
df_us_east_1.sort_values('ID', inplace=True)
df_us_east_1.reset_index(inplace=True, drop=True)
df_eu_central_1.sort_values('ID', inplace=True)
df_eu_central_1.reset_index(inplace=True, drop=True)
df_ap_south_1.sort_values('ID', inplace=True)
df_ap_south_1.reset_index(inplace=True, drop=True)
df_ap_northeast_1.sort_values('ID', inplace=True)
df_ap_northeast_1.reset_index(inplace=True, drop=True)
df_uploaded.sort_values('ID', inplace=True)
df_uploaded.reset_index(inplace=True, drop=True)
print(df_us_west_1)
print(df_us_east_1)
print(df_eu_central_1)
print(df_ap_south_1)
print(df_ap_northeast_1)
print(df_uploaded)
