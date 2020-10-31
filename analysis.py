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
import datetime

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


def hops_statistics(df_list, cacheurl_list):
    cacheurl_dict_firsthop = dict.fromkeys(cacheurl_list, 0)
    cacheurl_dict_lasthop = dict.fromkeys(cacheurl_list, 0)
    cacheurl_dict_nredirects = dict.fromkeys(cacheurl_list, 0)
    for df in df_list:
        for index, row in df.iterrows():
            # print(row['CacheUrl'])
            if (type(row['CacheUrl']) == list) and (len(row['CacheUrl']) == 1):
                # print(str(row['CacheUrl'][0]))
                cacheurl_dict_firsthop[row['CacheUrl'][0]] += 1
            elif (type(row['CacheUrl']) == list) and (len(row['CacheUrl']) > 1):
                # print(str(row['CacheUrl'][0]))
                # print(str(row['CacheUrl'][-1]))
                cacheurl_dict_firsthop[row['CacheUrl'][0]] += 1
                cacheurl_dict_lasthop[row['CacheUrl'][-1]] += 1
                cacheurl_dict_nredirects[row['CacheUrl'][0]] += len(row['CacheUrl'])

            else:
                print("Error in extraction of hops statistics for Timestamp: %s" %(str(row['ID'])))
    return [cacheurl_dict_firsthop, cacheurl_dict_lasthop, cacheurl_dict_nredirects]

def divide_runs(g):
    col = list(g.columns)
    g1 = pd.DataFrame(columns=col)
    g2 = pd.DataFrame(columns=col)
    for index, row in g.iterrows():
        dt = datetime.datetime.strptime(row['ID'], '%Y-%m-%d %H:%M:%S.%f')
        if dt.day in [27, 28]:
            g1 = g1.append(row, ignore_index=True)
        elif dt.day in [29, 30]:
            g2 = g2.append(row, ignore_index=True)
    print(g1)
    print(g2)
    return [g1, g2]

def cumulative_redirect_stats(name, group, popular_video_list, not_popular_video_list):
    hours_list = [12, 18, 0, 6]
    print(popular_video_list)
    print(group)
    print(group['Url'])
    print(group['Url'].isin(popular_video_list))
    filt_pop_group = group[group['Url'].isin(popular_video_list)]
    filt_not_pop_group = group[group['Url'].isin(not_popular_video_list)]
    print(filt_pop_group)
    print(filt_not_pop_group)
    cacheurl_pop_nredirects = dict.fromkeys(hours_list, 0)
    cacheurl_not_pop_nredirects = dict.fromkeys(hours_list, 0)
    dt_logger = []
    dt_h_previous = -1
    for index, row in filt_pop_group.iterrows():
        dt = datetime.datetime.strptime(row['ID'], '%Y-%m-%d %H:%M:%S.%f')
        cacheurl_pop_nredirects[dt.hour] += len(row['CacheUrl'])
        # save a sample datetime
        if dt.hour != dt_h_previous:
            dt_h_previous = dt.hour
            dt_logger.append(dt.strftime("%Y-%m-%d %H:00"))
    for index, row in filt_not_pop_group.iterrows():
        dt = datetime.datetime.strptime(row['ID'], '%Y-%m-%d %H:%M:%S.%f')
        cacheurl_not_pop_nredirects[dt.hour] += len(row['CacheUrl'])
        # if (type(row['CacheUrl']) == list) and (len(row['CacheUrl']) == 1):
    print(cacheurl_pop_nredirects)
    print(cacheurl_not_pop_nredirects)
    list_of_values = list(zip(cacheurl_pop_nredirects.keys(), cacheurl_pop_nredirects.values(), cacheurl_not_pop_nredirects.values(), dt_logger))
    temp_df = pd.DataFrame(list_of_values, columns=['Hour', 'CumulRedirectsPopular', 'CumulRedirectsNotPopular', 'DateTime'])
    temp_df['Hour'] = temp_df['Hour'].astype(int)
    # temp_df.sort_values('Hour', inplace=True)
    print(temp_df)
    return temp_df

def plot_cumulative_redirects(df, name, r):
    ax = plt.gca()
    x_ticks = np.arange(0, 4, 1)
    plt.xticks(x_ticks)
    # labels = ['12', '18', '0', '6']
    # ax.set_xticklabels(labels)
    # ax.locator_params(axis='y', nbins=4)
    ax.set_xticklabels(df.DateTime, rotation=90)
    df.plot(y='CumulRedirectsPopular', use_index=True, kind = 'line', ax=ax, label='Popular videos')
    df.plot(y='CumulRedirectsNotPopular', use_index=True, kind = 'line', color='red', ax=ax, label='Non-popular videos')
    plt.title('Cumulative Redirects For Serving Videos\nVantage Point: %s (Run %d)' % (name, r))
    plt.xlabel('Datetime')
    plt.ylabel('Cumulated number of redirects')
    # ax.set_xticks(df_redirects_vantage_point.index)
    # ax.set_xticklabels(df_redirects_vantage_point.Hour, rotation=90)
    # plt.yticks(int)
    plt.show()

def plot_cumulative_redirects_test(df1, df2, name):
    for key, value in VANTAGE_POINT_HOSTNAME_LIST.items():  # for name, age in dictionary.iteritems():  (for Python 2.x)
        if name == value:
            availability_zone = key
    figure, axes = plt.subplots(nrows=1, ncols=2)
    figure.set_size_inches(18.5, 10.5, forward=True)
    x_ticks = np.arange(0, 4, 1)
    axes[0].set_xticks(x_ticks)
    labels = ['12', '18', '0', '6']
    # ax.set_xticklabels(labels)
    # ax.locator_params(axis='y', nbins=4)
    axes[0].set_xticklabels(df1.DateTime, rotation=90)
    df1.plot(y='CumulRedirectsPopular', use_index=True, kind = 'line', ax=axes[0], label='Popular videos')
    df1.plot(y='CumulRedirectsNotPopular', use_index=True, kind = 'line', color='red', ax=axes[0], label='Non-popular videos')
    # plt.title('Cumulative Redirects For Serving Videos\nVantage Point: %s (Run %d)' % (name, r))
    # axes[0].set_title('Cumulative Redirects For Serving Videos\nVantage Point: %s (Run 1)' % (name))
    # axes[0].set_xlabel('Datetime')
    # plt.ylabel('# redirect (cumulative)')
    figure.text(0.04, 0.5, '# redirect (cumulative)', va='center', rotation='vertical', fontsize=12)
    # axes[0].set_ylabel('# redirect (cumulative)')

    # axes[1].xticks(x_ticks)
    # labels = ['12', '18', '0', '6']
    # ax.set_xticklabels(labels)
    # ax.locator_params(axis='y', nbins=4)
    axes[1].set_xticks(x_ticks)
    axes[1].set_xticklabels(df2.DateTime, rotation=90)
    df2.plot(y='CumulRedirectsPopular', use_index=True, kind = 'line', ax=axes[1], label='Popular videos')
    df2.plot(y='CumulRedirectsNotPopular', use_index=True, kind = 'line', color='red', ax=axes[1], label='Non-popular videos')
    # axes[1].set_xlabel('Datetime')
    # axes[1].set_ylabel('# redirect (cumulative)')
    figure.suptitle('Vantage point: %s' % (availability_zone))
    axes[1].set_title('Second run')
    axes[0].set_title('First run')
    # axes[1].xlabel('Datetime')
    # axes[1].ylabel('Cumulated number of redirects')
    custom_ylim = (0, 60)
    plt.setp(axes, ylim=custom_ylim)
    plt.savefig('figures/' + availability_zone + '_cumulative_redirect.png')
    plt.show()

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
df_us_west_1_by_url = pd.DataFrame(columns=DB_COLUMNS)
df_us_west_1_by_url.drop(['RedirectUrl', 'StatusCode'], axis=1, inplace=True)
df_us_east_1_by_url = pd.DataFrame(columns=DB_COLUMNS)
df_us_east_1_by_url.drop(['RedirectUrl', 'StatusCode'], axis=1, inplace=True)
df_eu_central_1_by_url = pd.DataFrame(columns=DB_COLUMNS)
df_eu_central_1_by_url.drop(['RedirectUrl', 'StatusCode'], axis=1, inplace=True)
df_ap_south_1_by_url = pd.DataFrame(columns=DB_COLUMNS)
df_ap_south_1_by_url.drop(['RedirectUrl', 'StatusCode'], axis=1, inplace=True)
df_ap_northeast_1_by_url = pd.DataFrame(columns=DB_COLUMNS)
df_ap_northeast_1_by_url.drop(['RedirectUrl', 'StatusCode'], axis=1, inplace=True)

# create empty total records dataframe
df_total = pd.DataFrame(columns=DB_COLUMNS)
df_total_by_url = pd.DataFrame(columns=DB_COLUMNS)
# create empty vantage point dataframes for totals
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



    # group retrieved dataframe by 'Url'
    df_group_by_url = group_dataframe(df)
    # add retrieved dataframe into vantage point daframes for totals, and grouped dataframe into appropriate vantage point general dataframe
    vantage_hostname = db.split('/')[-1].split('.youtube')[0]             # original: /../ip-172-31-14-163.youtube.2020-10-28.06_00_01.pytomo_database.db
    for v, hostname in VANTAGE_POINT_HOSTNAME_LIST.items():
        if hostname == vantage_hostname:
            df_select = v
    if df_select == 'us-west-1':
        df_us_west_1 = pd.concat([df_us_west_1, df], axis=0)
        df_us_west_1_by_url = pd.concat([df_us_west_1_by_url, df_group_by_url], axis=0)
    elif df_select == 'us-east-1':
        df_us_east_1 = pd.concat([df_us_east_1, df], axis=0)
        df_us_east_1_by_url = pd.concat([df_us_east_1_by_url, df_group_by_url], axis=0)
    elif df_select == 'eu-central-1':
        df_eu_central_1 = pd.concat([df_eu_central_1, df], axis=0)
        df_eu_central_1_by_url = pd.concat([df_eu_central_1_by_url, df_group_by_url], axis=0)
    elif df_select == 'ap-south-1':
        df_ap_south_1 = pd.concat([df_ap_south_1, df], axis=0)
        df_ap_south_1_by_url = pd.concat([df_ap_south_1_by_url, df_group_by_url], axis=0)
    elif df_select == 'ap-northeast-1':
        df_ap_northeast_1 = pd.concat([df_ap_northeast_1, df], axis=0)
        df_ap_northeast_1_by_url = pd.concat([df_ap_northeast_1_by_url, df_group_by_url], axis=0)

# # sanitize all vantage point general dataframes
# # df_us_west_1_by_url
# print("Prova")
# print(df_us_west_1_by_url)
# df_us_west_1_by_url.dropna(subset=['StatusCode'], inplace=True)
# print(df_us_west_1_by_url)
# print(df_us_west_1_by_url['TimeTogetFirstByte'].isnull())
# drop_index = df_us_west_1_by_url[(df_us_west_1_by_url['TimeTogetFirstByte'].isnull()) & (df_us_west_1_by_url['StatusCode'] == 200.0)].index
# print(drop_index)
# print(df_us_west_1_by_url.iloc[drop_index])
# df_us_west_1_by_url.drop(drop_index , inplace=True)
# # df_us_east_1_by_url
# df_us_east_1_by_url.dropna(subset=['StatusCode'], inplace=True)
# drop_index = df_us_east_1_by_url[(df_us_east_1_by_url['TimeTogetFirstByte'].isnull()) & (df_us_east_1_by_url['StatusCode'] == 200.0)].index
# print(drop_index)
# df_us_east_1_by_url.drop(drop_index , inplace=True)
# # df_eu_central_1_by_url
# df_eu_central_1_by_url.dropna(subset=['StatusCode'], inplace=True)
# drop_index = df_eu_central_1_by_url[(df_eu_central_1_by_url['TimeTogetFirstByte'].isnull()) & (df_eu_central_1_by_url['StatusCode'] == 200.0)].index
# print(drop_index)
# df_eu_central_1_by_url.drop(drop_index , inplace=True)
# # df_ap_south_1_by_url
# df_ap_south_1_by_url.dropna(subset=['StatusCode'], inplace=True)
# drop_index = df_ap_south_1_by_url[(df_ap_south_1_by_url['TimeTogetFirstByte'].isnull()) & (df_ap_south_1_by_url['StatusCode'] == 200.0)].index
# print(drop_index)
# df_ap_south_1_by_url.drop(drop_index , inplace=True)
# # df_ap_northeast_1_by_url
# df_ap_northeast_1_by_url.dropna(subset=['StatusCode'], inplace=True)
# drop_index = df_ap_northeast_1_by_url[(df_ap_northeast_1_by_url['TimeTogetFirstByte'].isnull()) & (df_ap_northeast_1_by_url['StatusCode'] == 200.0)].index
# print(drop_index)
# df_ap_northeast_1_by_url.drop(drop_index , inplace=True)


# extract records for uploaded video and mark vantage point
vantage_point = []
columns_uploaded = DB_COLUMNS
df_uploaded = pd.DataFrame(columns=DB_COLUMNS)
df_uploaded.drop(['RedirectUrl', 'StatusCode'], axis=1, inplace=True)
# df_uploaded = pd.DataFrame(columns=list(itertools.chain(DB_COLUMNS, ['VantagePoint']))
# df_us_west_1_by_url
subdf = df_us_west_1_by_url[df_us_west_1_by_url['Url'] == URL_UPLOADED_VIDEO]
vantage_point.extend([VANTAGE_POINT_HOSTNAME_LIST['us-west-1']]*len(subdf.index))
df_us_west_1_by_url.drop(subdf.index, inplace=True)
df_uploaded = pd.concat([df_uploaded, subdf], axis=0, ignore_index=True)
# df_us_east_1_by_url
subdf = df_us_east_1_by_url[df_us_east_1_by_url['Url'] == URL_UPLOADED_VIDEO]
vantage_point.extend([VANTAGE_POINT_HOSTNAME_LIST['us-east-1']]*len(subdf.index))
df_us_east_1_by_url.drop(subdf.index, inplace=True)
df_uploaded = pd.concat([df_uploaded, subdf], axis=0, ignore_index=True)
# df_eu_central_1_by_url
subdf = df_eu_central_1_by_url[df_eu_central_1_by_url['Url'] == URL_UPLOADED_VIDEO]
vantage_point.extend([VANTAGE_POINT_HOSTNAME_LIST['eu-central-1']]*len(subdf.index))
df_eu_central_1_by_url.drop(subdf.index, inplace=True)
df_uploaded = pd.concat([df_uploaded, subdf], axis=0, ignore_index=True)
# df_ap_south_1_by_url
subdf = df_ap_south_1_by_url[df_ap_south_1_by_url['Url'] == URL_UPLOADED_VIDEO]
vantage_point.extend([VANTAGE_POINT_HOSTNAME_LIST['ap-south-1']]*len(subdf.index))
df_ap_south_1_by_url.drop(subdf.index, inplace=True)
df_uploaded = pd.concat([df_uploaded, subdf], axis=0, ignore_index=True)
# df_ap_south_1_by_url
subdf = df_ap_northeast_1_by_url[df_ap_northeast_1_by_url['Url'] == URL_UPLOADED_VIDEO]
vantage_point.extend([VANTAGE_POINT_HOSTNAME_LIST['ap-northeast-1']]*len(subdf.index))
df_ap_northeast_1_by_url.drop(subdf.index, inplace=True)
df_uploaded = pd.concat([df_uploaded, subdf], axis=0, ignore_index=True)


df_uploaded['VantagePoint'] = vantage_point

# pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', -1)

# sorting by ID and reset index
df_us_west_1_by_url.sort_values('ID', inplace=True)
df_us_west_1_by_url.reset_index(inplace=True, drop=True)
df_us_east_1_by_url.sort_values('ID', inplace=True)
df_us_east_1_by_url.reset_index(inplace=True, drop=True)
df_eu_central_1_by_url.sort_values('ID', inplace=True)
df_eu_central_1_by_url.reset_index(inplace=True, drop=True)
df_ap_south_1_by_url.sort_values('ID', inplace=True)
df_ap_south_1_by_url.reset_index(inplace=True, drop=True)
df_ap_northeast_1_by_url.sort_values('ID', inplace=True)
df_ap_northeast_1_by_url.reset_index(inplace=True, drop=True)
df_uploaded.sort_values('ID', inplace=True)
df_uploaded.reset_index(inplace=True, drop=True)
print(df_us_west_1_by_url)
print(df_us_east_1_by_url)
print(df_eu_central_1_by_url)
print(df_ap_south_1_by_url)
print(df_ap_northeast_1_by_url)
print(df_uploaded)


### Look for similarieties Url <-> IP from different vantage points (NOTE: video uploaded is not included)
# add vantage point byurl dataframes into total records (groupedbyurl) dataframe
vantage_point = []
df_total_by_url = pd.concat([df_total_by_url, df_us_west_1_by_url], axis = 0, ignore_index=True)
vantage_point.extend([VANTAGE_POINT_HOSTNAME_LIST['us-west-1']]*len(df_us_west_1_by_url.index))
df_total_by_url = pd.concat([df_total_by_url, df_us_east_1_by_url], axis = 0, ignore_index=True)
vantage_point.extend([VANTAGE_POINT_HOSTNAME_LIST['us-east-1']]*len(df_us_east_1_by_url.index))
df_total_by_url = pd.concat([df_total_by_url, df_eu_central_1_by_url], axis = 0, ignore_index=True)
vantage_point.extend([VANTAGE_POINT_HOSTNAME_LIST['eu-central-1']]*len(df_eu_central_1_by_url.index))
df_total_by_url = pd.concat([df_total_by_url, df_ap_south_1_by_url], axis = 0, ignore_index=True)
vantage_point.extend([VANTAGE_POINT_HOSTNAME_LIST['ap-south-1']]*len(df_ap_south_1_by_url.index))
df_total_by_url = pd.concat([df_total_by_url, df_ap_northeast_1_by_url], axis = 0, ignore_index=True)
vantage_point.extend([VANTAGE_POINT_HOSTNAME_LIST['ap-northeast-1']]*len(df_ap_northeast_1_by_url.index))
df_total_by_url['VantagePoint'] = vantage_point
df_total_by_url.drop(['RedirectUrl', 'StatusCode'], axis=1, inplace=True)
# groupby url
df_total_by_url_grouped = df_total_by_url.groupby('Url')
print("Table for similarieties Url <-> IP lookup between different vantage points")
for name, group in df_total_by_url_grouped:
    print(name)
    print(group[['ID', 'Url', 'IP', 'VantagePoint']])
    print("\n")


### Cumulative redirect comparison between popular and non-popular videos, by vantage point
popular_video_list = []
with open('popular_video_urls.txt', 'rt') as f:
    for line in f.read().splitlines():
        popular_video_list.append(line)
not_popular_video_list = []
with open('not_popular_video_urls.txt', 'rt') as f:
    for line in f.read().splitlines():
        not_popular_video_list.append(line)
df_total_by_url_by_vantage_point = df_total_by_url.groupby('VantagePoint')
for name, group in df_total_by_url_by_vantage_point:
    group_run_1, group_run_2 = divide_runs(group)
    df_redirects_vantage_point_run_1 = cumulative_redirect_stats(name, group_run_1, popular_video_list, not_popular_video_list)
    # plot_cumulative_redirects(df_redirects_vantage_point_run_1, name, 1)
    df_redirects_vantage_point_run_2 = cumulative_redirect_stats(name, group_run_2, popular_video_list, not_popular_video_list)
    # plot_cumulative_redirects(df_redirects_vantage_point_run_2, name, 2)
    plot_cumulative_redirects_test(df_redirects_vantage_point_run_1, df_redirects_vantage_point_run_2, name)

# sort('Url', 'ID')
# print(df_us_west_1_by_url.sort_values(['Url', 'ID']))

# print(df_us_west_1_by_url[df_us_west_1_by_url['Url'] == 'https://youtu.be/3YxLYPAoUYQ'][['CacheUrl', 'CacheServerDelay', 'IP', 'PingMin', 'TimeTogetFirstByte']])
# print(df_us_east_1_by_url[df_us_east_1_by_url['Url'] == 'https://youtu.be/3YxLYPAoUYQ'][['CacheUrl', 'CacheServerDelay', 'IP', 'PingMin', 'TimeTogetFirstByte']])
# print(df_eu_central_1_by_url[df_eu_central_1_by_url['Url'] == 'https://youtu.be/3YxLYPAoUYQ'][['CacheUrl', 'CacheServerDelay', 'IP', 'PingMin', 'TimeTogetFirstByte']])
# print(df_ap_south_1_by_url[df_ap_south_1_by_url['Url'] == 'https://youtu.be/3YxLYPAoUYQ'][['CacheUrl', 'CacheServerDelay', 'IP', 'PingMin', 'TimeTogetFirstByte']])
# print(df_ap_northeast_1_by_url[df_ap_northeast_1_by_url['Url'] == 'https://youtu.be/3YxLYPAoUYQ'][['CacheUrl', 'CacheServerDelay', 'IP', 'PingMin', 'TimeTogetFirstByte']])
# print(df_uploaded)


# add vantage point dataframes into total records dataframe
vantage_point = []
df_total = pd.concat([df_total, df_us_west_1], axis = 0, ignore_index=True)
vantage_point.extend([VANTAGE_POINT_HOSTNAME_LIST['us-west-1']]*len(df_us_west_1.index))
df_total = pd.concat([df_total, df_us_east_1], axis = 0, ignore_index=True)
vantage_point.extend([VANTAGE_POINT_HOSTNAME_LIST['us-east-1']]*len(df_us_east_1.index))
df_total = pd.concat([df_total, df_eu_central_1], axis = 0, ignore_index=True)
vantage_point.extend([VANTAGE_POINT_HOSTNAME_LIST['eu-central-1']]*len(df_eu_central_1.index))
df_total = pd.concat([df_total, df_ap_south_1], axis = 0, ignore_index=True)
vantage_point.extend([VANTAGE_POINT_HOSTNAME_LIST['ap-south-1']]*len(df_ap_south_1.index))
df_total = pd.concat([df_total, df_ap_northeast_1], axis = 0, ignore_index=True)
vantage_point.extend([VANTAGE_POINT_HOSTNAME_LIST['ap-northeast-1']]*len(df_ap_northeast_1.index))
df_total['VantagePoint'] = vantage_point


## Get first && last hop occurrences per cacheurl among all the dataframes
cacheurl_values = df_total['CacheUrl'].values.ravel()
cacheurl_unique =  pd.unique(cacheurl_values)
first_hop_stats, last_hop_stats, cumulation_first_redirect_stats = hops_statistics([df_us_west_1_by_url, df_us_east_1_by_url, df_eu_central_1_by_url, df_ap_south_1_by_url, df_ap_northeast_1_by_url], cacheurl_unique)


## Association CacheUrl <-> IP analysis
cacheurl_values = df_total['CacheUrl'].values.ravel()
cacheurl_unique =  pd.unique(cacheurl_values)
print("Number unique CacheUrls: ", len(cacheurl_unique))
# print("Values unique CacheUrls: ", cacheurl_unique)
cacheurl_dict = dict.fromkeys(cacheurl_unique, [])
grouped_by_cacheurl = df_total.groupby('CacheUrl')
# grouped_by_cacheurl = df_total.groupby('CacheUrl').size().reset_index(name='counts')
# assign cacheurls with related ip
for name, group in grouped_by_cacheurl:
    unique_ips = pd.unique(group['IP'].values.ravel())
    cacheurl_dict[name] = unique_ips.tolist()
# print("After", cacheurl_dict)
# check 1:1 relationship for cacheurl:ip
cacheurl_ip_rel = []
for key, value in cacheurl_dict.items():
    cacheurl_ip_rel.append(len(value))
print(cacheurl_ip_rel)


## IP address occurrence in serving requests
print(df_total.groupby('IP').size().reset_index(name='counts').sort_values('counts', ascending=False))


## PingMin per CacheUrl per vantage point
grouped_by_vantagepoint_by_cacheurl = df_total.groupby(['VantagePoint', 'CacheUrl'])['PingMin'].min().to_frame(name = 'PingMinGeneral').reset_index()
# df.set_index('VantagePoint', drop=True, inplace=True)
# print(grouped_by_vantagepoint_by_cacheurl)
grouped_by_vantagepoint_by_cacheurl_test = grouped_by_vantagepoint_by_cacheurl.groupby('CacheUrl')
# for key, item in grouped_by_vantagepoint_by_cacheurl_test:
    # print(grouped_by_vantagepoint_by_cacheurl_test.get_group(key), "\n\n")
df_ping_table = pd.DataFrame(columns=itertools.chain(['CacheUrl'], VANTAGE_POINT_HOSTNAME_LIST))
for name, group in grouped_by_vantagepoint_by_cacheurl_test:
    record = dict.fromkeys(itertools.chain(['CacheUrl'], VANTAGE_POINT_HOSTNAME_LIST.values(), ['CounterFirstHop', 'CounterLastHop']), np.nan)
    record['CacheUrl'] = name
    record['CounterFirstHop'] = first_hop_stats.get(name)
    record['CounterLastHop'] = last_hop_stats.get(name)
    record['CumulativeFirstRedir'] = cumulation_first_redirect_stats.get(name)
    for index, row in group.iterrows():
        record[row['VantagePoint']] = row['PingMinGeneral']
    df_ping_table = df_ping_table.append(record, ignore_index=True)
# df_ping_table.set_index('CacheUrl', drop=True, inplace=True)

# print(df_us_west_1)
# print(df_us_east_1)
# print(df_eu_central_1)
# print(df_ap_south_1)
# print(df_ap_northeast_1)

# print(df_total)

# print(df_ping_table.sort_values('CounterFirstHop', ascending=False))
df_ping_table.dropna(axis=0, how='any', inplace=True)
