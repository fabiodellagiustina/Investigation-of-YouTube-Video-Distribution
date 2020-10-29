#!/usr/bin/env python3

import os
import glob
from prettytable import PrettyTable
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from zipfile import ZipFile
from sqlalchemy import create_engine

def retrieve_database(db):
    conn = create_engine(db)
    # df = pd.read_sql_table('abc', conn)
    table_date = db.split('/')[-1].split('youtube.')[1].split('.pytomo')[0].replace('-', '_').replace('.', '_')
    table_name = 'pytomo_crawl_' + table_date
    df = pd.read_sql_query('select ' + ",".join(db_columns) + ' from ' + table_name, conn)
    return df


#### MAIN
db_columns = ['id', 'url', 'cacheurl', 'cacheserverdelay', 'ip', 'asnumber', 'pingmin', 'timetogetfirstbyte', 'redirecturl', 'statuscode']
vantage_list = {'us-west-1': '172.31.14.163', 'us-east-1': '172.31.85.175', 'eu-central-1': '172.31.43.135', 'ap-south-1': '172.31.13.30', 'ap-northeast-1': '172.31.4.22'}

zips_orig-folder = 'new/'
zips_dest-folder = 'extracted/'
# retrieve all measurement zips available
zips_path = glob.glob(zips_orig-folder + 'ip-*.tbz')
# extract all zip files in dest folder
for zip_file in zips_path:
    with ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(zips_dest-folder)

# create empty vantage points dataframes
df_us-west-1 = pd.DataFrame(columns=['id', 'url', 'cacheurl', 'cacheserverdelay', 'ip', 'asnumber', 'pingmin', 'timetogetfirstbyte', 'redirecturl', 'statuscode'])
df_us-east-1 = pd.DataFrame(columns=['id', 'url', 'cacheurl', 'cacheserverdelay', 'ip', 'asnumber', 'pingmin', 'timetogetfirstbyte', 'redirecturl', 'statuscode'])
df_eu-central-1 = pd.DataFrame(columns=['id', 'url', 'cacheurl', 'cacheserverdelay', 'ip', 'asnumber', 'pingmin', 'timetogetfirstbyte', 'redirecturl', 'statuscode'])
df_ap-south-1 = pd.DataFrame(columns=['id', 'url', 'cacheurl', 'cacheserverdelay', 'ip', 'asnumber', 'pingmin', 'timetogetfirstbyte', 'redirecturl', 'statuscode'])
df_ap-northeast-1 = pd.DataFrame(columns=['id', 'url', 'cacheurl', 'cacheserverdelay', 'ip', 'asnumber', 'pingmin', 'timetogetfirstbyte', 'redirecturl', 'statuscode'])

# retrieve all databases available
dbs_path = glob.glob(zips_dest-folder + 'ip-*.db')
for db in dbs_path:
    df = retrieve_database(db)
    # add retrieved dataframe into vantage point appropriate one
    vantage_ip = name.split('/')[-1].split('.youtube')[0].split('ip-')[1].replace('-', '.')
    for v, ip in vantage_list.items():
        if ip == vantage_ip:
            df_select = v
            break
    if df_select == 'us-west-1':
        pd.concat([df_us-west-1, df], ignore_index=True)
    elif df_select == 'us-east-1':
        pd.concat([df_us-east-1, df], ignore_index=True)
    elif df_select == 'eu-central-1':
        pd.concat([df_eu-central-1, df], ignore_index=True)
    elif df_select == 'ap-south-1':
        pd.concat([df_ap-south-1, df], ignore_index=True)
    elif df_select == 'ap-northeast-1':
        pd.concat([df_ap-northeast-1, df], ignore_index=True)
