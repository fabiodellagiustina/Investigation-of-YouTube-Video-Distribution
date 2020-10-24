#!/usr/bin/env python3

# Random YouTube video generator script using YouTube Data API v3 written in Python.
# Due to its query parameter this script fetches low viewed (0 - 250 views) YouTube video IDs oftenly.

# In order to enable YouTube APIs on client:
# pip3 install --upgrade google-api-python-client

# import numpy as np
# import pandas as pd

from googleapiclient.discovery import build
import random
from datetime import datetime
from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Nominatim

DEVELOPER_KEY = 'AIzaSyCXtzn-QdwWUrLbNuRSBWWrtf2dVpsi9Ww'
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'
N_VIDEOS_PER_QUERY = 5

prefix = ['IMG ', 'IMG_', 'IMG-', 'DSC ']
postfix = [' MOV', '.MOV', ' .MOV']
location = ['San Jose' , 'Alexandria', 'Frankfurt', 'Mumbai', 'Tokyo']

def findGeocode(city):
    # try and catch is used to overcome
    # the exception thrown by geolocator
    # using geocodertimedout
    try:
        # Specify the user_agent as your
        # app name it should not be none
        geolocator = Nominatim(user_agent="your_app_name")
        return geolocator.geocode(city)
    except GeocoderTimedOut:
        return findGeocode(city)

def youtube_search(n):
  youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=DEVELOPER_KEY)
  search_response = youtube.search().list(
    q=random.choice(prefix) + str(random.randint(999, 9999)) + random.choice(postfix),
    part='snippet',
    maxResults=N_VIDEOS_PER_QUERY,
    type='video'
  ).execute()

  videos = []

  for search_result in search_response.get('items', []):
    if search_result['id']['kind'] == 'youtube#video':
      videos.append('%s' % (search_result['id']['videoId']))
  return (videos)

#### MAIN
ids = []
# time division
now_dt = datetime.utcnow()
now = now_dt.timestamp()
now_dt = now_dt.isoformat("T") + "Z"
print("Current time: %s" % (now))
day_ago = datetime.utcfromtimestamp(now - 86400).isoformat("T") + "Z"
week_ago = datetime.utcfromtimestamp(now - 86400*7).isoformat("T") + "Z"
month_ago = datetime.utcfromtimestamp(now - 86400*30).isoformat("T") + "Z"
year_ago = datetime.utcfromtimestamp(now - 86400*365).isoformat("T") + "Z"
timeframes = [ [None, year_ago], [year_ago, month_ago], [month_ago, week_ago], [week_ago, day_ago], [day_ago, now] ]

for tf in timeframes:
    youtube_search(tf)

for city in location:
    city_code = findGeocode(city)
    city_latitude = city_code.latitude
    city_longitude = city_code.longitude

ids = youtube_search(n_videos)
print("YouTube video id fetched:\n ", ids)
with open("url_library.txt", 'w') as file:
    for line in ids:
        file.write(line)
