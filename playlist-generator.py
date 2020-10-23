#!/usr/bin/env python3

# Random YouTube video generator script using YouTube Data API v3 written in Python.
# Due to its query parameter this script fetches low viewed (0 - 250 views) YouTube video IDs oftenly.

# In order to enable YouTube APIs on client:
# pip3 install --upgrade google-api-python-client

# import numpy as np
# import pandas as pd

from googleapiclient.discovery import build
import random

DEVELOPER_KEY = 'AIzaSyCXtzn-QdwWUrLbNuRSBWWrtf2dVpsi9Ww'
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'

prefix = ['IMG ', 'IMG_', 'IMG-', 'DSC ']
postfix = [' MOV', '.MOV', ' .MOV']

def youtube_search(n):
  youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=DEVELOPER_KEY)
  if (n > 50 or n < 0):
      print("%s OUT OF RANGE\nUsing default 5\n")
      n = 5
  search_response = youtube.search().list(
    q=random.choice(prefix) + str(random.randint(999, 9999)) + random.choice(postfix),
    part='snippet',
    maxResults=n
  ).execute()

  videos = []

  for search_result in search_response.get('items', []):
    if search_result['id']['kind'] == 'youtube#video':
      videos.append('%s' % (search_result['id']['videoId']))
  return (videos)

#### MAIN
n_videos = int(input("How many random videos do you want to fetch from YouTube? (0-50) "))
ids = []
ids = youtube_search(n_videos)
print("YouTube video id fetched:\n ", ids)
with open("url_library.txt", 'w') as file:
    for line in ids:
        file.write(line)
