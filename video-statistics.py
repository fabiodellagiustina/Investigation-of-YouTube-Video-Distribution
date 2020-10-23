#!/usr/bin/env python3

import os
import glob
from prettytable import PrettyTable
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pafy

def get_stats(l, keys):
        video = pafy.new(l)
        # id = l.split('v=')[1]
        id = video.videoid
        title = video.title
        rating = video.rating
        views = video.viewcount
        duration = video.duration
        likes = video.likes
        dislikes = video.dislikes
        stream_url = video.streams[0].url
        # stream_url = video.getbest(preftype="mp4").url
        stats = [id, title, rating, views, duration, likes, dislikes, stream_url]
        d = zip(keys, stats)
        return d

#### MAIN
filepath = glob.glob("url-library.txt")
filename = filepath.split('/')[-1]
with open(filepath, 'rt') as url_library:
    url_links = url_library.read().splitlines()
# define columns and create DataFrame
k = ['id', 'title', 'rating', 'views', 'duration', 'likes', 'dislikes', 'stream_url']
df = pd.DataFrame(columns = k)
# extract statistics for each video and populate DataFrame
for link in url_links:
    df = df.append(get_stats(link, k), ignore_index=True)
