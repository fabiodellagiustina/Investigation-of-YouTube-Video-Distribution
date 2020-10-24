#!/usr/bin/env python3

import googleapiclient.discovery
import datetime

videos_per_query = 5

api_service_name = 'youtube'
api_version = 'v3'
api_developer_key = 'AIzaSyCXtzn-QdwWUrLbNuRSBWWrtf2dVpsi9Ww'
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_developer_key)

def youtube_video_search(publishedAfter=None, publishedBefore=None):
    video_ids = []

    # popular videos
    request = youtube.search().list(
        part='snippet',
        maxResults=videos_per_query,
        order='viewCount',
        publishedAfter=publishedAfter,
        publishedBefore=publishedBefore,
        type='video',
        videoDuration='medium'
    )
    response = request.execute()
    for item in response.get('items', []):
        video_ids.append('%s' % item['id']['videoId'])

    # not popular videos
    request = youtube.search().list(
        part='snippet',
        maxResults=videos_per_query,
        publishedAfter=publishedAfter,
        publishedBefore=publishedBefore,
        q='IMG|DSC|MOV',
        type='video',
        videoDuration='medium'
    )
    response = request.execute()
    for item in response.get('items', []):
        video_ids.append('%s' % item['id']['videoId'])

    return video_ids

def main():
    now = datetime.datetime.utcnow().replace(microsecond=0)
    a_day_ago = (now - datetime.timedelta(days = 1)).isoformat() + 'Z'
    a_week_ago = (now - datetime.timedelta(days = 7)).isoformat() + 'Z'
    a_month_ago = (now - datetime.timedelta(days = 30)).isoformat() + 'Z'
    a_year_ago = (now - datetime.timedelta(days = 365)).isoformat() + 'Z'

    video_ids = []
    video_ids.extend(youtube_video_search(publishedAfter=a_day_ago))
    video_ids.extend(youtube_video_search(publishedAfter=a_week_ago, publishedBefore=a_day_ago))
    video_ids.extend(youtube_video_search(publishedAfter=a_month_ago, publishedBefore=a_week_ago))
    video_ids.extend(youtube_video_search(publishedAfter=a_year_ago, publishedBefore=a_month_ago))
    video_ids.extend(youtube_video_search(publishedBefore=a_year_ago))

    with open('video_urls.txt', 'w') as f:
        for video_id in video_ids:
            f.write('https://youtu.be/%s\n' % video_id)

main()
