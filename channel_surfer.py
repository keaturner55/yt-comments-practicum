# -*- coding: utf-8 -*-
"""
Created on Tue Jul 12 21:20:55 2022

@author: keatu
"""

#%%
from apiclient.discovery import build
import pandas as pd
import datetime
import logging
import sqlite3
import json
import logging
logger = logging.getLogger("channel_surfer")

#%%

def get_channel_videos(channel_id, api_key):
    yt = build("youtube", "v3", developerKey = api_key)
    response = yt.search().list(part="snippet", channelId =channel_id, maxResults = 100, order='viewCount').execute()
    
    rdf = pd.DataFrame()
    video_count = 0
    limit = False
    
    if not response:
        logger.ERROR("No response for request")
    while response:
        if limit:
            logger.info("Processed max videos for channel: {}".format(channel_id))
            break
        for item in response["items"]:
            video_count += 1
            if video_count == 101:
                limit = True
                break
            video_dict = item["snippet"]
            video_dict["videoId"] = item['id']['videoId']
            video_dict.pop("thumbnails")
            video_dict.pop("liveBroadcastContent")
            
            rdf = rdf.append(video_dict, ignore_index = True)
        if 'nextPageToken' in response:
            p_token = response['nextPageToken']
            response = yt.search().list(part="snippet", pageToken=p_token, channelId =channel_id, maxResults = 100, order='viewCount').execute()
        else:
            logger.info("Processed {} videos for channel {}".format(video_count, channel_id))
            break
    return rdf
    
#%%

def upload_tosql(cdf, outdb_name):
    #now = datetime.now()
    outcon = sqlite3.connect(outdb_name)
    cdf.astype(str).to_sql("videos",con=outcon)
    outcon.close()
#%%
if __name__ == "__main__":
    start_time = datetime.datetime.now()
    
    channel_id = 'UCBi2mrWuNuyYy4gbM6fU18Q'
    channels = []
    
    print_time = "{}-{}-{}_{}-{}-{}".format(start_time.year, start_time.month, start_time.day,
                  start_time.hour, start_time.minute, start_time.second)
    outdb_name = r"C:\Users\keatu\Regis_archive\practicum2_data\comments.db"
    key_file =  r"C:\Users\keatu\Regis_archive\practicum2_data\resources\api_key.json"
    api_key = ""
    with open(key_file) as f:
        api_key = json.load(f)["key"]
    
    logging.basicConfig(filename="{}.log".format(print_time),format='%(asctime)s %(message)s',
                        datefmt='%m-%d-%Y %I:%M:%S', level = logging.INFO)
    
    logger.info("Analyzing channel: {}".format(channel_id))
    logger.info("Building YT object")
    #yt = build("youtube", "v3", developerKey = api_key)
    logger.info("Retrieving comments")
    rdf = get_channel_videos(channel_id,api_key)
    logger.info("Uploading results to database: {}".format(outdb_name))
    #upload_tosql(cdf, outdb_name)
    logger.info("Finished")
    end_time = datetime.datetime.now()
    logger.info("Total videos: {}".format(len(rdf)))
    logger.info("Total runtime: {}".format((end_time - start_time)))    
