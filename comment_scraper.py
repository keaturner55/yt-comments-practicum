"""
Youtube comment scraper

Given a video id (video_id) and valid API key (api_key),
this script will retrieve comments on a youtube video and save
relevant fields to a dataframe object. This dataframe is then
exported to an sqlite database file.

@author: Keaton Turner
"""

#%%
from apiclient.discovery import build
import pandas as pd
import datetime
import os
import logging
import sqlite3
import json
import traceback
import numpy as np

# global vars
logger = logging.getLogger("comment_scraper")
DATA_DIR = 'C:\\Users\\keatu\\Regis_archive\\practicum2_data\\'


#%%
def get_comments(video_id, api_key, comment_limit = 100):
    
    yt = build("youtube", "v3", developerKey = api_key)
    video_response = yt.commentThreads().list(part = 'snippet',videoId = video_id, order='relevance').execute()
    
    cdf = pd.DataFrame()
    comment_count = 0
    limit = False

    if not video_response:
        logger.error("No response for request")
        
    while video_response:
        for item in video_response["items"]:
            comment_count += 1
            if (comment_count % 250) == 0:
                logger.info("{} Comments Processed".format(comment_count))

            comment_dict = item["snippet"]["topLevelComment"]["snippet"]
            comment_dict["commentId"] = item["id"]
            comment_dict["videoId"] = video_id
            #comment_dict["channelId"] =  item["snippet"]["channelId"]
            comment_dict.pop("authorChannelId")
            comment_dict.pop("authorChannelUrl")
            comment_dict.pop("authorProfileImageUrl")
            comment_dict.pop('moderationStatus', None)
            
            cdf = cdf.append(comment_dict, ignore_index = True)

            if comment_count == comment_limit:
                logger.info("Processed max videos for video: {}".format(video_id))
                break

        if 'nextPageToken' in video_response and comment_count<comment_limit:
            p_token = video_response['nextPageToken']
            video_response = yt.commentThreads().list(part = 'snippet',videoId = video_id, order='relevance',pageToken = p_token).execute()
        else:
            logger.info("Processed {} comments for video {}".format(comment_count, video_id))
            break
    return cdf

#%%
def upload_tosql(cdf, outdb_name):
    #now = datetime.now()
    outcon = sqlite3.connect(outdb_name)
    cdf.astype(str).to_sql("comments",con=outcon, if_exists='append')
    outcon.close()

#%%
def get_sql_table(table, db_name):
    con = sqlite3.connect(db_name)
    df = pd.read_sql("select * from {}".format(table),con)
    return df

#%%
if __name__ == "__main__":
    start_time = datetime.datetime.now()
    print_time = "{}-{}-{}_{}-{}-{}".format(start_time.year, start_time.month, start_time.day,
                  start_time.hour, start_time.minute, start_time.second)

    outdb_name = os.path.join(DATA_DIR, "Youtube_Data.db")
    key_file =  os.path.join(DATA_DIR,"resources\\api_key.json")
    api_key = ""
    with open(key_file) as f:
        api_key = json.load(f)["key"]
    
    logfile = os.path.join(DATA_DIR,"comment_scraper_logs","{}.log".format(print_time))
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%m-%d-%Y %I:%M:%S', level = logging.INFO)

    video_df = get_sql_table("videos",outdb_name)
    video_df = video_df.replace('nan',0)

    # grab most viewed videos per channel (top 20)
    top_videos = video_df.sort_values("viewCount", ascending=False).groupby("channelId").head(20).reset_index(drop=True)
    
    top_videos = top_videos.loc[top_videos["channelId"]=='UCaXkIU1QidjPwiAYu6GcHjg']

    # controversial topic videos
    #test_videos = {'MSNBC':'1pJcU253iRU','Daily Caller':'bZnlFEUvn7Y'}

    testdf = video_df.loc[(video_df["channelId"]=='UCaXkIU1QidjPwiAYu6GcHjg') & (video_df["commentCount"]>=1000) & (video_df["commentCount"]<=2000) ]
    for i, row in testdf.iterrows():
    #for channel, vid_id in test_videos.items():
        video_id = row["videoId"]
        logger.info("Analyzing channel: {}, video: {}".format(row["channelTitle"],row['title']))
        try:
            cdf = get_comments(video_id,api_key, comment_limit=1000000)
            upload_tosql(cdf, outdb_name)
        except:
            traceback.print_exc()
    logger.info("Finished")
    end_time = datetime.datetime.now()
    logger.info("Total runtime: {}".format((end_time - start_time)))    
    

#%%
    