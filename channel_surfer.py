"""
Youtube channel surfer

Given a csv file with youtube channel info and valid API key (api_key),
this script will retrieve N number of the most popular videos from
that channel. This dataframe is then exported to an sqlite database file.

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

logger = logging.getLogger("channel_surfer")
DATA_DIR = 'C:\\Users\\keatu\\Regis_archive\\practicum2_data\\'

#%%

# gather the playlist_id for each channel in order to build a playlist object containing videos
# don't need to loop through page_tokens because channel count is under 50
def get_channel_info(channel_list, api_key):
    yt = build("youtube","v3", developerKey = api_key)
    channel_response = yt.channels().list(part="contentDetails,statistics", id =','.join(channel_list)).execute()
    channeldf = pd.DataFrame() 
    for channel_id, response in zip(channel_list,channel_response["items"]):
        cdict = response['statistics']
        cdict['playlist_id'] = response['contentDetails']['relatedPlaylists']['uploads']
        cdict['channel_id'] = channel_id
        channeldf = channeldf.append(cdict, ignore_index=True)
    
    return channeldf

#%%

def get_channel_videos(playlist_id, api_key, video_limit = 50):
    yt = build("youtube", "v3", developerKey = api_key)

    rdf = pd.DataFrame()
    video_count = 0
    limit = False

    response = yt.playlistItems().list(part="snippet", playlistId = playlist_id).execute()
    
    if not response:
        logger.error("No response for request")
    while response:
        for item in response["items"]:
            video_count += 1

            snippet = item['snippet']
            video_dict = {}
            video_dict['publishedAt'] = snippet['publishedAt']
            video_dict['channelId'] = snippet['channelId']
            video_dict['channelTitle'] = snippet['channelTitle']
            video_dict['title'] = snippet['title']
            video_dict['description'] = snippet['description']
            video_dict['videoId'] = snippet['resourceId']['videoId']
            
            rdf = rdf.append(video_dict, ignore_index = True)

            if video_count == video_limit:

                logger.info("Processed max videos for channel: {}".format(playlist_id))
                break
        if 'nextPageToken' in response and video_count<video_limit:
            p_token = response['nextPageToken']
            response = yt.playlistItems().list(part="snippet", pageToken=p_token, playlistId =playlist_id).execute()
        else:
            logger.info("Processed {} videos for channel {}".format(video_count, playlist_id))
            break
    return rdf
#%%

def get_video_statistics(video_list, api_key):
    statsdf = pd.DataFrame()
    yt = build("youtube","v3", developerKey = api_key)

    # divide list into chunks of 50
    for i in range(0,len(video_list), 50):
        temp_list = video_list[i:i+50]
        if len(temp_list)==0:
            break
        response = yt.videos().list(part="statistics", id = ','.join(temp_list)).execute()

        while response:
            for item in response['items']:
                stats_dict = item['statistics']
                stats_dict["videoId"] = item['id']
                statsdf = statsdf.append(stats_dict, ignore_index=True)

            if 'nextPageToken' in response:
                p_token = response['nextPageToken']
                response = yt.videos().list(part="statistics", id = ','.join(temp_list),pageToken=p_token).execute()
            else:
                break

    return statsdf
    
#%%

def upload_tosql(df, outdb_name, table):
    #now = datetime.now()
    outcon = sqlite3.connect(outdb_name)
    df.astype(str).to_sql(table,con=outcon, if_exists='append')
    outcon.close()
#%%
if __name__ == "__main__":
    start_time = datetime.datetime.now()
    
    print_time = "{}-{}-{}_{}-{}-{}".format(start_time.year, start_time.month, start_time.day,
                  start_time.hour, start_time.minute, start_time.second)
    filename=os.path.join(DATA_DIR,"channel_surfer_logs","{}.log".format(print_time))              
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%m-%d-%Y %I:%M:%S', level = logging.INFO)

    outdb_name = os.path.join(DATA_DIR, "Youtube_Data.db")
    key_file =  os.path.join(DATA_DIR,"resources\\api_key.json")
    channel_file = os.path.join(DATA_DIR, "resources\\political_channels.csv")

    api_key = ""
    with open(key_file) as f:
        api_key = json.load(f)["key"]

    channel_df = pd.read_csv(channel_file)

    logger.info("Getting channel statistics/details")
    try:
        channel_info = get_channel_info(channel_df["youtube_id"], api_key)
        channel_info = channel_df.merge(channel_info, left_on="youtube_id", right_on="channel_id" )
        upload_tosql(channel_info,outdb_name, "channel_info")
    except:
        traceback.print_exc()
    
    for i, row in channel_info.iterrows():
        logger.info("Analyzing channel {}/{}".format(i+1, len(channel_df)))
        try:
            rdf = get_channel_videos(row["playlist_id"],api_key, video_limit=500)
            vid_statsdf = get_video_statistics(rdf['videoId'], api_key)
            videodf = rdf.merge(vid_statsdf, how="inner", on="videoId")
            upload_tosql(videodf, outdb_name, 'videos')
        except:
            traceback.print_exc()
            continue
    
    logger.info("Finished")
    end_time = datetime.datetime.now()
    logger.info("Total runtime: {}".format((end_time - start_time)))    

# %%
