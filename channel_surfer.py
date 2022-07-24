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
import logging
logger = logging.getLogger("channel_surfer")


DATA_DIR = 'C:\\Users\\keatu\\Regis_archive\\practicum2_data\\'

#%%

# gather the playlist_id for each channel in order to build a playlist object containing videos
def get_playlist_ids(channel_list, api_key):
    yt = build("youtube","v3", developerKey = api_key)
    channel_response = yt.channels().list(part="contentDetails", id =','.join(channel_list)).execute()
    channel_playlist_dict = {}
    for channel_id, response in zip(channel_list,channel_response["items"]):
        playlist_id = response['contentDetails']['relatedPlaylists']['uploads']
        channel_playlist_dict[channel_id] = playlist_id
    
    return channel_playlist_dict


#%%

def get_channel_videos(playlist_id, api_key, video_limit = 50):
    yt = build("youtube", "v3", developerKey = api_key)

    response = yt.playlistItems().list(part="snippet", playlistId = playlist_id, maxResults = 50).execute()
    
    rdf = pd.DataFrame()
    video_count = 0
    limit = False
    
    if not response:
        logger.ERROR("No response for request")
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
            response = yt.playlistItems().list(part="snippet", pageToken=p_token, playlistId =playlist_id, maxResults = 50).execute()
        else:
            logger.info("Processed {} videos for channel {}".format(video_count, playlist_id))
            break
    return rdf
    
#%%

def upload_tosql(df, outdb_name):
    #now = datetime.now()
    outcon = sqlite3.connect(outdb_name)
    df.astype(str).to_sql("videos",con=outcon, if_exists='append')
    outcon.close()
#%%
if __name__ == "__main__":
    start_time = datetime.datetime.now()
    
    print_time = "{}-{}-{}_{}-{}-{}".format(start_time.year, start_time.month, start_time.day,
                  start_time.hour, start_time.minute, start_time.second)
    logging.basicConfig(filename=os.path.join(DATA_DIR,"channel_surfer_logs","{}.log".format(print_time)),format='%(asctime)s %(message)s',
                        datefmt='%m-%d-%Y %I:%M:%S', level = logging.INFO)

    outdb_name = os.path.join(DATA_DIR, "Youtube_Data.db")
    key_file =  os.path.join(DATA_DIR,"resources\\api_key.json")
    channel_file = os.path.join(DATA_DIR, "resources\\political_channels.csv")

    channel_df = pd.read_csv(channel_file)
    api_key = ""
    with open(key_file) as f:
        api_key = json.load(f)["key"]
    
    df_list = []
    for i, row in channel_df.head(n=2).iterrows():
        logger.info("Analyzing channel {}/{}: {}".format(i+1, len(channel_df),row["outlet"]))
        try:
            rdf = get_channel_videos(row["youtube_id"],api_key, 100)
            df_list.append(rdf)
        except Exception as e:
            logger.ERROR(e)
            continue
    logger.info("Uploading results to database: {}".format(outdb_name))
    alldf = pd.concat(df_list)
    upload_tosql(all, outdb_name)
    logger.info("Finished")
    end_time = datetime.datetime.now()
    logger.info("Total runtime: {}".format((end_time - start_time)))    
    rdf.to_csv('test.csv')

# %%
