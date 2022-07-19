"""
Youtube comment scraper code.

Given a video id (video_id) and valid API key (api_key),
this script will retrieve comments on a youtube video and save
relevant fields to a dataframe object. This dataframe is then
exported to an sqlite database file.

@author: keatu
"""

#%%
from apiclient.discovery import build
import pandas as pd
import datetime
import logging
import sqlite3
import json
logger = logging.getLogger("comment_scraper")

#%%

def get_comments(video_id, api_key):
    
    yt = build("youtube", "v3", developerKey = api_key)
    video_response = yt.commentThreads().list(part = 'snippet',videoId = video_id).execute()
    
    cdf = pd.DataFrame()
    comment_count = 0
    
    if not video_response:
        logger.ERROR("No response for request")
    while video_response:
        for item in video_response["items"]:
            comment_count += 1
            if (comment_count % 500) == 0:
                logger.info("{} Comments Processed".format(comment_count))
            comment_dict = item["snippet"]["topLevelComment"]["snippet"]
            comment_dict["commentId"] = item["id"]
            comment_dict["videoId"] = video_id
            comment_dict["authorId"] =  item["snippet"]["topLevelComment"]["snippet"]["authorChannelId"]["value"]
            comment_dict.pop("authorChannelId")
            comment_dict.pop("authorChannelUrl")
            comment_dict.pop("authorProfileImageUrl")
            
            cdf = cdf.append(comment_dict, ignore_index = True)
        if 'nextPageToken' in video_response:
            p_token = video_response['nextPageToken']
            video_response = yt.commentThreads().list(part = 'snippet',videoId = video_id,pageToken = p_token).execute()
        else:
            break
    return cdf, comment_count

def upload_tosql(cdf, outdb_name):
    #now = datetime.now()
    outcon = sqlite3.connect(outdb_name)
    cdf.astype(str).to_sql("comments",con=outcon)
    outcon.close()

#%%
if __name__ == "__main__":
    start_time = datetime.datetime.now()
    print_time = "{}-{}-{}_{}-{}-{}".format(start_time.year, start_time.month, start_time.day,
                  start_time.hour, start_time.minute, start_time.second)
    video_id = "dHY571KZDGU"
    outdb_name = r"C:\Users\keatu\Regis_archive\practicum2_data\comments.db"
    key_file =  r"C:\Users\keatu\Regis_archive\practicum2_data\resources\api_key.json"
    api_key = ""
    with open(key_file) as f:
        api_key = json.load(f)["key"]
    
    logging.basicConfig(filename="{}.log".format(print_time),format='%(asctime)s %(message)s',
                        datefmt='%m-%d-%Y %I:%M:%S', level = logging.INFO)
    
    logger.info("Analyzing video: {}".format(video_id))
    logger.info("Building YT object")
    #yt = build("youtube", "v3", developerKey = api_key)
    logger.info("Retrieving comments")
    cdf, comment_count = get_comments(video_id,api_key)
    logger.info("Uploading results to database: {}".format(outdb_name))
    #upload_tosql(cdf, outdb_name)
    logger.info("Finished")
    end_time = datetime.datetime.now()
    logger.info("Total comments: {}".format(comment_count))
    logger.info("Total runtime: {}".format((end_time - start_time)))    
    

#%%
    