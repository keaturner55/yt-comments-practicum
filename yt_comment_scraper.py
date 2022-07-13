#%%
from apiclient.discovery import build
import pandas as pd
import datetime
import logging
import sqlite3

#%%

def get_comments(video_id, api_key):
    
    logger = logging.getLogger(__name__)
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
    outcon = sqlite3.connect(outdb_name)
    cdf.to_sql(outcon)

#%%
if __name__ == "__main__":
    
    logger = logging.getLogger(__name__)
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m-%d-%Y %I:%M:%S', level = logging.DEBUG)
    logger.setLevel(logging.DEBUG)
    start_time = datetime.datetime.now()    
    
    api_key = "AIzaSyAIUj50C7H3uvK4eXXHvapA_vmUHofnoNE"
    video_id = "MoojVXE5Xao"
    
    logger.info("Analyzing video: {}".format(video_id))
    logger.info("Building YT object")
    yt = build("youtube", "v3", developerKey = api_key)
    logger.info("Retrieving comments")
    cdf, comment_count = get_comments(video_id,api_key)
    logger.info("Finished")
    end_time = datetime.datetime.now()
    logger.info("Total comments: {}".format(comment_count))
    logger.info("Total runtime: {}".format((end_time - start_time)))    
    

#%%
    