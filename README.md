# Sentiment Analysis of YouTube Comments
Code for final Regis practicum project - MSDS696

This project and the code within attempts to provide utility for scraping youtube comments, creating and analyzing methods for text classification, determining sentiment for comment text, and providing some final analysis/visualization for particular videos, channels, and genres.


## Code Directory

### [Channel Surfer](./channel_surfer.py)
Given a csv file with youtube channel info and valid API key (api_key),
this script will retrieve N number of the most popular videos from
that channel. This dataframe is then exported to an sqlite database file.

### [Comment Scraper](./comment_scraper.py)
Given a video id (video_id) and valid API key (api_key),
this script will retrieve comments on a youtube video and save
relevant fields to a dataframe object. This dataframe is then
exported to an sqlite database file.

### [Preprocessing](./pre_proc.py)
Here several text pre-processing techniques were tested as part of the model training pipeline, but none of them ended up being used.

### [Naive Bayes Model](./1naivebayes.ipynb)
Model training and validation for the Naive Bayes Bernoulli model. In this script you'll see the messy iterative process behind model optimization.

### [VADER vs Pattern](./rule-based_models.ipynb)
Test VADER and Pattern package sentiment analysers and see how they perform on the test data.

### [Final Analysis](./yt_comment_analysis.ipynb)
Put it all together to provide an analysis of comments that aggregates at different levels
- Individual videos
- YouTube channels
- Entire content genres (e.g. political)