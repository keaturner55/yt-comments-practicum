#%%
# supress warnings from sklearn
def warn(*args, **kwargs):
    pass
import warnings
warnings.warn = warn

#from sklearn.base import BaseEstimator, TransformerMixin
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import TweetTokenizer # tweet tokenizer is good for handling common words/symbols/emoticons found in social media data
from nltk.corpus import stopwords
from nltk.tag import pos_tag
import pandas as pd
import os
from multiprocessing import Pool
import numpy as np
from datetime import datetime
import string
from glob import glob

DATA_DIR = 'C:\\Users\\keatu\\Regis_archive\\practicum2_data\\'

#%%
def clean_text_list(text_list):
    """
    Apply data cleaning pre-process on a bulk list
    Useful if using function in a pipeline
    """
    lemmatizer = WordNetLemmatizer()
    tokenizer = TweetTokenizer()
    #stopwords_en=stopwords.words('english')
    all_text = []
    for each_text in text_list:
        each_text = each_text.translate(str.maketrans('', '', string.punctuation))
        lemmatized_tokens = []
        tokens=tokenizer.tokenize(each_text.lower())
        pos_tags=pos_tag(tokens)
        # lemmatize each word/token based on its part of speach
        for each_token, tag in pos_tags: 
            if tag.startswith('NN'): 
                pos='n'
            elif tag.startswith('VB'): 
                pos='v'
            else: 
                pos='a'
            lemmatized_token=lemmatizer.lemmatize(each_token, pos)
            #if lemmatized_token not in stopwords_en: # try keeping stopwords
            lemmatized_tokens.append(lemmatized_token)
        all_text.append(' '.join(lemmatized_tokens))
    return all_text

#%%
def clean_text(text):
    """
    Apply data cleaning pre-process
    """

    # remove punctuation
    text = text.translate(str.maketrans('', '', string.punctuation))

    lemmatizer = WordNetLemmatizer()
    tokenizer = TweetTokenizer()
    #stopwords_en=stopwords.words('english')
    lemmatized_tokens = []
    tokens=tokenizer.tokenize(text.lower())
    pos_tags=pos_tag(tokens)
    # lemmatize each word/token based on its part of speach
    for each_token, tag in pos_tags: 
        if tag.startswith('NN'): 
            pos='n'
        elif tag.startswith('VB'): 
            pos='v'
        
        else: 
            pos='a'
        lemmatized_token=lemmatizer.lemmatize(each_token, pos)
        #if lemmatized_token not in stopwords_en: # try keeping stopwords
        lemmatized_tokens.append(lemmatized_token)
    return ' '.join(lemmatized_tokens)

def proc_df(df):
    df['preproc_text'] = df['text'].apply(clean_text)
    now = str(datetime.now()).replace(" ","_").replace(":","-")
    print("writing df")
    df.to_csv(os.path.join(DATA_DIR,"temp_df","s140_{}.csv".format(now)), index=False)

#%%
if __name__=="__main__":
    DATA_DIR = 'C:\\Users\\keatu\\Regis_archive\\practicum2_data\\'
    colnames = ["target","id","date","flag","user","text"]
    print("reading csv")
    s140df = pd.read_csv(os.path.join(DATA_DIR,"resources","s140_train.csv"), names = colnames, encoding='latin-1')
    df_list = np.array_split(s140df, 16)
    print("cleaning text")
    pool = Pool()
    pool.map(proc_df,df_list)
    print("concatenating dfs")
    csvlist = glob(os.path.join(DATA_DIR,"temp_df","*csv"))
    dflist = [pd.read_csv(i) for i in csvlist]
    print("saving bigdf")
    pd.concat(dflist).to_csv(os.path.join(DATA_DIR,"s140_preproc_nopunctuation.csv"))

# %%

