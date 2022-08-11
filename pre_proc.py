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
from tqdm import tqdm

#%%
def clean_text(text_list):
    """
    Apply data cleaning pre-process
    """
    lemmatizer = WordNetLemmatizer()
    tokenizer = TweetTokenizer()
    #stopwords_en=stopwords.words('english')
    all_text = []
    for each_text in text_list:
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
if __name__=="__main__":
    DATA_DIR = 'C:\\Users\\keatu\\Regis_archive\\practicum2_data\\'
    colnames = ["target","id","date","flag","user","text"]
    print("reading csv")
    s140df = pd.read_csv(os.path.join(DATA_DIR,"resources","s140_train.csv"), names = colnames, encoding='latin-1')
    print("cleaning text")
    s140df['preproc_text'] = s140df['text'].apply(clean_text)
    s140df.to_csv(os.path.join(DATA_DIR,"s140_processed.csv"), index=False)
# %%

