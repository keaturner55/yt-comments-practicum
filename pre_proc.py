#%%
# supress warnings from sklearn
def warn(*args, **kwargs):
    pass
import warnings
warnings.warn = warn

from nltk.stem import WordNetLemmatizer
from nltk.tokenize import TweetTokenizer # tweet tokenizer is good for handling common words/symbols/emoticons found in social media data
from nltk.corpus import stopwords
from nltk.tag import pos_tag
import dask.dataframe as ddf
import os

#%%
def clean_text(text):
    """
    Apply data cleaning pre-process
    """
    lemmatizer = WordNetLemmatizer()
    tokenizer = TweetTokenizer()
    text_vector = []

    #loop through input text list and apply transformations on individual words
    for each_text in text:
        lemmatized_tokens = []
        tokens=tokenizer.tokenize(each_text.lower())
        pos_tags=pos_tag(tokens)
        for each_token, tag in pos_tags: 
            if tag.startswith('NN'): 
                pos='n'
            elif tag.startswith('VB'): 
                pos='v'
            else: 
                pos='a'
            lemmatized_token=lemmatizer.lemmatize(each_token, pos)
            lemmatized_tokens.append(lemmatized_token)
        text_vector.append(' '.join(lemmatized_tokens))
    return text_vector

#%%
if __name__=="__main__":
    DATA_DIR = 'C:\\Users\\keatu\\Regis_archive\\practicum2_data\\'
    colnames = ["target","id","date","flag","user","text"]
    s140df = ddf.read_csv(os.path.join(DATA_DIR,"resources","s140_train.csv"), names = colnames, encoding='latin-1', blocksize=25e6)
    s140df['preproc_text'] = s140df['text'].apply(clean_text)
    s140df.to_csv(os.path.join(DATA_DIR,"s140_processed.csv"), index=False)
# %%
