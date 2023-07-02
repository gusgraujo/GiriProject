import tweepy
import pandas as pd
from unicodedata import normalize
import os
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


def fetch_tweets(query, count,api,language):
    tweets = []
    try:
        fetched_tweets = api.search_tweets(q=query + "-filter:retweets", tweet_mode='extended', count=count, lang=language)
        for tweet in fetched_tweets:
            parsed_tweet = {}
            parsed_tweet["created_at"] = tweet.created_at
            parsed_tweet["text"] = tweet.full_text
            parsed_tweet["user"] = tweet.user.screen_name
            parsed_tweet["retweets"] = tweet.retweet_count
            parsed_tweet["favorites"] = tweet.favorite_count
            tweets.append(parsed_tweet)
        return tweets
    except tweepy.TweepyException as e:
        print("Error : " + str(e))
        
def show_df(df):
    print(df)


def write_csv(df, filename):
    # Get the local file path for the output file
    local_path = os.path.join(os.getcwd(), filename)

    # Write the Pandas DataFrame as a CSV file to the local file system
    df.to_csv(local_path, index=False)

    print("File saved to:", local_path)
    

def fetch_tweets_to_dataframes(queries, count, api,language):
    # create a list to hold the dataframes
    dfs = []

    # loop through the queries and fetch the tweets for each query
    for query in queries:
        # fetch the tweets using the fetch_tweets() function
        tweets = fetch_tweets(query, count, api,language)
        # loop through the tweets and create a dataframe for each batch of 100
        for i in range(0, len(tweets), 100): # type: ignore
            # get the batch of tweets
            batch = tweets[i:i + 100]
            # create a dataframe for the batch of tweets
            df = pd.DataFrame(batch)
            # add the dataframe to the list of dataframes
            dfs.append(df)

    # print the number of dataframes created
    print(len(dfs))

    # return the list of dataframes
    return dfs

def remover_acentos(txt):
    return normalize('NFKD', txt).encode('ASCII', 'ignore').decode('ASCII')






################################################################################################

#Analysis sentiment methods


################################################################################################

analyzer = SentimentIntensityAnalyzer()


def analyze_sentiment(text_col):
    scores = [analyzer.polarity_scores(text) for text in text_col]
    return scores

def extract_max_sentiment(scores):
    max_score = sorted(scores, key=lambda x: x["compound"])[-1]
    compound_score = max_score["compound"]
    if abs(compound_score) < 0.05:
        return "neutral"
    elif compound_score >= 0.5:
        return "positive"
    else:
        return "negative"
    


def analyze_sentiment_df(df, col_name):
    # Apply sentiment analysis to specified column using analyze_sentiment function
    df['sentiment_scores'] = df[col_name].apply(analyze_sentiment)

    # Extract the maximum sentiment for each row using extract_max_sentiment function
    df['max_sentiment'] = df['sentiment_scores'].apply(extract_max_sentiment)

    # Drop the 'sentiment_scores' column
    df = df.drop(columns=['sentiment_scores'])

    # Display the resulting dataframe
    print(df)

    return df