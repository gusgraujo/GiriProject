import tweepy
import configparser
import csv
from Tweet import Tweet


def write_tweets_to_csv(tweets, file_path):
    with open(file_path, 'w', encoding='utf-8', newline='') as csvfile:
        fieldnames = ['created_at', 'full_text', 'lang', 'user']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for tweet in tweets:
            writer.writerow({'created_at': tweet.created_at,
                             'full_text': tweet.full_text,
                             'lang': tweet.lang,
                             'user': tweet.user})


def get_tweets(search_word):
    # Define the word you want to search for
    search_word = search_word


    # Make a request for recent tweets containing the search word
    tweets = tweepy.Cursor(api.search_tweets, q=search_word, lang="en", tweet_mode='extended').items(100)

    tweets_list = [] 
    # Iterate through the tweets and print the full text of each one
    for tweet in tweets:
        tweet_object = Tweet(tweet.created_at, tweet.full_text, tweet.id, tweet.favorite_count, tweet.retweet_count, tweet.source, tweet.lang, tweet.user)
        tweets_list.append(tweet_object)
    
    return tweets_list






auth = tweepy.OAuthHandler("yWpYs24mFu5MBQoKec003C6Il", "UrKiQzB8g7dAQhkGmTX6eFWZgt1rGQkUx4yqyJdqgIo2VHMXJd")
auth.set_access_token("1321449776458727426-y7VWMo0FemOaBQFFu7PyTifCzDULmH", "etvbxareMqSa1YAymUhqAB6bBIDScHlZ6Gy0HSYOg8JCs")


# Create API object
api = tweepy.API(auth)

topics_list = ["#armas -ana","armas"]

for topic in topics_list:
    write_tweets_to_csv(get_tweets(topic), r'tweets_{}.csv'.format(topic))
