from unicodedata import normalize
from Methods import fetch_tweets_to_dataframes,show_df,write_csv,remover_acentos,analyze_sentiment_df
import tweepy
import pandas as pd
import datetime
import os

consumer_key  = "KUQ7A4UP83OYFrXlbwE1rLumI"
consumer_secret  = "WkhjGnArh20ufPXEn3EJyp3bO1KzvVBqQPAZGowRO2usIgRHrR"


access_token = "1321449776458727426-NE19ALrOzmq2FAKme3ipqo8YPzULxV"
access_token_secret = "wQTfYiQlhxOgDDyFJYHskLV83Dryc8XObLQhVnAGvMAr8"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

queries_pt = ["armas inteligentes","armas autonomas OR armas autonômas OR  armas autónomas",
            "robos assassinos OR robôs assassinos","robos que matam OR robôs que matam"
            ,"matar robo OR matar robô","Inteligencia artificial armas OR Inteligência artificial armas",
            "Inteligência Artificial Militar OR Inteligencia Artificial Militar ",
            "Sistemas de armas inteligentes",
            "VANTs",
            "Inteligência artificial guerra OR Inteligencia artificial guerra",
            "robô senciente","Automação militar",
            "Máquinas guerra autônomas OR Maquinas guerra autonomas",
            "Sistema de armas autonômas OR Sistema de armas autonomas", 
            "#ArmasAutônomas OR #ArmasAutonomas"
        ]
queries_es = ["armas inteligentes",
            "armas autónomas OR armas autónomas OR armas autónomas", 
            "robot asesinos OR robots asesinos",
            "robot que matam OR robots que matam",
            " matar robot OR matar robots",
            "Inteligencia artificial armas OR Inteligência armas artificiales",
            "Inteligência Artificial Militar OR Inteligencia Artificial Militar",
            "Sistemas de armas inteligentes",
            "aeronaves no tripuladas",
            "Inteligencia artificial guerra",
            "Robots con consciencia",
            "automatización militar",
            "Máquinas guerra autónomas OR maquinas guerra autónomas",
            "Sistema de armas autónomas OR Sistema de armas autónomas",
            "#ArmasAutónomas"]

queries_teste = ["Taylor swift", "Counter-Strike", "Lula"]


def executeQueries(count,queries,language):
    

    dfs = fetch_tweets_to_dataframes(queries,count,api,language)

    for i, df in enumerate(dfs):
        print(f"Number of tweets fetched for query {i+1}: {len(df)}")

    return dfs



dfs = executeQueries(100,queries_teste,"pt")


i = 0
today = datetime.datetime.today().strftime('%Y%m%d')
for df in dfs:
    resultDF = analyze_sentiment_df(df, "text")
    orderedDF = resultDF.sort_values('favorites', ascending=False)
    directory_name = "Tweet{}/".format(today)
    if not os.path.exists(directory_name):
        os.makedirs(directory_name)
    filename = "{}Tweets_{}.csv".format(directory_name, remover_acentos(queries_es[i]).replace(" ", "_"))
    orderedDF.to_csv(filename, index=False)
    i += 1

