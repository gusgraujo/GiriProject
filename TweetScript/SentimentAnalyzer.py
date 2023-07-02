from googletrans import Translator

def translate(text):
    translator = Translator()

    translated_text = translator.translate(text)

    return translated_text


def analyze_sentiment(text_col):
    scores = [analyzer.polarity_scores(translate(text)) for text in text_col]
    return scores

def extract_max_sentiment(scores):
    max_sentiment = max(scores, key=lambda x: x['score'])
    return str(abs(max_sentiment['score']))

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
    # register UDFs
    udf_analyze_sentiment = udf(analyze_sentiment, ArrayType(MapType(StringType(), FloatType())))
    udf_extract_max_sentiment = udf(extract_max_sentiment, StringType())

    # apply sentiment analysis UDF to specified column
    df = df.withColumn("sentiment_scores", udf_analyze_sentiment(col_name))

    # extract max sentiment for each row
    df = df.withColumn("max_sentiment", udf_extract_max_sentiment("sentiment_scores"))
    df = df.drop("sentiment_scores") # drop sentiment_scores column
    df.show()
    return df




df = spark.table("tweets_armas_autonomas_or_armas_autonomas_or__armas_autonomas")

# initialize SentimentIntensityAnalyzer
analyzer = SentimentIntensityAnalyzer()



udf_analyze_sentiment = udf(analyze_sentiment, ArrayType(MapType(StringType(), FloatType())))

# read input DataFrame
df = spark.table("tweets_matar_robo_or_matar_robo")
df2 = spark.table("tweets_matar_robo_or_matar_robo")
# apply UDF to analyze sentiment
df = df.withColumn("sentiment_scores", udf_analyze_sentiment("text"))

udf_extract_max_sentiment = udf(extract_max_sentiment, StringType())

# extract max sentiment for each row
df = df.withColumn("max_sentiment", udf_extract_max_sentiment("sentiment_scores"))

df.show()


df2 = analyze_sentiment_df(df2,"text")
df2.show()
# Import the necessary libraries
from pyspark.sql.functions import *
import os
import shutil

# Define the path where the CSV files will be stored
output_path = "/mnt/my-blob-container/delta-csv-files/"

# Create the output directory if it doesn't exist
if not os.path.exists(output_path):
    os.makedirs(output_path)

# Define the path where the zip file will be stored
zip_path = "/mnt/my-blob-container/delta-csv-files.zip"

# Get a list of all Delta tables in the current workspace
delta_tables = spark.sql("SHOW TABLES").select("tableName").collect()

# Loop through each Delta table and export it to CSV
for delta_table in delta_tables:
    table_name = delta_table["tableName"]
    table_path = os.path.join(output_path, table_name)
    df = spark.table(table_name)
    df.write.format("csv").option("header", "true").mode("overwrite").save(table_path)

# Create a zip file of the CSV files
shutil.make_archive(zip_path[:-4], 'zip', output_path)


from azure.storage.blob import BlobServiceClient

# Define the connection string for the Azure Blob Storage account
connection_string = "DefaultEndpointsProtocol=https;AccountName=<account_name>;AccountKey=<account_key>;EndpointSuffix=core.windows.net"

# Create a BlobServiceClient object
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Get a BlobClient object for the zip file
blob_client = blob_service_client.get_blob_client(container="my-blob-container", blob="delta-csv-files.zip")

# Download the zip file
with open("delta-csv-files.zip", "wb") as f:
    download_stream = blob_client.download_blob()
    f.write(download_stream.readall())