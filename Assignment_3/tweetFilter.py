import pymongo as pymongo
import json
import re
from os import listdir as twitterdir
from os.path import isfile, join


def get_list_of_tweets_from_file(tweet_file):
    tweet_file_path = "tweetop/" + tweet_file
    tweet_file_content = open(tweet_file_path, 'r', encoding="utf-8")
    tweet_file_data = tweet_file_content.readlines()
    tweet_file_content.close()
    tweet_loaded_json_from_file = json.loads(tweet_file_data[0])  # stored in single line.
    return tweet_loaded_json_from_file["data"]  # data key has the tweets


# reference: https://en.wikipedia.org/wiki/Unicode_block
# reference: https://gist.github.com/Alex-Just/e86110836f3f93fe7932290526529cd1#gistcomment-3208085
EMOTICONS_REGEX = re.compile(
    "["
    "\U0001F1E0-\U0001F1FF"  # flags (iOS)
    "\U0001F300-\U0001F5FF"  # symbols & pictographs
    "\U0001F600-\U0001F64F"  # emoticons 1F600 to 
    "\U0001F680-\U0001F6FF"  # transport & map symbols
    "\U0001F700-\U0001F77F"  # alchemical symbols
    "\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
    "\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
    "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
    "\U0000FFF0-\U0000FFFF"  # Specials
    "\U00002705"  # Tick Mark
    "]+"
)


def clean_array_of_tweets(tweets_json_list):
    tweet_cleaned_list = []
    regex_to_remove_url = r'http\S+'
    for tweet_data in tweets_json_list:
        tweet_text = tweet_data["text"]
        url_removed_text = re.sub(regex_to_remove_url, '', tweet_text)
        clean_tweet_text = re.sub(EMOTICONS_REGEX, '', url_removed_text)
        tweet_data["text"] = clean_tweet_text
        if not "referenced_tweets" in tweet_data:  # to remove the retweets
            tweet_cleaned_list.append(tweet_data)
    return tweet_cleaned_list


# Main code Starts here
def twitter_filter_engine():
    tweet_files_list = [tweet_file
                        for tweet_file in twitterdir("tweetop/")
                        if isfile(join("tweetop/", tweet_file))]
    mongo_client_connection = pymongo.MongoClient("mongodb+srv://krish17:<password>@assignment3.zkgxx.mongodb.net"
                                                  "/myMongoTweet"
                                                  "?retryWrites=true&w=majority")
    twitter_database = mongo_client_connection["myMongoTweet"]
    twitter_collection = twitter_database["tweets"]
    for tweet_file in tweet_files_list:
        tweet_json_data = get_list_of_tweets_from_file(tweet_file)
        cleaned_json_tweets = clean_array_of_tweets(tweet_json_data)
        twitter_collection.insert_many(cleaned_json_tweets)


print("Inserting data to Mongo Database")
twitter_filter_engine()
print("Finished inserting tweets")
