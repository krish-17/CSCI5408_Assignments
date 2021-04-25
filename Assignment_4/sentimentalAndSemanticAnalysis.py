# Problem 2 and Problem 3

import json
import re
from os import listdir as twitterdir
from os.path import isfile, join
import math


# method used in Assignment 3 to extract only twitter data
def get_list_of_tweets_from_file(tweet_file):
    tweet_file_path = "../Assignment_3/tweetop/" + tweet_file
    tweet_file_content = open(tweet_file_path, 'r', encoding="utf-8")
    tweet_file_data = tweet_file_content.readlines()
    tweet_file_content.close()
    tweet_loaded_json_from_file = json.loads(tweet_file_data[0])  # stored in single line.
    return tweet_loaded_json_from_file["data"]  # data key has the tweets


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


# Cleaning and filtration steps as used in assignment 3 is used again
def clean_array_of_tweets(tweets_json_list):
    tweet_cleaned_list = []
    regex_to_remove_url = r'http\S+'
    regex_to_remove_line_breaks = "\n"
    regex_to_remove_at_mentions = "@"
    for tweet_data in tweets_json_list:
        tweet_text = tweet_data["text"]
        url_removed_text = re.sub(regex_to_remove_url, '', tweet_text)
        clean_tweet_text = re.sub(EMOTICONS_REGEX, '', url_removed_text)
        clean_tweet_text_line_breaks = re.sub(regex_to_remove_line_breaks, '', clean_tweet_text)
        clean_tweet_text_without_at_mentions = re.sub(regex_to_remove_at_mentions, '', clean_tweet_text_line_breaks)
        tweet_data["text"] = clean_tweet_text_without_at_mentions
        tweet_cleaned_list.append(tweet_data["text"])
    return tweet_cleaned_list


# Problem 2 : Bag Of Words approach
def tweet_word_count(tweet):
    tweet_word_dict = {}
    for word in tweet:
        if word in tweet_word_dict.keys():
            tweet_word_dict[word] = tweet_word_dict[word] + 1
        else:
            tweet_word_dict[word] = 1
    return tweet_word_dict


"""
List of positive words and negative words are referred from https://github.com/shekhargulati 
url : https://github.com/shekhargulati/sentiment-analysis-python/tree/master/opinion-lexicon-English
Visited on: March 28 2021
"""


def detect_overall_polarity_by_word(word_count):
    positive_file_content = open("positive_words.txt", 'r')
    positive_list = positive_file_content.read().splitlines()
    positive_file_content.close()
    negative_file_content = open("negative_words.txt", 'r')
    negative_list = negative_file_content.read().splitlines()
    negative_file_content.close()
    polarity_list = []
    match_list = []
    for word in word_count.keys():
        if word in positive_list:
            polarity_list.append(word_count[word] * 1)
            match_list.append(word)
        if word in negative_list:
            polarity_list.append(word_count[word] * -1)
            match_list.append(word)
    return match_list, sum(polarity_list)


def sentimental_analysis(all_tweets):
    sentimental_analysis_list = []
    index = 1
    for tweet in all_tweets:
        bag_of_words = tweet.split(" ")
        word_count = tweet_word_count(bag_of_words)
        tweet_polarity = detect_overall_polarity_by_word(word_count)
        polarity = "neutral"
        if tweet_polarity[1] > 0:
            polarity = "positive"
        if tweet_polarity[1] < 0:
            polarity = "negative"
        sa = {"index": index, "tweets": tweet, "match": tweet_polarity[0], "polarity": polarity}
        sentimental_analysis_list.append(sa)
        index = index + 1
    print(sentimental_analysis_list)


def semantic_analysis(tweet_documents):
    keywords_dict = {"flu": 0, "snow": 0, "cold": 0}
    documents_size = len(tweet_documents)
    tf_idf_table = {"document_count": len(tweet_documents)}
    for document in tweet_documents:
        for keyword in keywords_dict.keys():
            if keyword in document.split(" "):
                keywords_dict[keyword] = keywords_dict[keyword] + 1
    index = 1
    for keyword in keywords_dict.keys():
        word_tfidf_dict = {"Search Query": keyword, "df": keywords_dict[keyword]}
        term_frequency = documents_size / keywords_dict[keyword]
        frequency = keywords_dict[keyword]
        word_tfidf_dict["TF"] = str(documents_size) + "/" + str(frequency)
        word_tfidf_dict["IDF"] = math.log(term_frequency)
        tf_idf_table[index] = word_tfidf_dict
        index = index + 1
    print("It's solution for problem 3.a")
    print(tf_idf_table)
    cold_occurrence_table = {"search_term": "cold"}
    index = 1
    cold = "cold"
    highest_relative_frequency = 0
    highest_relative_frequency_article = ""
    for document in tweet_documents:
        frequency = 0
        word_occurrences = tweet_word_count(document.split(" "))
        if cold in word_occurrences.keys():
            frequency = word_occurrences[cold]
        article_dict = {"m": len(document.split(" ")), "f": frequency}
        if article_dict["f"] / article_dict["m"] > highest_relative_frequency:
            highest_relative_frequency = article_dict["f"] / article_dict["m"]
            highest_relative_frequency_article = document
        cold_occurrence_table[index] = article_dict
        index = index + 1
    print("It's solution for problem 3.b")
    print(cold_occurrence_table)
    print("It's solution for problem 3.c")
    print("Highest relative frequency for cold is,")
    print(highest_relative_frequency)
    print("The article with the highest relative frequency is,")
    print(highest_relative_frequency_article)


tweet_files_list = [tweet_file
                    for tweet_file in twitterdir("../Assignment_3/tweetop/")
                    if isfile(join("../Assignment_3/tweetop/", tweet_file))]
tweets_texts_in_file = []
for tweet_file in tweet_files_list:
    tweet_json_data = get_list_of_tweets_from_file(tweet_file)
    cleaned_json_tweets = clean_array_of_tweets(tweet_json_data)
    tweets_texts_in_file.append(cleaned_json_tweets)
total_tweets = [tweets for tweets_in_file in tweets_texts_in_file for tweets in tweets_in_file]
print("Problem 2: Sentimental Analysis")
sentimental_analysis(total_tweets)
print("Problem 3: Semantic Analysis")
semantic_analysis(total_tweets)
