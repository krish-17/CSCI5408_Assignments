# Using RDD
from pyspark import SparkContext
from operator import add
import re as regex

word_list = ["flu", "snow", "emergency"]


def seperate_words_from_tweets(line):
    line = regex.sub(r'^\W+|\W+$', '', line)
    return map(str.lower, regex.split(r'\W+', line))


def given_words(words):
    if words in word_list:
        return True
    return False


if __name__ == '__main__':
    sc = SparkContext("local", "wordcount")
    tweet_text_file_content = sc.textFile('tweetop/*.txt')
    tweet_words = tweet_text_file_content.flatMap(seperate_words_from_tweets)
    filtered_tweet_words = tweet_words.filter(given_words)
    # Creating a Mapper (K,v) [Words, Count] -> initially its 1.
    initialize_word_count = filtered_tweet_words.map(lambda x: (x, 1))
    # Sending the (K,V) output of Map function to reducer which does (K, (sum(v)) to do the total count
    counts = initialize_word_count.reduceByKey(add)
    print(counts.collect())
