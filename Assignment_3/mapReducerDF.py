from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext

sc = SparkContext('local', 'pyspark')
spark = SparkSession(sc)


def reducer(word_list):
    return word_list.groupBy('word').count()


def remove_punctuation(value):
    return trim(lower(regexp_replace(value, '([^\s\w_]|_)+', ''))).alias('tweet_text')


if __name__ == '__main__':
    # Load data from twitter
    tweet_df = spark.read.json("tweetop/*.txt")
    # Navigate to the text field in twitter json. As it holds the tweet data.
    # clean the text for better clarity. Cleaning involves removing punctuation.
    tweet_text = tweet_df.select(explode("data.text").alias("tweet_text")).select(remove_punctuation(col('tweet_text')))
    # Breaking tweet text lines into words. This generates a word array at each row in Dataframe.
    tweetWordsSplitDF = (tweet_text
                         .select(split(tweet_text.tweet_text, '\s+').alias('words')))
    # Exploding the array to flatten the structure. By this point a Map of clean Words are established
    tweetWordsSingleDF = (tweetWordsSplitDF
                          .select(explode(tweetWordsSplitDF.words).alias('word')))
    # Applying the reducer in the resultant of mapper using sql groupBy function and sql count function
    twitter_words_with_count = reducer(tweetWordsSingleDF)
    # Applying the filter over the output of reducer for the words given in task.
    tweetFilterWordsDF = twitter_words_with_count.filter(
        (twitter_words_with_count.word == "flu") | (twitter_words_with_count.word == "snow") | (
                twitter_words_with_count.word == "emergency"))
    tweetFilterWordsDF.show()
