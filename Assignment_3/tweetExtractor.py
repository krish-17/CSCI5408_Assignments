import requests
import time
import tweepy

my_twitter_header = {
    'Authorization': 'Bearer *******'}


class TwitterExtractor:
    def __init__(self):
        self.fileCount = 0
        self.tweetCount = 0
        self.last_file_name = ''
        self.last_file_count = 0
        self.next_token = ''

    def increment_file(self):
        self.fileCount += 1

    def increment_tweet(self):
        self.tweetCount += 100

    def set_last_file_name(self, file_name):
        self.last_file_name = file_name

    def set_last_file_count(self, last_file_count):
        self.last_file_count = last_file_count

    def set_next_token(self, next_token):
        self.next_token = next_token

    def write_to_file(self, search_response):
        file_name = 'tweetop/' + str(round(time.time() * 1000)) + '.txt'
        store_resp_json_in_file = open(file_name, 'a', encoding="utf-8")
        store_resp_json_in_file.write(search_response.text)
        store_resp_json_in_file.close()
        self.set_last_file_name(file_name)
        self.increment_file()
        self.increment_tweet()
        pass

    def call_twitter_search_api(self):
        print("twitter search API is Called")
        twitter_search_url = "https://api.twitter.com/2/tweets/search/recent"
        search_query = {
            'tweet.fields': 'author_id,created_at,entities,geo,in_reply_to_user_id,lang,possibly_sensitive,'
                            'referenced_tweets,source',
            'query': 'covid OR emergency OR immune OR vaccine OR flu OR snow', 'max_results': 100}
        if len(self.next_token) > 0:
            search_query['next_token'] = self.next_token
        search_response = requests.get(twitter_search_url, params=search_query, headers=my_twitter_header)
        if search_response.status_code == 200:
            load_res_json = search_response.json()
            self.write_to_file(search_response)
            res_meta_json = load_res_json['meta']
            if 'next_token' in res_meta_json.keys() and len(res_meta_json['next_token']) > 0 and self.tweetCount <= 3500:
                self.set_next_token(res_meta_json['next_token'])
                self.call_twitter_search_api()
            else:
                if res_meta_json['result_count'] < 100:
                    self.set_last_file_count(res_meta_json['result_count'])
        else:
            print("Error fetching results from Twitter")
            print(search_response.text)
        pass


my_twitter = TwitterExtractor()
my_twitter.call_twitter_search_api()
print(my_twitter.last_file_count)
print(my_twitter.last_file_name)
print(my_twitter.tweetCount)
print(my_twitter.fileCount)


