import csv
import json
import os
import requests
import sys

# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
bearer_token = os.environ.get("BEARER_TOKEN")


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    # print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    # print(json.dumps(response.json()))
    print(f'Successfully removed existing rules')


def get_trending_topics(woeid):
    '''Retrieve the top 50 trending topics for a given location based on its WOEID and
       return each trending term in a list of JSON items in the format { value: term }.
    '''
    trend_params = { 'id': woeid }
    response = requests.get(
        'https://api.twitter.com/1.1/trends/place.json',
        auth=bearer_oauth,
        params=trend_params
    )
    print(f'get_trending_topics returned status code [ {response.status_code} ]')
    if response.status_code != 200:
        raise Exception(
            'Cannot get trending topics (HTTP {}): {}'.format(
                response.status_code, response.text
            )
        )
    json_response = json.loads(response.content)

    # create the rules in a format Twitter understands
    return [ { 'value': str(item['name']) + ' lang:en' } for item in json_response[0]['trends'] ]


def set_rules(rules, delete=None):
    # add the rules JSON body in a list to the request
    payload = { "add": rules }
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    # print(json.dumps(response.json()))
    print(f'Successfully set new rules')


def get_stream(rule_set, max_tweets=10):
    ''''''
    
    payload = {
        'tweet.fields': 'created_at',
        'expansions': 'author_id',
    }
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", 
        auth=bearer_oauth, 
        params=payload,
        stream=True,
    )

    # print(f'Formatted response: [ {response.url} ]')
    # print(f'Status code: [ {response.status_code} ]')

    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    
    print(f'Streaming tweets')
    # write tweets to CSV
    with open('../data/raw/training-tweets-2.csv', mode='w') as f:
        tweet_writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        tweet_writer.writerow(['author_id', 'tweet_created_at', 'tweet_id', 'tweet_text'])

        rows = 0
        for response_line in response.iter_lines():
            if response_line:
                rows += 1
                tweet_response = json.loads(response_line)
                tweet_writer.writerow([
                    tweet_response['data']['author_id'],
                    tweet_response['data']['created_at'],
                    tweet_response['data']['id'],
                    tweet_response['data']['text'].rstrip(),
                    
                ])
                # print(json.dumps(tweet_response, indent=4, sort_keys=True))
            if rows % 5000 == 0:
                print(f'\t{rows} tweets collected')
            if rows > max_tweets:
                print(f'Maximum tweets collected. Exiting')
                sys.exit()

def main():
    # initialize the filtered stream request with rules set from the top 25 trending topics
    # and stream the data into a csv file
    current_rules = get_rules()
    delete_all_rules(current_rules)
    rules = get_trending_topics(woeid=1)
    rules = rules[:24] 
    print(json.dumps(rules, indent=4))
    # new_rules = set_rules(rules)
    # get_stream(new_rules, 50000)

if __name__ == "__main__":
    main()