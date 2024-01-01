# -*- coding: utf-8 -*-

import requests
import json
import time
from datetime import datetime, timedelta
from pymongo.mongo_client import MongoClient




"""
获取文章的的用户信息

param article: 新闻的json数据
return: {"name": "xxx", "user_id": "xxx", "description": "xxx", "verified_content": "xxx"}
"""


def get_user_info(article):
    user_info = {"name": "", "user_id": "",
                 "description": "", "verified_content": ""}
    if ('user' in article):
        user = article['user']
        user_info['name'] = user['name']
        user_info['user_id'] = user['user_id']
        user_info['description'] = user['desc'] if 'desc' in user else ''
        user_info['verified_content'] = user['verified_content'] if 'verified_content' in user else ''
    elif ('user_info' in article):
        user = article['user_info']
        user_info['name'] = user['name']
        user_info['user_id'] = user['user_id']
        user_info['description'] = user['description'] if 'description' in user else ''
        user_info['verified_content'] = user['verified_content'] if 'verified_content' in user else ''
    return user_info


"""
获取文章的的链接

param article: 新闻的json数据
return: {"url": "xxx"}
"""


def get_url_info(article):
    url = {"url": ""}
    if ('url' in article and article['url'] != ""):
        url["url"] = article['url']
    elif ('article_url' in article and article['article_url'] != ""):
        url["url"] = article['article_url']
    elif ('share_url' in article):
        url["url"] = article['share_url']

    return url


"""
获取文章的标题

param article: 新闻的json数据
return: {"title": "xxx"}
"""


def get_title_info(article):
    title = {"title": ""}
    if ('title' in article and article['title'] != ""):
        title["title"] = article['title']
    elif ('content' in article and article['content'] != ""):
        title["title"] = article['content']
    elif ('abstract' in article and article['abstract'] != ""):
        title["title"] = article['abstract']
    elif ('card_title' in article):
        title['title'] = article['card_title']
    return title


def underscore_to_camelcase(string):
    words = string.split('_')
    capitalized_words = [word.capitalize() for word in words[1:]]
    camelcase_string = words[0] + ''.join(capitalized_words)
    return camelcase_string


'''
获取文章统计数据
'''


def get_count_info(article):
    count_info = {"show_count": 0, "read_count": 0, "read_rate": 0, "digg_count": 0,
                  "comment_count": 0, "video_watch_count": 0, "show_count": 0}
    if ('itemCell' in article and 'itemCounter' in article['itemCell']):
        for key in count_info.keys():
            tmp_key = underscore_to_camelcase(key)
            item_counter = article['itemCell']['itemCounter']
            count_info[key] = item_counter[tmp_key] if tmp_key in item_counter else 0
    else:
        for key in count_info.keys():
            count_info[key] = article[key] if key in article else 0

    if (count_info['show_count'] > 0):
        count_info['read_rate'] = "{:.2%}".format(count_info['read_count'] /
                                                  count_info['show_count'])
    return count_info


'''
获取文章类型
'''


def get_type_info(article):
    article_type = {'article_type': ''}
    if ('log_pb' in article and 'article_type' in article['log_pb']):
        article_type['article_type'] = article['log_pb']['article_type']

    return article_type


'''
获取标签信息
'''


def get_tag_info(article):
    tag_info = {'tag': ''}
    tag_info['tag'] = article['tag'] if 'tag' in article else ''

    return tag_info


'''
获取发布时间
'''


def get_pushlish_time(article):
    pushlish_time = {'publish_time': '', 'publish_date': ''}
    if ('publish_time' in article and article['publish_time'] != ''):
        dt = datetime.fromtimestamp(article['publish_time'])
        pushlish_time['publish_time'] = dt.strftime('%Y-%m-%d %H:%M:%S')
        pushlish_time['publish_date'] = dt.strftime('%Y-%m-%d')

    return pushlish_time


def get_item_id(article):
    item_id = {'item_id': ''}
    if ('item_id' in article):
        item_id['item_id'] = article['item_id']
    elif ('thread_id' in article):
        item_id['item_id'] = article['thread_id']
    return item_id


def get_feed_url(count, size):
    return f"http://api5-normal-c-lf.snssdk.com/api/news/feed/v88/?list_count={count}&count={size}"





def get_articles(times, sleeps, cookie, filter):
    headers = {
        'Host': 'api5-normal-c-lf.snssdk.com',
        'Connection': 'keep-alive',
        'Cookie': cookie,
        'User-Agent': 'com.ss.android.article.news/8090 (Linux; U; Android 10; zh_CN; meizu 17 Pro; Build/QKQ1.200127.002; Cronet/TTNetVersion:e062d68f 2021-01-05 QuicVersion:47946d2a 2020-10-14)',
        'Accept-Encoding': 'gzip, deflate',
    }

    filter_name = filter.split(",")

    articles = []
    for i in range(0, times):
        feed_url = get_feed_url(i, 100)
        print(
            f">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> start {i}, feed_url: {feed_url}")
        respons = requests.get(url=feed_url, headers=headers)
        content = json.loads(respons.text).get('data')
        for data in content:
            try:
                print("======================================================")
                article = json.loads(data['content'])
                # print(json.dumps(article, ensure_ascii=False))
                title = get_title_info(article)
                counter = get_count_info(article)
                article_type = get_type_info(article)
                url = get_url_info(article)
                user_info = get_user_info(article)
                tag = get_tag_info(article)
                pushlish_time = get_pushlish_time(article)
                item_id = get_item_id(article)
                article_tmp = {**title, **pushlish_time, **article_type, **tag, **counter, **article_type, **url,
                               **user_info, **item_id, **{'behot_time': article['behot_time']}}
                if user_info['name'] not in filter_name:
                    articles.append(article_tmp)
            except KeyError:
                print(json.dumps(article, ensure_ascii=False))
                continue

            print(json.dumps(article_tmp, ensure_ascii=False))
        print(
            f">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> end {i}, sleep {sleeps}... ")
        time.sleep(sleeps)

    return articles


def get_user_feed_url(token, max_behot_time, signature):
    return f"https://www.toutiao.com/api/pc/list/user/feed?category=profile_all&token={token}&max_behot_time={max_behot_time}&aid=24&app_name=toutiao_web&_signature={signature}"
    

def get_user_articles(times, sleeps, cookie, token, signature):
    headers = {
        "authority": "www.toutiao.com",
        "accept": "application/json, text/plain, */*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        'Cookie': cookie,
        "referer": "https://www.toutiao.com/c/user/token/MS4wLjABAAAAonAK-1v-Zpq9OZwZk0EKfWM39ba5Clx3wSRLCx-E3HABwYW6EcRC3WTSINnh6pY5/?",
        "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "macOS",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    articles = []
    max_behot_time = 0
    for i in range(0, times):
        feed_url = get_user_feed_url(token, max_behot_time, signature)
        print(
            f">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> start {i}, feed_url: {feed_url}")
        respons = requests.get(url=feed_url, headers=headers)
        print(respons)
        data = json.loads(respons.content).get('data')
        for article in data:
            try:
                print("======================================================")
                title = get_title_info(article)
                counter = get_count_info(article)
                article_type = get_type_info(article)
                url = get_url_info(article)
                user_info = get_user_info(article)
                tag = get_tag_info(article)
                pushlish_time = get_pushlish_time(article)
                item_id = get_item_id(article)
                article_tmp = {**title, **pushlish_time, **article_type, **tag, **counter, **article_type, **url,
                               **user_info, **item_id, **{'behot_time': article['behot_time']}}
                if article['behot_time'] > max_behot_time:
                    max_behot_time = article['behot_time']
                articles.append(article_tmp)
            except KeyError:
                print(json.dumps(article, ensure_ascii=False))
                continue

            # print(json.dumps(articles, ensure_ascii=False))
        print(
            f">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> end {i}, sleep {sleeps}... ")
        time.sleep(sleeps)

    return articles
    
    
def save_artices_to_mogono(articles, mongo_uri, mongo_db, mongo_collection, delete_old_data=False, delete_old_data_days=10):
    client = MongoClient(mongo_uri)
    db = client[mongo_db]
    collection = db[mongo_collection]
    for article in articles:
        if article['item_id'] is not None and article['_id'] is None:
            collection.replace_one(
                {'_id': article['item_id']}, article, upsert=True)

    ## 删除过期数据
    if delete_old_data:
        current_date = datetime.now()  # 当前日期和时间
        target_date = current_date - timedelta(days=delete_old_data_days)  # 目标日期（当前日期减去10天）
        query = {"publish_date": {"$lt": target_date}}
        result = collection.delete_many(query)
        print(f"Deleted {result.deleted_count} documents.")
