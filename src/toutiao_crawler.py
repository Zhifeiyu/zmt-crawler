# -*- coding: utf-8 -*-

import argparse
import requests
import json
import time
from datetime import datetime, timedelta
import pandas as pd
import os
import notion_df
import pandas as pd
from notion_df.configs import *
from notion_client import Client
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


def get_schema_article():
    configs = {
        "标题": TitleConfig(),
        "X_publish_date": DateConfig(type="date", date={}),
        "A1_发布时间": DateConfig(type="date", date={}),
        "X_标签": SelectConfig(type="select", select={}),
        "B2_类型": SelectConfig(type="select", select={}),
        "A4_展示量": NumberConfig(type="number", number=NumberFormat(format='number')),
        "A5_阅读量": NumberConfig(type="number", number=NumberFormat(format='number')),
        "A6_点击率": RichTextConfig(type="rich_text", rich_text={}),
        "A7_点赞量": NumberConfig(type="number", number=NumberFormat(format='number')),
        "A8_评论数": NumberConfig(type="number", number=NumberFormat(format='number')),
        "A9_播放量": NumberConfig(type="number", number=NumberFormat(format='number')),
        "B1_URL": URLConfig(type="url", url={}),
        "A2_来源": RichTextConfig(type="rich_text", rich_text={}),
        "X_user_id": NumberConfig(type="number", number=NumberFormat(format='number')),
        "B3_来源说明": RichTextConfig(type="rich_text", rich_text={}),
        "B4_来源认证":  RichTextConfig(type="rich_text", rich_text={}),
        "X_behot_time":  NumberConfig(type="number", number=NumberFormat(format='number'))
    }

    schema = DatabaseSchema(configs)
    print(schema.query_dict())

    return schema


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


def save_articles_to_file(articles, output_path):
    if not os.path.exists(output_path):
        os.makedirs(output_path)
        print(f"creat path: {output_path} ")
    current_time = datetime.now()
    formatted_time = current_time.strftime("%Y%m%d-%H-%M")
    df = pd.DataFrame(articles)

    df.drop_duplicates(subset=['url']).to_excel(
        f"{output_path}/articles-{formatted_time}.xlsx", index=False, engine='xlsxwriter')

    all_articles_file = f"{output_path}/articles-all.xlsx"
    merged_df = df
    if os.path.exists(all_articles_file):
        all_df = pd.read_excel(all_articles_file)
        merged_df = pd.concat([all_df, df], ignore_index=True)

    with pd.ExcelWriter(all_articles_file) as writer:
        merged_df = merged_df.sort_values('digg_count', ascending=False).drop_duplicates(
            subset=['url'], keep='first').sort_values('publish_time', ascending=False)
        for column in merged_df.columns:
            if type(merged_df[column]) == str:
                merged_df[column] = merged_df[column].str.replace(
                    '\x00', '')  # Remove NULL characters
        merged_df.to_excel(writer, index=False, engine='xlsxwriter')

    return merged_df


def save_articles_to_notion(df, notion_token, notion_url, limit):
    # 保存到notion
    current_time = datetime.now()
    title = current_time.strftime("%Y%m%d%H%M")
    start_date = datetime.now() - timedelta(days=3)
    print(f"start_date: {start_date}")
    n_df = df
    n_df['publish_time'] = pd.to_datetime(
        df['publish_time']) + timedelta(hours=8)
    n_df = n_df[(n_df['title'] != '') & (n_df['article_type'] ==
                                         'weitoutiao') & (n_df['publish_time'] >= start_date) & (n_df['show_count'] > 10000)]
    new_column_names = {"title": "标题", "publish_time": "A1_发布时间", "publish_date": "X_publish_date", "article_type": "B2_类型", "tag": "X_标签", "show_count": "A4_展示量", "read_count": "A5_阅读量", "read_rate": "A6_点击率",
                        "digg_count": "A7_点赞量", "comment_count": "A8_评论数", "video_watch_count": "A9_播放量", "url": "B1_URL", "name": "A2_来源", "user_id": "X_user_id", "description": "B3_来源说明", "verified_content": "B4_来源认证", "behot_time": "X_behot_time"}
    n_df = n_df.rename(columns=new_column_names).sort_values(
        by=['X_publish_date', 'A4_展示量'], ascending=[False, False])
    notion_df.upload(n_df[:limit], notion_url, title=title,
                     api_key=notion_token, title_col='标题', schema=get_schema_article())


def save_artices_to_mogono(articles, mongo_url, mongo_db, mongo_collection):
    client = MongoClient(mongo_url)
    db = client[mongo_db]
    collection = db[mongo_collection]
    for article in articles:
        if article['item_id'] is not None:
            collection.replace_one(
                {'_id': article['item_id']}, article, upsert=True)


if __name__ == '__main__':
    # 创建ArgumentParser对象
    parser = argparse.ArgumentParser(description='toutiao crawler argument ')

    # 添加命令行参数
    parser.add_argument('--times', type=int, help='爬取次数', default=50)
    parser.add_argument('--sleeps', type=int, help='爬取一页后等待时间，单位秒', default=3)
    parser.add_argument('--cookie', type=str,
                        help='用户 cookies')
    parser.add_argument('--output', type=str,
                        help='结果保存目录', default='./output/toutiao_v2')
    parser.add_argument('--notion_token', type=str,
                        help='notion api token', default='')
    parser.add_argument('--notion_url', type=str,
                        help='notion page url', default='')
    parser.add_argument('--notion_articles', type=int,
                        help='notion 文章数量', default=300)

    parser.add_argument('--mongo_url', type=str)
    parser.add_argument('--mongo_db', type=str, default='zmt_crawler')
    parser.add_argument('--mongo_collection', type=str, default='articles')
    parser.add_argument('--filter', type=str, help='过滤的用户',
                        default='央视新闻,人民网,新华网,央视网,中国青年网,闽南网,头条段子,人民日报,新华社')

    # 解析命令行参数
    args = parser.parse_args()
    cookie = args.cookie
    times = args.times
    output = args.output
    sleeps = args.sleeps
    notion_token = args.notion_token
    notion_url = args.notion_url
    notion_articles = args.notion_articles
    mongo_url = args.mongo_url
    mongo_db = args.mongo_db
    mongo_collection = args.mongo_collection

    filter_name = args.filter

    print(f">>>>>>>>>>>> output: {output}")

    articles = get_articles(times, sleeps, cookie, filter_name)
    save_artices_to_mogono(articles, mongo_url, mongo_db, mongo_collection)
    # df = save_articles_to_file(articles, output)

    # if ((notion_token != '') & (notion_url != '')):
    #     save_articles_to_notion(df, notion_token, notion_url, notion_articles)
