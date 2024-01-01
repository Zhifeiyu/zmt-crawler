# -*- coding: utf-8 -*-

import argparse
from datetime import datetime, timedelta
import pandas as pd
import os
import notion_df
import pandas as pd
from notion_df.configs import *
from notion_client import Client
from pymongo.mongo_client import MongoClient
import toutiao_utils


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



if __name__ == '__main__':
    # 创建ArgumentParser对象
    parser = argparse.ArgumentParser(description='toutiao crawler argument ')

    # 添加命令行参数
    parser.add_argument('--times', type=int, help='爬取次数', default=50)
    parser.add_argument('--sleeps', type=int, help='爬取一页后等待时间，单位秒', default=3)
    parser.add_argument('--cookie', type=str, help='用户 cookies')
    parser.add_argument('--output', type=str,
                        help='结果保存目录', default='./output/toutiao_v2')
    parser.add_argument('--notion_token', type=str,
                        help='notion api token', default='')
    parser.add_argument('--notion_url', type=str,
                        help='notion page url', default='')
    parser.add_argument('--notion_articles', type=int,
                        help='notion 文章数量', default=300)

    parser.add_argument('--mongo_url', type=str)
    parser.add_argument('--mongo_username', type=str)
    parser.add_argument('--mongo_password', type=str)
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
    mongo_username = args.mongo_username
    mongo_password = args.mongo_password
    mongo_db = args.mongo_db
    mongo_collection = args.mongo_collection

    filter_name = args.filter
    mongon_uri = f"mongodb+srv://{mongo_username}:{mongo_password}@{mongo_url}/?retryWrites=true&w=majority"
    articles = toutiao_utils.get_articles(times, sleeps, cookie, filter_name)
    toutiao_utils.save_artices_to_mogono(articles, mongon_uri, mongo_db, mongo_collection)
