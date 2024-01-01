import toutiao_utils
import argparse
from datetime import datetime



if __name__ == "__main__":
    
     # 创建ArgumentParser对象
    parser = argparse.ArgumentParser(description='toutiao crawler argument ')

    # 添加命令行参数
    parser.add_argument('--times', type=int, help='爬取次数', default=10)
    parser.add_argument('--sleeps', type=int, help='爬取一页后等待时间，单位秒', default=10)
    parser.add_argument('--cookie', type=str, help='用户 cookies', default="tt_webid=7318239315536430592; ttcid=f97c8990d8844d9ba07b1bec1b1b5e8612; s_v_web_id=verify_lqrk91yy_3H2nbLc4_P2mD_4qQN_BG6P_DVOjQhyFrzfk; _ga=GA1.1.1497500034.1703910390; local_city_cache=%E7%A6%8F%E5%B7%9E; csrftoken=031e2d12d13a86a604a1b608661f326c; store-region=cn-fj; store-region-src=uid; _S_DPR=2; _S_IPAD=0; n_mh=C2VjjrClJ4P43iSu9eQBr7i0sa_4jF0dZoLuZJOcVLM; toutiao_sso_user=72140e70cd0f1a7bfc4b840581275443; toutiao_sso_user_ss=72140e70cd0f1a7bfc4b840581275443; passport_auth_status=e25f12f3c0df902e311100935ec1a556%2C9fa596043115b44eb2dcb322c19a37fb; passport_auth_status_ss=e25f12f3c0df902e311100935ec1a556%2C9fa596043115b44eb2dcb322c19a37fb; sid_guard=6b54d06ec792e7e765074c3a19d5724d%7C1704021565%7C5184001%7CThu%2C+29-Feb-2024+11%3A19%3A26+GMT; uid_tt=b71f38b3606942f2db6cc165558c57cb; uid_tt_ss=b71f38b3606942f2db6cc165558c57cb; sid_tt=6b54d06ec792e7e765074c3a19d5724d; sessionid=6b54d06ec792e7e765074c3a19d5724d; sessionid_ss=6b54d06ec792e7e765074c3a19d5724d; sid_ucp_v1=1.0.0-KDVmMDc5YjlhZDIwMDcyY2Y0MWM2MWFlYmM2ZWJjZjdiZjk0YmI2ZTIKGAi9naCVjczOBBC9nMWsBhgYIAw4BkD0BxoCbHEiIDZiNTRkMDZlYzc5MmU3ZTc2NTA3NGMzYTE5ZDU3MjRk; ssid_ucp_v1=1.0.0-KDVmMDc5YjlhZDIwMDcyY2Y0MWM2MWFlYmM2ZWJjZjdiZjk0YmI2ZTIKGAi9naCVjczOBBC9nMWsBhgYIAw4BkD0BxoCbHEiIDZiNTRkMDZlYzc5MmU3ZTc2NTA3NGMzYTE5ZDU3MjRk; msToken=OiFTdbltkiqvZmUC6hll5kLS_YubbQFbIMAohsbF_nmHh3MfM7yQPdfKQZHXLLJey5gUyTW5aUVWMsrLYeI0GP7XsgACeeuY_gED6qsdOf79CW07UvKu; tt_anti_token=dnxuntqAp7-d36f96fcd865913e32482cec997622348a71e283a54a70a169e4635cf6bd42b9; passport_csrf_token=7dcd026879e4331f2861c0afaf24b005; passport_csrf_token_default=7dcd026879e4331f2861c0afaf24b005; odin_tt=6f33323b1ff9acffd8e23c7469296f94236d1584cee68063e38f1b9fad65dd5b; sso_uid_tt=d1c261cb57a942a62d0101727fa77f60; sso_uid_tt_ss=d1c261cb57a942a62d0101727fa77f60; sid_ucp_sso_v1=1.0.0-KDM5Yzc2OWQ2NjI0NzQ4ZTExM2NhNDA5MzRkNDNmNTRhMzYzYjExMWQKCBDEwsisBhgYGgJobCIgNzIxNDBlNzBjZDBmMWE3YmZjNGI4NDA1ODEyNzU0NDM; ssid_ucp_sso_v1=1.0.0-KDM5Yzc2OWQ2NjI0NzQ4ZTExM2NhNDA5MzRkNDNmNTRhMzYzYjExMWQKCBDEwsisBhgYGgJobCIgNzIxNDBlNzBjZDBmMWE3YmZjNGI4NDA1ODEyNzU0NDM; _S_WIN_WH=1920_968; tt_scid=TRSSdTIxpUnM8PaT86guEeNBSBr-bRsIdIaVfky52G7VBCHzUQ4f5LKcS9uOD1oB1185; _ga_QEHZPBE5HH=GS1.1.1704074752.13.1.1704075629.0.0.0; ttwid=1%7C8iU-QxZWnTcAPqDSrpQzO492KYfUgnAtNB_vZoWjr1Y%7C1704075629%7Ca8b45ed31c3ec67edcbb10c0eca30dfdc205175dfba2964fba4f21d4bed1357c")
    parser.add_argument('--token', type=str, default='MS4wLjABAAAAonAK-1v-Zpq9OZwZk0EKfWM39ba5Clx3wSRLCx-E3HABwYW6EcRC3WTSINnh6pY5')
    parser.add_argument('--signature', type=str, default='_02B4Z6wo00d01hDDn3QAAIDBQjy.bcOFq-IQ55vAAOGwXMQzDyzKakICDmicJjryZocasBKGvl6JZotXD1ybWYs325ZpsAyShoDvDHYylgiNztcWK.Gkl2vNPUGqwPAJ3c2jOyvYIFoWRT6Tff')
    parser.add_argument('--notion_articles', type=int,
                        help='notion 文章数量', default=300)

    parser.add_argument('--mongo_url', type=str)
    parser.add_argument('--mongo_username', type=str)
    parser.add_argument('--mongo_password', type=str)
    parser.add_argument('--mongo_db', type=str, default='zmt_crawler')
    parser.add_argument('--mongo_collection', type=str, default='articles_personal_zfy')

    args = parser.parse_args()
    times = args.times
    sleeps = args.sleeps
    cookie = args.cookie
    token = args.token
    signature = args.signature
    mongo_url = args.mongo_url
    mongo_username = args.mongo_username
    mongo_password = args.mongo_password
    mongo_db = args.mongo_db
    mongo_collection = args.mongo_collection
    
    articles = toutiao_utils.get_user_articles(times, sleeps, cookie, token, signature)
    
    
    current_time = datetime.now()
    collect_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
    for article in articles:
        article['collect_time'] = collect_time
        article['_id'] = article['item_id'] + article['collect_time']
    
    mongon_uri = f"mongodb+srv://{mongo_username}:{mongo_password}@{mongo_url}/?retryWrites=true&w=majority"
    toutiao_utils.save_artices_to_mogono(articles, mongon_uri, mongo_db, mongo_collection, True, 10)
