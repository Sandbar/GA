from pymongo import MongoClient
import datetime
from datetime import timedelta
import urllib
import pytz
from scipy import stats
import math
import configparser as cfg
import os


config = cfg.ConfigParser()
config.read(os.environ['config'])
# 建立MongoDB数据库连接
MONGO_USR = config.get('mongo_conf', 'mongo_usr')
MONGO_PWD = config.get('mongo_conf', 'mongo_pwd')
MONGO_HOST = config.get('mongo_conf', 'mongo_host')
MONGO_DBNAME = config.get('mongo_conf', 'mongo_dbname')
MONGO_CLUSTER = config.get('mongo_conf', 'mongo_cluster')
cluster_string = ''
if len(MONGO_CLUSTER) > 0:
    cluster_string = 'replicaSet=' + MONGO_CLUSTER + '&'
mongo_server_uri = 'mongodb://' + urllib.parse.quote_plus(MONGO_USR) + ':' + \
                   urllib.parse.quote_plus(MONGO_PWD) + '@' + MONGO_HOST + \
                   '/' + '?' + cluster_string + 'authSource=' + MONGO_DBNAME
tz = pytz.timezone('Asia/Shanghai')
install_stats = stats.norm(loc=50, scale=50)
roi_stats = stats.norm(loc=0.05, scale=0.1)
client = MongoClient(mongo_server_uri, maxPoolSize=200)


def get_income_logs_len():
    db = client.get_database(MONGO_DBNAME)
    out = db.income_logs.find({})
    record_len = len(list(out))
    return record_len


def select_ads():
    last5_date = (datetime.datetime.now(tz) - timedelta(5)).strftime('%Y-%m-%d')
    db = client.get_database(MONGO_DBNAME)
    out = db.incomes.find({'revenue': {'$gte': 1}, 'daily_install_count': {'$gte': 5},
                           'report_date': {'$gte': last5_date}})
    out_statis = dict()
    for cell in list(out):
        tmp_ad_id = cell['ad_id']
        coef = math.sqrt(install_stats.cdf(cell['daily_install_count']) * roi_stats.cdf(cell['roi']))
        if tmp_ad_id not in out_statis:
            out_statis[tmp_ad_id] = {'coef': coef}
        else:
            tmp_coef = out_statis[tmp_ad_id]['coef']
            if coef > tmp_coef:
                out_statis[tmp_ad_id]['coef'] = coef
    ads_id_array = list(out_statis.keys())
    ads_pts = get_pts(ads_id_array)
    for ad_id in ads_id_array:
        if 'pt' not in out_statis[ad_id]:
            if ad_id in ads_pts:
                out_statis[ad_id]['pt'] = ads_pts[ad_id]['pt']
                out_statis[ad_id]['country'] = ads_pts[ad_id]['country']
                out_statis[ad_id]['key'] = ads_pts[ad_id]['key']
                out_statis[ad_id]['platform'] = ads_pts[ad_id]['platform']
            else:
                del out_statis[ad_id]
    return out_statis


def get_pts(ads_array):
    db = client.get_database(MONGO_DBNAME)
    out_dict = dict()
    out = db.ads.find({"ad_id": {"$in": ads_array}}, {'ad_id': 1, 'pt': 1, 'delt_name': 1, '_id': 0})
    for row in list(out):
        if row['ad_id'] not in out_dict:
            country, platform, key = get_tuple(row['delt_name'])
            out_dict[row['ad_id']] = {'pt': row['pt'], 'country': country, 'platform': platform, 'key': key}
    return out_dict


def get_country(delt_name):
    db = client.get_database(MONGO_DBNAME)
    out = db.delivery.find({'name': delt_name}, {'country': 1, '_id': 0})
    country = list(out)[0]['country']
    return country


def get_platform(delt_name):
    db = client.get_database(MONGO_DBNAME)
    out = db.delivery.find({'name': delt_name}, {'platform': 1, '_id': 0})
    country = list(out)[0]['platform']
    return country


def get_key(delt_name):
    db = client.get_database(MONGO_DBNAME)
    out = db.delivery.find({'name': delt_name}, {'country': 1, 'platform': 1, '_id': 0})
    tmp_row = list(out)[0]
    country = tmp_row['country']
    platform = tmp_row['platform']
    return country + '|' + platform


def get_tuple(delt_name):
    db = client.get_database(MONGO_DBNAME)
    out = db.delivery.find({'name': delt_name}, {'country': 1, 'platform': 1, '_id': 0})
    tmp_row = list(out)[0]
    country = tmp_row['country']
    platform = tmp_row['platform']
    return country, platform, country + '|' + platform
