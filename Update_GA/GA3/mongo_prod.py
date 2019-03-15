from pymongo import MongoClient
import datetime
from datetime import timedelta
import urllib
import pytz
from scipy import stats
import math
from numpy.random import choice
import numpy as np
import math
import os

tz = pytz.timezone('Asia/Shanghai')
install_stats = stats.norm(loc=50, scale=50)
roi_stats = stats.norm(loc=0.05, scale=0.1)


db_host = os.enviorn['db_host']
db_name = os.enviorn['db_name']
db_port = int(os.enviorn['db_port'])
db_user = os.enviorn['db_user']
db_pwd = os.enviorn['db_pwd']
db_report_name = 'report'
client = MongoClient(db_host, db_port, maxPoolSize=200)
db = client.get_database(db_name)
db.authenticate(db_user, db_pwd)


def get_income_logs_len():
    out = db.income_logs.find({})
    record_len = len(list(out))
    return record_len


def parameter_setting(tmp_day=7, tmp_revenue=0.1, tmp_daily_install_count=1, create_amount=0):
    sign = 1
    while True:
        last5_date = (datetime.datetime.now(tz) - timedelta(tmp_day)).strftime('%Y-%m-%d')
        res = db.incomes.find({'revenue': {'$gte': tmp_revenue}, 'daily_install_count': {'$gte': tmp_daily_install_count},
                               'report_date': {'$gte': last5_date}}, {'_id': 0, 'ad_id': 1}).count()
        if res >= create_amount or (tmp_day == 14 and tmp_daily_install_count <= 2 and tmp_revenue <= 0.2):
            return tmp_day, tmp_revenue, tmp_daily_install_count
        elif sign == 1 and tmp_day < 14:
            tmp_day += 2
            sign = 1 - sign
        elif (sign == 0 or tmp_day == 14) and (tmp_daily_install_count > 0 or tmp_revenue > 0):
            sign = 1 - sign
            if tmp_daily_install_count >= 3:
                tmp_daily_install_count -= 2
            if tmp_revenue >= 0.3:
                tmp_revenue -= 0.2
        else:
            return tmp_day, tmp_revenue, tmp_daily_install_count


def select_ads(create_amount=0):
    tmp_day = 14
    tmp_revenue = 0.1
    tmp_daily_install_count = 2
    tmp_day, tmp_revenue, tmp_daily_install_count = parameter_setting(tmp_day, tmp_revenue, tmp_daily_install_count,
                                                                      create_amount)
    last_date = (datetime.datetime.now(tz) - timedelta(tmp_day)).strftime('%Y-%m-%d')
    out = db.incomes.find({'revenue': {'$gte': tmp_revenue}, 'daily_install_count': {'$gte': tmp_daily_install_count},
                           'report_date': {'$gte': last_date}})
    # out = list(out)
    out_statis = dict()
    for cell in out:
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
                out_statis[ad_id]['platform'] = ads_pts[ad_id]['platform']
            else:
                del out_statis[ad_id]
    return out_statis


def get_pts(ads_array):
    out_dict = dict()
    out = db.ads.find({"ad_id": {"$in": ads_array}}, {'ad_id': 1, 'pt': 1, 'delt_name': 1, '_id': 0})
    for row in out:
        if row['ad_id'] not in out_dict:

            country = ''
            platform = ''
            tdelt_name = row['delt_name'].split('_')
            if len(tdelt_name) == 3:
                if tdelt_name[1].upper() in ['IOS', 'ADR', 'ANDROID']:
                    platform = 'Android'
                    if tdelt_name[1].upper() == 'IOS':
                        platform = 'iOS'
                    country = tdelt_name[2].upper()
                elif tdelt_name[2].upper() in ['IOS', 'ADR', 'ANDROID']:
                    platform = 'Android'
                    if tdelt_name[2].upper() == 'IOS':
                        platform = 'iOS'
                    country = tdelt_name[1].upper()
            out_dict[row['ad_id']] = {'pt': row['pt'], 'country': country, 'platform': platform}
    return out_dict


def get_tuple(delt_name=None):
    cp = dict()
    if delt_name:
        out = db.delivery.find({'name': delt_name}, {'country': 1, 'platform': 1, '_id': 0})
        out = list(out)
        if len(out) == 0:
            print(delt_name)
        if len(out) > 0:
            cp = out[0]
            return cp
    tcps = delt_name.split('_')
    if cp and len(tcps) == 3:
        cp['country'] = tcps[2].upper()
        if tcps[1].upper() == 'IOS':
            cp['platform'] = 'iOS'
        else:
            cp['platform'] = 'Android'
        return cp
    else:
        out = db.delivery.find({}, {'country': 1, 'platform': 1, '_id': 0})
        out = list(out)
        if len(out) > 0:
            rand_index = choice(np.arange(len(out)), 1)[0]
            cp = out[rand_index]
            return cp
    return {'country': '', 'platform': ''}


def get_key():
    out = db.delivery.find({}, {'platform': 1, '_id': 0})
    out = list(out)
    rand_index = choice(np.arange(len(out)), 1)[0]
    platform = out[rand_index]['platform']
    return platform


def get_country():
    out = db.delivery.find({}, {'country': 1, '_id': 0})
    out = list(out)
    rand_index = choice(np.arange(len(out)), 1)[0]
    country = out[rand_index]['country']
    return country


def get_key_country(delt_name=None):
    out = db.delivery.find({'name': delt_name}, {'platform': 1, 'country': 1, '_id': 0})
    out = list(out)
    if len(out) == 0:
        return None, None
    rand_index = choice(np.arange(len(out)), 1)[0]
    country = out[rand_index]['country']
    platform = out[rand_index]['platform']
    return platform, country


def close():
    client.close()


def test_ga_maker():
    out = db.delivery.distinct('name')
    out = list(out)
    return out


def test():
    ad_ids = db.report.distinct('ad_id')
    print(ad_ids)
    colles_ads = db.ads.find({'ad_id': {'$in': ad_ids}}, {'_id': 0, 'ad_id': 1,
                                                              'pt.adset_spec.targeting.geo_locations': 1})
    print('ads。。。')
    size = list()
    for ads in colles_ads:
        pt = ads['pt']
        if pt.get('adset_spec') and pt['adset_spec'].get('targeting') and \
            pt['adset_spec']['targeting'].get('geo_locations') and \
            pt['adset_spec']['targeting']['geo_locations'].get('custom_locations'):
                custom_locations = pt['adset_spec']['targeting']['geo_locations']['custom_locations']
                print(custom_locations)
                size.append(len(custom_locations))
    import pandas as pd
    pd.DataFrame({'size': size}).to_csv('location_size.csv',index=False)
    print(max(size), min(size))
    pass



if __name__ == '__main__':
    print(get_key_country('bet3_us_c2_android_tz_r12'))
    close()
