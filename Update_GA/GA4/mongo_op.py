from pymongo import MongoClient
import datetime
from datetime import timedelta
import pytz
from numpy.random import choice
import numpy as np


tz = pytz.timezone('Asia/Shanghai')
# install_stats = stats.norm(loc=50, scale=50)
# roi_stats = stats.norm(loc=0.05, scale=0.1)

import os

db_host = os.environ['db_host']
db_name = os.environ['db_name']
db_port = int(os.environ['db_port'])
db_user = os.environ['db_user']
db_pwd = os.environ['db_pwd']
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


def string_to_datetime(tdate):
    return datetime.datetime.strptime(tdate, "%Y-%m-%d")


def select_ads(tdate=None):
    tmp_day = 30
    pay = 0
    cost = 1
    install = 1
    # tmp_day, tmp_revenue, tmp_daily_install_count = parameter_setting(tmp_day, tmp_revenue, tmp_daily_install_count,
    #                                                                   create_amount)
    last_date = (string_to_datetime(tdate) - timedelta(tmp_day)).strftime('%Y-%m-%d')
    out = db.report.find({'cost': {'$gt': cost}, 'install': {'$gt': install}, 'pay': {'$gt': pay},
                          'cohort_date': {'$gt': last_date, '$lte': tdate}},
                         {'_id': 0, 'ad_id': 1, 'cohort_date': 1, 'country': 1, 'platform': 1})
    print(last_date, tdate)
    out_statis = dict()
    for rp in out:
        tid = rp['ad_id']
        if tid not in out_statis:
            out_statis[tid] = {'country': rp['country'], 'platform': rp['platform'], 'tdate': rp['cohort_date']}
        elif out_statis[tid]['tdate'] < rp['cohort_date']:
            out_statis[tid]['tdate'] = rp['cohort_date']
    ads_pts = get_pts(list(out_statis.keys()))
    ads_id_array = list(out_statis.keys())
    for ad_id in ads_id_array:
        if ad_id in ads_pts:
            out_statis[ad_id]['pt'] = ads_pts[ad_id]['pt']
            out_statis[ad_id]['delt_name'] = find_deltname(out_statis[ad_id]['country'],
                                                           out_statis[ad_id]['platform'],
                                                           ads_pts[ad_id]['delt_name'])
        else:
            del out_statis[ad_id]
    print('size: ', len(out_statis))
    return out_statis


def find_deltname(country, platform, delt_name):
    dn = db.delivery.find({'country': country, 'platform': platform,
                           'name': {'$regex': delt_name.split('_')[0], '$options': 'i'},
                           'class': {'$regex': '^S', '$options': 'i'}}, {'_id': 0, 'name': 1})
    if dn.count() > 0:
        return list(dn)[0]['name']
    return None


def find_delivery_by_cp(country, platform):
    timezone = db.delivery.find({'country': country, 'platform': platform,
                                 'class': {'$regex': '^S', '$options': 'i'}}, {'_id': 0, 'timezone': 1}).limit(1)
    return list(timezone)[0]['timezone']


def get_pts(ads_array):
    out_dict = dict()
    out = db.ads.find({"ad_id": {"$in": ads_array}}, {'ad_id': 1, 'pt': 1, 'delt_name': 1, '_id': 0})
    for row in out:
        if row['ad_id'] not in out_dict:
            out_dict[row['ad_id']] = {'pt': row['pt'], 'delt_name': row['delt_name']}
    return out_dict


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


def find_cp_by_deltname(delt_name=None):
    out = db.delivery.find({'name': delt_name}, {'platform': 1, 'country': 1, '_id': 0})
    out = list(out)
    if len(out) > 0:
        return out[0]['country'], out[0]['platform']
    return None, None


def get_key_country(delt_name=None):
    out = db.delivery.find({'name': delt_name}, {'platform': 1, 'country': 1, '_id': 0})
    out = list(out)
    if len(out) == 0:
        return None, None
    rand_index = choice(np.arange(len(out)), 1)[0]
    country = out[rand_index]['country']
    platform = out[rand_index]['platform']
    return platform, country


def insert_baits(pts=None):
    index = 0
    for pt in pts:
        print(pt['delt_name'])
        # db.baits_2018_09.insert(pt)
        index += 1
    return index


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
        ad_id = ads['ad_id']
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
    # print(get_key_country('bet3_us_c2_android_tz_r12'))
    # print(find_cp_by_deltname('bet3_us_c2_android_tz_r12'))
    # print(find_delivery_by_cp('US', 'Android'))
    print(select_ads('2018-09-17'))
    close()
