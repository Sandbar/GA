
from numpy.random import choice
import copy
import time
import random
import pytz
import datetime
import logging
from pytz import timezone, utc
import os
import mongo_op
import common_op
import mysql_op
from datetime import timedelta
tz = pytz.timezone('Asia/Shanghai')


class DeliveryVars:
    def __init__(self):
        self.ads = dict()
        self.tads = dict()
        # self.ads_other = dict()


def custom_time():
    # 配置logger
    utc_dt = utc.localize(datetime.datetime.utcnow())
    my_tz = timezone("Asia/Shanghai")
    converted = utc_dt.astimezone(my_tz)
    return converted.timetuple()


logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
if not os.path.exists(r'./logs'):
    os.mkdir('./logs')
    if not os.path.exists('./ga4_maker_log.txt'):
        fp = open("./logs/ga4_maker_log.txt", 'w')
        fp.close()
handler = logging.FileHandler("./logs/ga4_maker_log.txt", encoding="UTF-8")
handler.setLevel(logging.INFO)
logging.Formatter.converter = custom_time
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def ga4_maker_pts(tdate=None):
    if not tdate:
        return []
    print('start...')
    global_vars = DeliveryVars()
    global_vars.ads = mongo_op.select_ads(tdate)
    if len(global_vars.ads) == 0:
        return []
    for ad_id in global_vars.ads.keys():
        if global_vars.ads[ad_id]['tdate'] == tdate:
            global_vars.tads[ad_id] = global_vars.ads[ad_id]

    ''' 获取interests信息的属性值名称以及权重 '''
    all_name, all_weight = mysql_op.select_weigth()
    creative_media = mysql_op.select_url()
    pt_pool = list()
    for fad_id in global_vars.tads.keys():
        father = global_vars.tads[fad_id]
        del global_vars.ads[fad_id]
        for mad_id in global_vars.ads.keys():
            mother = global_vars.ads[mad_id]
            t1 = time.time()
            pt_out = compose_baby(father, mother, all_name, all_weight, creative_media)
            print(time.time()-t1)
            for pp in pt_out:
                pt_pool.append(pp)
    mongo_op.insert_baits(pt_pool)
    mongo_op.close()
    return len(pt_pool)


def compose_baby(father_ad, mother_ad, all_name, all_weight, creative_media):
    columns = ['behaviors', 'genders', 'wireless_carrier']
    if father_ad['platform'] == mother_ad['platform']:
        columns.append('user_device')
    father_pt = copy.deepcopy(father_ad['pt'])
    mother_pt = copy.deepcopy(mother_ad['pt'])
    random_dim = choice(columns, 1)[0]
    if common_op.node_exist(mother_pt, random_dim) and common_op.node_exist(father_pt, random_dim):
        tmp = copy.deepcopy(mother_pt['adset_spec']['targeting'][random_dim])
        mother_pt['adset_spec']['targeting'][random_dim] = copy.deepcopy(father_pt['adset_spec']['targeting'][random_dim])
        father_pt['adset_spec']['targeting'][random_dim] = copy.deepcopy(tmp)

    if not common_op.node_exist(father_pt, 'geo_locations') or \
       not common_op.node_exist_geolocation(father_pt, 'custom_locations'):
        father_pt['adset_spec']['targeting']['geo_locations'] = {'custom_locations': None}
    if not common_op.node_exist(mother_pt, 'geo_locations') or \
       not common_op.node_exist_geolocation(mother_pt, 'custom_locations'):
        mother_pt['adset_spec']['targeting']['geo_locations'] = {'custom_locations': None}
    if mother_ad['country'] == father_ad['country']:
        tmp = copy.deepcopy(mother_pt['adset_spec']['targeting']['geo_locations']['custom_locations'])
        mother_pt['adset_spec']['targeting']['geo_locations']['custom_locations'] = \
            copy.deepcopy(father_pt['adset_spec']['targeting']['geo_locations']['custom_locations'])
        father_pt['adset_spec']['targeting']['geo_locations']['custom_locations'] = copy.deepcopy(tmp)

    if not common_op.node_exist_geolocation(mother_pt, 'custom_locations'):
        ''' 根据国家获取坐标信息 '''
        geo_name, geo_weight = mysql_op.select_geo_weigth(mother_ad['country'])
        mother_pt['adset_spec']['targeting']['geo_locations']['custom_locations'] = choice_geolocation(geo_name,
                                                                                                       geo_weight)
    if not common_op.node_exist_geolocation(father_pt, 'custom_locations'):
        ''' 根据国家获取坐标信息 '''
        geo_name, geo_weight = mysql_op.select_geo_weigth(father_ad['country'])
        father_pt['adset_spec']['targeting']['geo_locations']['custom_locations'] = choice_geolocation(geo_name,
                                                                                                       geo_weight)

    father_pt['adset_spec']['targeting']['geo_locations']['countries'] = []
    mother_pt['adset_spec']['targeting']['geo_locations']['countries'] = []
    if len(father_pt['adset_spec']['targeting']['geo_locations']['custom_locations']) > 0:
        del father_pt['adset_spec']['targeting']['geo_locations']['countries']
    father_pt['adset_spec']['targeting']['interests'] = choice_interest(father_pt, mother_pt, all_name, all_weight)
    if father_ad['platform'] == mother_ad['platform']:
        mother_pt['adset_spec']['targeting']['interests'] = choice_interest(mother_pt, father_pt, all_name, all_weight)
        if len(mother_pt['adset_spec']['targeting']['geo_locations']['custom_locations']) > 0:
            del mother_pt['adset_spec']['targeting']['geo_locations']['countries']

        return [common_op.modify_infos(pt=father_pt, delt_name=father_ad['delt_name'], creative_medias=creative_media,
                                       country=father_ad['country'], platform=father_ad['platform']),
                common_op.modify_infos(pt=mother_pt, delt_name=mother_ad['delt_name'], creative_medias=creative_media,
                                       country=father_ad['country'], platform=father_ad['platform'])]
    else:
        return [common_op.modify_infos(pt=father_pt, delt_name=father_ad['delt_name'], creative_medias=creative_media,
                                       country=father_ad['country'], platform=father_ad['platform'])]


def choice_geolocation(geo_name=None, geo_weight=None):
    geo_location = list()
    if len(geo_name) > 0:
        tmp_all_weight = common_op.weighted(geo_weight)
        rand_size = random.randint(10, 160)
        rand_ids = choice(list(tmp_all_weight.keys()), rand_size, p=list(tmp_all_weight.values()), replace=False)
        for rids in rand_ids:
            rid = rids.split('_')
            if len(rid) == 3:
                geo_location.append({'latitude': rid[0], 'longitude': rid[1],
                                     'radius': rid[2], 'distance_unit': geo_name[rids]})
    return geo_location


def choice_interest(father_pt, mother_pt, all_name, all_weight):
    interests = list()
    if len(all_name) == 0:
        return interests

    tmp_all_weight = copy.deepcopy(all_weight)
    fid = dict()
    mid = dict()
    tmp_ids = []
    if common_op.node_exist(father_pt, 'interests'):
        finterests = father_pt['adset_spec']['targeting']['interests']
        if isinstance(finterests, list):
            for fi in finterests:
                fid[fi['id']] = 1
        elif isinstance(finterests, dict):
            for fi in finterests.values():
                fid[fi['id']] = 1

    if common_op.node_exist(mother_pt, 'interests'):
        minterests = mother_pt['adset_spec']['targeting']['interests']
        if isinstance(minterests, list):
            for mi in minterests:
                mid[mi['id']] = 1
        elif isinstance(minterests, dict):
            for mi in minterests.values():
                mid[mi['id']] = 1

    mid_weight = all_weight[list(tmp_all_weight.keys())[int(len(all_weight)/2)]]
    for fk in list(fid.keys()):
        if all_weight.get(fk):
            if mid.get(fk):
                interests.append({'id': fk, 'name': all_name[fk]})
                del mid[fk]
            elif all_weight[fk] >= mid_weight:
                interests.append({'id': fk, 'name': all_name[fk]})
            tmp_ids.append(fk)

    for mk in mid.keys():
        if all_weight.get(mk):
            if all_weight[mk] >= mid_weight and len(interests) < 400:
                interests.append({'id': mk, 'name': all_name[mk]})
            tmp_ids.append(mk)

    rand_size = round((400 - len(interests))*random.uniform(0.08, 0.3))
    ''' 随机一个[0.05,0.2],然后乘以已存在interests的量和400的差值，再作新生成的随机产生的量 '''
    if rand_size > 0:
        for tpid in tmp_ids:
            del tmp_all_weight[tpid]
        prob = list(common_op.weighted(tmp_all_weight).values())
        rand_ids = choice(list(tmp_all_weight.keys()), rand_size, p=prob, replace=False)
        if rand_size <= len(tmp_all_weight):
            for rid in rand_ids:
                interests.append({'id': round(float(rid), 1), 'name': all_name[rid]})
    return interests


if __name__ == '__main__':
    lst = list()
    for index in range(0, 100):
        tmp_date = str((mongo_op.string_to_datetime('2018-08-01') + timedelta(index)).strftime('%Y-%m-%d'))
        # try:
        if tmp_date < '2018-09-28':
            t1 = time.time()
            print(tmp_date)
            pt_pool = ga4_maker_pts(tmp_date)
            for pt in pt_pool:
                mongo_op.insert_baits(pt)
            print(len(pt_pool))
            print(time.time()-t1)
        # except:
        #     lst.append(tmp_date)
    mongo_op.close()

    # import pandas as pd
    # pd.DataFrame({'tdate': lst}).to_csv('no_successful_date.csv', index=False)
