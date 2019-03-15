
import mongo_prod
import common_prod
import mysql_prod
import pre_loading
import time
import random
import pytz
tz = pytz.timezone('Asia/Shanghai')
import datetime
import logging
from numpy.random import choice
import copy
from pytz import timezone, utc
import os



class DeliveryVars:
    def __init__(self):
        self.cur_income_logs_len = 0
        self.ads = dict()
        self.ads_count = dict()
        self.ads_other = dict()


def custom_time(*args):
    utc_dt = utc.localize(datetime.datetime.utcnow())
    my_tz = timezone("Asia/Shanghai")
    converted = utc_dt.astimezone(my_tz)
    return converted.timetuple()


logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
if not os.path.exists(r'./logs'):
    os.mkdir('./logs')
    if not os.path.exists('./ga_maker_log.txt'):
        fp = open("./logs/ga_maker_log.txt", 'w')
        fp.close()
handler = logging.FileHandler("./logs/ga_maker_log.txt", encoding="UTF-8")
handler.setLevel(logging.INFO)
logging.Formatter.converter = custom_time
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def create_pt(delt_name=None, create_amount=100, pre_dict=None):
    logger.info('delt_name:%s, create_amount:%d' % (delt_name, create_amount))
    if create_amount <= 0:
        return []
    if delt_name:
        key, country, roas_average_floor = mongo_prod.get_key_country(delt_name)
        logger.info('the platform: %s, the country: %s' % (key, country))
        if key is None or country is None:
            return []
    else:
        return []
    global_vars = DeliveryVars()
    global_vars.ads = pre_dict['ads']
    if len(global_vars.ads) < 1:
        return []
    ads_count = dict()
    ads_other = dict()
    for ad_id in global_vars.ads:
        ads_other[ad_id] = global_vars.ads[ad_id]['coef']
        if global_vars.ads[ad_id]['platform'] == key:
            ads_count[ad_id] = global_vars.ads[ad_id]['coef']
    if len(ads_count) == 0:
        return []
    global_vars.ads_count = common_prod.weighted(ads_count)
    global_vars.ads_other = ads_other
    if len(ads_count) == 0:
        return []
    pt_pool = list()
    while True:
        ''' 根据给定投放名称的平台随机获取一个父样本 '''
        father_ad_id = choice(list(global_vars.ads_count.keys()), 1, p=list(global_vars.ads_count.values()))[0]
        copy_other = copy.deepcopy(global_vars.ads_other)
        ''' 在全部中删除获取的父样本，然后再剩下的样本中随机选取一个母样本'''
        del copy_other[father_ad_id]
        if len(copy_other) == 0:
            break
        copy_other = common_prod.weighted(copy_other)
        mother_ad_id = choice(list(copy_other.keys()), 1, p=list(copy_other.values()))[0]
        pt_out = compose_baby(country, global_vars.ads[father_ad_id], global_vars.ads[mother_ad_id], delt_name,
                              roas_average_floor, pre_dict)
        pt_pool.extend(pt_out)
        if len(pt_pool) >= create_amount:
            break
    mongo_prod.insert_baits(pt_pool)
    mongo_prod.close()
    logger.info('create amount is %d' % (len(pt_pool)))
    return {'size': len(pt_pool)}


def compose_baby(country, father_ad, mother_ad, delt_name, roas_average_floor, pre_dict):
    columns = ['behaviors', 'genders', 'wireless_carrier']
    if father_ad['platform'] == mother_ad['platform']:
        columns.append('user_device')
    father_pt = copy.deepcopy(father_ad['pt'])
    mother_pt = copy.deepcopy(mother_ad['pt'])
    random_dim = choice(columns, 1)[0]
    if common_prod.node_exist(mother_pt, random_dim) and common_prod.node_exist(father_pt, random_dim):
        tmp = copy.deepcopy(mother_pt['adset_spec']['targeting'][random_dim])
        mother_pt['adset_spec']['targeting'][random_dim] = copy.deepcopy(father_pt['adset_spec']['targeting'][random_dim])
        father_pt['adset_spec']['targeting'][random_dim] = copy.deepcopy(tmp)

    if not common_prod.node_exist(father_pt, 'geo_locations') or \
       not common_prod.node_exist_geolocation(father_pt, 'custom_locations'):
        father_pt['adset_spec']['targeting']['geo_locations'] = {'custom_locations': None}
    if not common_prod.node_exist(mother_pt, 'geo_locations') or \
       not common_prod.node_exist_geolocation(mother_pt, 'custom_locations'):
        mother_pt['adset_spec']['targeting']['geo_locations'] = {'custom_locations': None}
    if mother_ad['country'] == country and father_ad['country'] == country:
        tmp = copy.deepcopy(mother_pt['adset_spec']['targeting']['geo_locations']['custom_locations'])
        mother_pt['adset_spec']['targeting']['geo_locations']['custom_locations'] = \
            copy.deepcopy(father_pt['adset_spec']['targeting']['geo_locations']['custom_locations'])
        father_pt['adset_spec']['targeting']['geo_locations']['custom_locations'] = copy.deepcopy(tmp)
    if mother_ad['country'] != country or not common_prod.node_exist_geolocation(mother_pt, 'custom_locations'):
        mother_pt['adset_spec']['targeting']['geo_locations']['custom_locations'] = choice_geolocation(pre_dict['geo_dict']['name'][mother_ad['country']],
                                                                                                       pre_dict['geo_dict']['weight'][mother_ad['country']])
    if father_ad['country'] != country or not common_prod.node_exist_geolocation(father_pt, 'custom_locations'):
        father_pt['adset_spec']['targeting']['geo_locations']['custom_locations'] = choice_geolocation(pre_dict['geo_dict']['name'][mother_ad['country']],
                                                                                                       pre_dict['geo_dict']['weight'][mother_ad['country']])

    father_pt['adset_spec']['targeting']['geo_locations']['countries'] = {'0': str(country)}
    mother_pt['adset_spec']['targeting']['geo_locations']['countries'] = {'0': str(country)}
    if len(father_pt['adset_spec']['targeting']['geo_locations']['custom_locations']) > 0:
        del father_pt['adset_spec']['targeting']['geo_locations']['countries']
    father_pt['adset_spec']['targeting']['interests'] = choice_interest(father_pt, mother_pt,
                                                                        pre_dict['interests_name'],
                                                                        pre_dict['interests_weight'])
    if father_ad['platform'] == mother_ad['platform'] and mother_ad['country'] == country:
        mother_pt['adset_spec']['targeting']['interests'] = choice_interest(mother_pt, father_pt,
                                                                            pre_dict['interests_name'],
                                                                            pre_dict['interests_weight'])
        if len(mother_pt['adset_spec']['targeting']['geo_locations']['custom_locations']) > 0:
            del mother_pt['adset_spec']['targeting']['geo_locations']['countries']
        return [common_prod.modify_pt(pt=father_pt, delt_name=delt_name, creative_medias=pre_dict['media_url'],
                                      country=country, platform=father_ad['platform'],
                                      roas_average_floor=roas_average_floor),
                common_prod.modify_pt(pt=mother_pt, delt_name=delt_name, creative_medias=pre_dict['media_url'],
                                      country=country, platform=mother_ad['platform'],
                                      roas_average_floor=roas_average_floor)]
    else:
        return [common_prod.modify_pt(pt=father_pt, delt_name=delt_name, creative_medias=pre_dict['media_url'],
                                      country=country, platform=father_ad['platform'],
                                      roas_average_floor=roas_average_floor)]


def choice_geolocation(geo_name=None, geo_weight=None):
    geo_location = list()
    if len(geo_name) > 100:
        tmp_all_weight = common_prod.weighted(geo_weight)
        rand_size = random.randint(30, 180)
        if 0 < len(tmp_all_weight) < rand_size:
            rand_size = random.randint(0, len(tmp_all_weight))
        elif len(tmp_all_weight) == 0:
            return geo_location

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
    if common_prod.node_exist(father_pt, 'interests'):
        finterests = father_pt['adset_spec']['targeting']['interests']
        if isinstance(finterests, list):
            for fi in finterests:
                fid[fi['id']] = 1
        elif isinstance(finterests, dict):
            for fi in finterests.values():
                fid[fi['id']] = 1

    if common_prod.node_exist(mother_pt, 'interests'):
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

    rand_size = round((400 - len(interests))*random.uniform(0.1, 0.5))
    ''' 随机一个[0.1,0.5],然后乘以已存在interests的量和400的差值，再作新生成的随机产生的量 '''
    if rand_size > 0:
        for tpid in tmp_ids:
            del tmp_all_weight[tpid]
        prob = list(common_prod.weighted(tmp_all_weight).values())
        rand_ids = choice(list(tmp_all_weight.keys()), rand_size, p=prob, replace=False)
        if rand_size <= len(tmp_all_weight):
            for rid in rand_ids:
                interests.append({'id': round(float(rid), 1), 'name': all_name[rid]})
    return interests


if __name__ == '__main__':
    tmp_dict = pre_loading.pre_main()
    (tmp_dict.keys())
    out = mongo_prod.test_ga_maker()
    lst = list()
    for ot in set(out):
        # try:
            t1 = time.time()
            print(ot)
            pts_size = create_pt(delt_name=ot, create_amount=100, pre_dict=tmp_dict)
            print(pts_size)
            print(time.time()-t1)
        # except:
        #     print(ot)
        #     lst.append(ot)
            pass
    # import pandas as pd
    # pd.DataFrame({'delt_name': lst}).to_csv('no_successful.csv', index=False)
    # mongo_prod.close()
