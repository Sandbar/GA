
from numpy.random import choice
import copy
import mongo_prod
import common_prod
import mysql_prod
import time
import random
import pytz
tz = pytz.timezone('Asia/Shanghai')
import datetime
import logging
from pytz import timezone, utc
import os



class DeliveryVars:
    def __init__(self):
        self.cur_income_logs_len = 0
        self.ads = dict()
        self.ads_count = dict()
        self.ads_other = dict()

def custom_time(*args):
    # 配置logger
    utc_dt = utc.localize(datetime.datetime.utcnow())
    my_tz = timezone("Asia/Shanghai")
    converted = utc_dt.astimezone(my_tz)
    return converted.timetuple()


logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
if os.path.exists(r'./logs') == False:
    os.mkdir('./logs')
    if os.path.exists('./ga_maker_log.txt') == False:
        fp = open("./logs/ga_maker_log.txt", 'w')
        fp.close()
handler = logging.FileHandler("./logs/ga_maker_log.txt", encoding="UTF-8")
handler.setLevel(logging.INFO)
logging.Formatter.converter = custom_time
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


''' start '''
def create_pt(delt_name=None, create_amount=10):
    # print(delt_name, create_amount)
    logger.info('start create pt, two parameters are delt_name:%s, create_amount:%d' % (delt_name, create_amount))
    if create_amount <= 0:
        return []
    if delt_name:
        key, country = mongo_prod.get_key_country(delt_name)
        logger.info('the platform: %s, the country: %s' % (key, country))
        if key is None or country is None:
            return []
    else:
        return []
    global_vars = DeliveryVars()
    cur_len = mongo_prod.get_income_logs_len()
    if global_vars.cur_income_logs_len != cur_len:
        global_vars.cur_income_logs_len = cur_len
        global_vars.ads = mongo_prod.select_ads(create_amount)
        logger.info('the size of global_vars.ads is  %d' % (len(global_vars.ads)))
        if len(global_vars.ads) == 0:
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
        logger.info('the size of same platform is %d, other platform is %d' % (len(ads_count), len(ads_other)))
        if len(ads_count) == 0:
            return []
    pt_pool = list()
    ''' 获取interests信息的属性值名称以及权重 '''
    all_name, all_weight = mysql_prod.select_weigth()
    logger.info('the size of interests in dw_dim_interests is  %d' % (len(all_name)))
    ''' 根据国家获取坐标信息 '''
    geo_name, geo_weight = mysql_prod.select_geo_weigth(country)
    logger.info('the size of coordinates in dw_dim_coordinate is  %d' % (len(geo_name)))

    creative_media = mysql_prod.select_url()
    logger.info('the size of creative_media is %d' % len(creative_media))
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
        pt_out = compose_baby(country, global_vars.ads[father_ad_id], global_vars.ads[mother_ad_id], all_name,
                              all_weight, geo_name, geo_weight, delt_name, creative_media)
        for pp in pt_out:
            pt_pool.append(pp)
        if len(pt_pool) >= create_amount:
            break
    mongo_prod.close()
    logger.info('create amount pt is  %d' % (len(pt_pool)))
    import json
    json.dumps(pt_pool)
    return pt_pool

''' 遗传子代 '''
def compose_baby(country, father_ad, mother_ad, all_name, all_weight, geo_name, geo_weight, delt_name, creative_media):
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

    if common_prod.node_exist(father_pt, 'geo_locations') == False or \
            common_prod.node_exist_geolocation(father_pt, 'custom_locations') == False:
        father_pt['adset_spec']['targeting']['geo_locations'] = {'custom_locations': None}
    if common_prod.node_exist(mother_pt, 'geo_locations') == False or \
            common_prod.node_exist_geolocation(mother_pt, 'custom_locations') == False:
        mother_pt['adset_spec']['targeting']['geo_locations'] = {'custom_locations': None}
    if mother_ad['country'] == country and father_ad['country'] == country:
        tmp = copy.deepcopy(mother_pt['adset_spec']['targeting']['geo_locations']['custom_locations'])
        mother_pt['adset_spec']['targeting']['geo_locations']['custom_locations'] = \
            copy.deepcopy(father_pt['adset_spec']['targeting']['geo_locations']['custom_locations'])
        father_pt['adset_spec']['targeting']['geo_locations']['custom_locations'] = copy.deepcopy(tmp)

    if mother_ad['country'] != country or common_prod.node_exist_geolocation(mother_pt, 'custom_locations') == False:
        mother_pt['adset_spec']['targeting']['geo_locations']['custom_locations'] = choice_geolocation(geo_name, geo_weight)
    if father_ad['country'] != country or common_prod.node_exist_geolocation(father_pt, 'custom_locations') == False:
        father_pt['adset_spec']['targeting']['geo_locations']['custom_locations'] = choice_geolocation(geo_name, geo_weight)

    father_pt['adset_spec']['targeting']['geo_locations']['countries'] = [str(country)]
    mother_pt['adset_spec']['targeting']['geo_locations']['countries'] = [str(country)]
    if len(father_pt['adset_spec']['targeting']['geo_locations']['custom_locations']) > 0:
        del father_pt['adset_spec']['targeting']['geo_locations']['countries']
    father_pt['adset_spec']['targeting']['interests'] = choice_interest(father_pt, mother_pt, all_name, all_weight)
    if father_ad['platform'] == mother_ad['platform'] and mother_ad['country'] == country:
        mother_pt['adset_spec']['targeting']['interests'] = choice_interest(mother_pt, father_pt, all_name, all_weight)
        if len(mother_pt['adset_spec']['targeting']['geo_locations']['custom_locations']) > 0:
            del mother_pt['adset_spec']['targeting']['geo_locations']['countries']
        return [common_prod.modify_pt(pt=father_pt, delt_name=delt_name, sign='[GA3]', creative_medias=creative_media),
                common_prod.modify_pt(pt=mother_pt, delt_name=delt_name, sign='[GA3]', creative_medias=creative_media)]
    else:
        return [common_prod.modify_pt(pt=father_pt, delt_name=delt_name, sign='[GA3]', creative_medias=creative_media)]


''' 这部分是父母都没有直接进行weight选择 '''
def choice_geolocation(geo_name=None, geo_weight=None):
    geo_location = list()
    if len(geo_name) > 0:
        tmp_all_weight = common_prod.weighted(geo_weight)
        rand_size = random.randint(10, 160)
        rand_ids = choice(list(tmp_all_weight.keys()), rand_size, p=list(tmp_all_weight.values()), replace=False)
        for rids in rand_ids:
            rid = rids.split('_')
            if len(rid) == 3:
                geo_location.append({'latitude': rid[0], 'longitude': rid[1],
                                     'radius': rid[2], 'distance_unit': geo_name[rids]})
    return geo_location


''' 先进行通过父母的遗传，再通过weight进行选择来补充子代的兴趣数量 '''
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
            finterests = finterests.values()
            for fi in finterests.items():
                fid[fi['id']] = 1

    if common_prod.node_exist(mother_pt, 'interests'):
        minterests = mother_pt['adset_spec']['targeting']['interests']
        if isinstance(minterests, list):
            for mi in minterests:
                mid[mi['id']] = 1
        elif isinstance(minterests, dict):
            minterests = minterests.values()
            for mi in minterests.items():
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

    rand_size = round((400 - len(interests))*random.uniform(0.1, 0.4))
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
    out = mongo_prod.test_ga_maker()
    for ot in out:
        t1 = time.time()
        # pt_pool = create_pt(delt_name='bbt3_kr_android', create_amount=1)
        pt_pool = create_pt(delt_name=ot, create_amount=1000)
        for pt in pt_pool:
            print(pt)
        print(len(pt_pool))
        print(time.time()-t1)
    mongo_prod.close()
