import mongo
from numpy.random import choice
import random
import object
import copy
import common


class DeliveryVars:
    def __init__(self):
        self.cur_income_logs_len = 0
        self.ads_count = dict()
        self.ads = dict()
        self.interests = dict()
        self.behaviors = dict()
        self.coordinates = dict()


global_vars = dict()


def select(dic_obj, max_num):
    if max_num >= 10:
        num = random.sample(range(int(max_num/3), max_num+1), 1)[0]
    else:
        num = max_num
    sub_out = choice(list(dic_obj.keys()), num, p=list(dic_obj.values()), replace=False)
    out = list()
    for value in sub_out:
        out.append(value.to_json())
    return out


def father_stats(ads_tmp):
    interests_temp = dict()
    behaviors_temp = dict()
    coordinates_temp = dict()
    for ad_id in ads_tmp:
        country = ads_tmp[ad_id]['country']
        if common.node_exist(ads_tmp[ad_id]['pt'], 'interests'):
            out = ads_tmp[ad_id]['pt']['adset_spec']['targeting']['interests']
            if type(out) is dict:
                out = out.values()
            for value in out:
                tmp = object.IntObj(value['id'], value['name'])
                if tmp not in interests_temp:
                    interests_temp[tmp] = ads_tmp[ad_id]['coef']
                else:
                    interests_temp[tmp] = interests_temp[tmp] + ads_tmp[ad_id]['coef']
        if common.node_exist(ads_tmp[ad_id]['pt'], 'behaviors'):
            out = ads_tmp[ad_id]['pt']['adset_spec']['targeting']['behaviors']
            if type(out) is dict:
                out = out.values()
            for value in out:
                tmp = object.BevObj(value['id'], value['name'])
                if 'US' not in behaviors_temp:
                    behaviors_temp['US'] = {tmp: ads_tmp[ad_id]['coef']}
                else:
                    if value['id'] not in behaviors_temp['US']:
                        behaviors_temp['US'][tmp] = ads_tmp[ad_id]['coef']
                    else:
                        behaviors_temp['US'][tmp] = behaviors_temp['US'][tmp] + ads_tmp[ad_id]['coef']
                if country != 'US':
                    if 'NonUS' not in behaviors_temp:
                        behaviors_temp['NonUS'] = {tmp: ads_tmp[ad_id]['coef']}
                    else:
                        if value['id'] not in behaviors_temp['NonUS']:
                            behaviors_temp['NonUS'][tmp] = ads_tmp[ad_id]['coef']
                        else:
                            behaviors_temp['NonUS'][tmp] = behaviors_temp['NonUS'][tmp] + ads_tmp[ad_id]['coef']
        if 'custom_locations' in ads_tmp[ad_id]['pt']['adset_spec']['targeting']['geo_locations']:
            out = ads_tmp[ad_id]['pt']['adset_spec']['targeting']['geo_locations']['custom_locations']
            if type(out) is dict:
                out = out.values()
            for value in out:
                tmp = object.CorObj(value['distance_unit'], value['latitude'], value['longitude'], value['radius'])
                if country not in coordinates_temp:
                    coordinates_temp[country] = {tmp: ads_tmp[ad_id]['coef']}
                else:
                    if tmp not in coordinates_temp[country]:
                        coordinates_temp[country][tmp] = ads_tmp[ad_id]['coef']
                    else:
                        coordinates_temp[country][tmp] = coordinates_temp[country][tmp] + ads_tmp[ad_id]['coef']
    return interests_temp, behaviors_temp, coordinates_temp


def build_baby(delt_name, create_amount):
    key = mongo.get_key(delt_name)
    if key not in global_vars:
        global_vars[key] = DeliveryVars()
    cur_len = mongo.get_income_logs_len()
    out_pts = []
    genders = [[1], [2], [1, 2]]
    if global_vars[key].cur_income_logs_len != cur_len:
        global_vars[key].cur_income_logs_len = cur_len
        global_vars[key].ads = mongo.select_ads()
        if len(global_vars[key].ads) == 0:
            return []
        ads_count = dict()
        for ad_id in global_vars[key].ads:
            if global_vars[key].ads[ad_id]['key'] == key:
                ads_count[ad_id] = global_vars[key].ads[ad_id]['coef']
        if len(ads_count) == 0:
            return []
        global_vars[key].ads_count = common.weighted(ads_count)
        interests, behaviors, coordinates = father_stats(global_vars[key].ads)
        global_vars[key].interests = interests
        global_vars[key].behaviors = behaviors
        global_vars[key].coordinates = coordinates

    ads_out = choice(list(global_vars[key].ads_count.keys()), create_amount,
                     p=list(global_vars[key].ads_count.values()))
    ads_pool = dict()
    for ad_id in ads_out:
        if ad_id not in ads_pool:
            ads_pool[ad_id] = 1
        else:
            ads_pool[ad_id] = ads_pool[ad_id] + 1
    # 为每个父样本产生指定数量的子代
    for ad_id in ads_pool:
        country = global_vars[key].ads[ad_id]['country']
        columns = list()
        columns.append('genders')
        if common.node_exist(global_vars[key].ads[ad_id]['pt'], 'interests'):
            columns.append('interests')
        if common.node_exist(global_vars[key].ads[ad_id]['pt'], 'behaviors'):
            columns.append('behaviors')
        if 'custom_locations' in global_vars[key].ads[ad_id]['pt']['adset_spec']['targeting']['geo_locations']:
            columns.append('custom_locations')
        for i in range(0, ads_pool[ad_id]):
            tmp_pt = copy.deepcopy(global_vars[key].ads[ad_id]['pt'])
            random_dim = choice(columns, 1)[0]
            if random_dim == 'genders':
                tmp_pt['adset_spec']['targeting']['genders'] = choice(genders, 1)[0]
            if random_dim == 'interests':
                int_len = len(tmp_pt['adset_spec']['targeting']['interests'])
                tmp_pt['adset_spec']['targeting']['interests'] = \
                    select(common.weighted(global_vars[key].interests), int_len)
            if random_dim == 'behaviors':
                bev_len = len(tmp_pt['adset_spec']['targeting']['behaviors'])
                if country == 'US':
                    wts_obj = common.weighted(global_vars[key].behaviors['US'])
                else:
                    wts_obj = common.weighted(global_vars[key].behaviors['NonUS'])
                tmp_pt['adset_spec']['targeting']['behaviors'] = select(wts_obj, bev_len)
            if random_dim == 'custom_locations':
                loc_len = len(tmp_pt['adset_spec']['targeting']['geo_locations']['custom_locations'])
                tmp_pt['adset_spec']['targeting']['geo_locations']['custom_locations'] = \
                    select(common.weighted(global_vars[key].coordinates[country]), loc_len)
            #替换日期和时间
            out_pts.append(common.modify_pt(tmp_pt, '[GA1]'))
    return out_pts
