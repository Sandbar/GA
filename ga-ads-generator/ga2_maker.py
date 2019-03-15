import mongo
from numpy.random import choice
import copy
import common


class DeliveryVars:
    def __init__(self):
        self.cur_income_logs_len = 0
        self.ads = dict()
        self.ads_count = dict()
        self.ads_other = dict()


global_vars = dict()


def build_baby(delt_name, create_amount):
    key = mongo.get_key(delt_name)
    if key not in global_vars:
        global_vars[key] = DeliveryVars()
    cur_len = mongo.get_income_logs_len()
    if global_vars[key].cur_income_logs_len != cur_len:
        global_vars[key].cur_income_logs_len = cur_len
        global_vars[key].ads = mongo.select_ads()
        if len(global_vars[key].ads) == 0:
            return []
        ads_count = dict()
        ads_other = dict()
        for ad_id in global_vars[key].ads:
            ads_other[ad_id] = global_vars[key].ads[ad_id]['coef']
            if global_vars[key].ads[ad_id]['key'] == key:
                ads_count[ad_id] = global_vars[key].ads[ad_id]['coef']
        if len(ads_count) == 0:
            return []
        global_vars[key].ads_count = common.weighted(ads_count)
        global_vars[key].ads_other = ads_other
    pt_pool = list()
    while True:
        # 从ads_count中随机摇一个广告
        father_ad_id = choice(list(global_vars[key].ads_count.keys()), 1,
                              p=list(global_vars[key].ads_count.values()))[0]
        # 踢除该广告，从剩余的广告中再摇出一个广告
        copy_other = copy.deepcopy(global_vars[key].ads_other)
        del copy_other[father_ad_id]
        copy_other = common.weighted(copy_other)
        mother_ad_id = choice(list(copy_other.keys()), 1, p=list(copy_other.values()))[0]
        # 组合生成新PT
        pt_out = compose_baby(global_vars[key].ads[father_ad_id], global_vars[key].ads[mother_ad_id])
        for pt in pt_out:
            pt_pool.append(pt)
        if len(pt_pool) >= create_amount:
            break
    return pt_pool


def compose_baby(father_ad, mother_ad):
    # 选取一个可以交叉重组的维度
    columns = ['genders', 'interests', 'wireless_carrier']
    if father_ad['country'] == mother_ad['country']:
        columns.append('behaviors')
        columns.append('geo_locations')
    if father_ad['platform'] == mother_ad['platform']:
        columns.append('user_device')
    random_dim = choice(columns, 1)[0]
    while common.node_exist(father_ad['pt'], random_dim) is False and \
            common.node_exist(mother_ad['pt'], random_dim) is False:
        random_dim = choice(columns, 1)[0]
    father_pt = copy.deepcopy(father_ad['pt'])
    mother_pt = copy.deepcopy(mother_ad['pt'])
    if common.node_exist(father_pt, random_dim) is True and \
            common.node_exist(mother_pt, random_dim) is True:
        tmp_value = copy.deepcopy(father_pt['adset_spec']['targeting'][random_dim])
        father_pt['adset_spec']['targeting'][random_dim] = \
            copy.deepcopy(mother_pt['adset_spec']['targeting'][random_dim])
        mother_pt['adset_spec']['targeting'][random_dim] = tmp_value
    elif common.node_exist(father_pt, random_dim) is False:
        tmp_value = copy.deepcopy(mother_pt['adset_spec']['targeting'][random_dim])
        father_pt['adset_spec']['targeting'][random_dim] = tmp_value
        del mother_pt['adset_spec']['targeting'][random_dim]
    elif common.node_exist(mother_pt, random_dim) is False:
        tmp_value = copy.deepcopy(father_pt['adset_spec']['targeting'][random_dim])
        mother_pt['adset_spec']['targeting'][random_dim] = tmp_value
        del father_pt['adset_spec']['targeting'][random_dim]
    if father_ad['country'] == mother_ad['country'] and father_ad['platform'] == mother_ad['platform']:
        return [common.modify_pt(father_pt, '[GA2]'), common.modify_pt(mother_pt, '[GA2]')]
    else:
        return [common.modify_pt(father_pt, '[GA2]')]
