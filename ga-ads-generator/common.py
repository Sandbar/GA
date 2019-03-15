import datetime
import re
import pytz
tz = pytz.timezone('Asia/Shanghai')


def weighted(dic_obj):
    sum_value = sum(list(dic_obj.values()))
    for key in dic_obj:
        dic_obj[key] = dic_obj[key]/sum_value
    return dic_obj


def node_exist(pt, node_name):
    if node_name in pt['adset_spec']['targeting']:
        return True
    else:
        return False


def modify_pt(pt, method):
    cur_date = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    new_name = re.subn(r"(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2})", cur_date, pt['name'])[0]
    new_name = new_name + ' ' + method
    pt['name'] = new_name
    pt['adset_spec']['name'] = new_name
    pt['adset_spec']['campaign_spec']['name'] = new_name
    return pt
