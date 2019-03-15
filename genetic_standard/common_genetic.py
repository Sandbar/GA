

import datetime
import re
import pytz
tz = pytz.timezone('Asia/Shanghai')
import uuid
import os
import pandas as pd

def load_modify_ads(ads):
    tmp_ads = dict()
    if 'ad_id' in ads:
        tmp_ads['ad_id']= ads['ad_id']
    if 'pt' in ads:
        tmp_ads['pt'] = ads['pt']
        tmp_ads['type'] = None
        if 'adset_spec' in ads['pt'] and'targeting' in ads['pt']['adset_spec']:
            if 'custom_audiences' in ads['pt']['adset_spec']['targeting'] or ('name' in ads['pt'] and 'seed' in ads['pt']['name'].lower()):
                tmp_ads['type'] = 'seed'
            elif 'interests' in ads['pt']['adset_spec']['targeting'] or ('name' in ads['pt'] and 'interest' in ads['pt']['name'].lower()):
                tmp_ads['type'] = 'interests'
            elif 'behaviors' in ads['pt']['adset_spec']['targeting'] or ('name' in ads['pt'] and 'behavior' in ads['pt']['name'].lower()):
                tmp_ads['type'] = 'behaviors'
        tmp_ads['delt_name'] = None
        tmp_ads['delivery_mode'] = 'event'
        if 'name' in ads['pt']:
            tname = ads['pt']['name'].split(' ')
            if len(tname) > 0:
                tmp_ads['delt_name'] = tname[0].replace('_TEST', '')
            if 'value' in ads['pt']['name'].lower() or ('adset_spec' in ads['pt'] and 'optimization_goal' in ads['pt']['adset_spec']
                                                        and 'value' in ads['pt']['adset_spec']['optimization_goal'].lower()):
                tmp_ads['delivery_mode'] = 'value'
        if 'country' in ads['pt']:
            tmp_ads['country'] = ads['pt']['country']
        else:
            tmp_ads['country'] = None

    return tmp_ads


def read_combo_adids():
    if os.path.exists('combo_adids.txt'):
        combo_adids = pd.read_csv('combo_adids.txt')['ad_id']
        return list(combo_adids)
    else:
        return []


def save_combo_adids(combo_adids):
    pd.DataFrame({'ad_id':combo_adids}).to_csv('combo_adids.txt', index=False)


def modify_name(ads):
    cur_date = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    new_name = re.subn(r"(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2})", cur_date, ads['pt']['name'])[0]
    new_name = new_name.replace('[GA4]', '').strip()
    new_name = new_name.replace('EVENT', 'VALUE')+' [GA4]'
    ads['pt']['name'] = new_name
    ads['pt']['adset_spec']['name'] = new_name
    ads['pt']['adset_spec']['campaign_spec']['name'] = new_name
    # 1、去掉pt中的bid_amount
    if ads['pt'].get('adset_spec') and ads['pt']['adset_spec'].get('bid_amount'):
        del ads['pt']['adset_spec']['bid_amount']
    # 2、修改bid_strategy为LOWEST_COST_WITHOUT_CAP
    ads['pt']['adset_spec']['bid_strategy'] = 'LOWEST_COST_WITH_MIN_ROAS'

    # 3、修改optimization_goal为VALUE
    ads['pt']['adset_spec']['optimization_goal'] = 'VALUE'

    # 4、修改daily_budget为10000
    ads['pt']['adset_spec']['daily_budget'] = 10000

    # 5、设置roas
    ads['pt']['adset_spec']['bid_constraints'] = {'roas_average_floor': os.environ['roas_average_floor']}

    platform = 'Android'
    if 'ios' in ads['delt_name'].lower():
        platform = 'iOS'
    tpt = {
        'hash': str(uuid.uuid4()),
        'pt': ads['pt'],
        'algo': 'genetic',
        'status': 'available',
        'country': ads['country'],
        'platform': platform,
        'delt_name': ads['delt_name'].lower(),
        'created_at': datetime.datetime.now(tz).strftime('%Y-%m-%dT%H:%M:%S:%sZ')
    }
    return tpt