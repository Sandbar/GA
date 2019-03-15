import datetime
import re
import pytz
from random import choice
import uuid


tz = pytz.timezone('Asia/Shanghai')


def weighted(dic_obj):
    sum_value = sum(list(dic_obj.values()))
    for key in dic_obj:
        dic_obj[key] = dic_obj[key]/sum_value
    return dic_obj


def node_exist(pt, node_name):
    if pt['adset_spec']['targeting'].get(node_name):
        return True
    else:
        return False


def node_exist_geolocation(pt, node_name):
    if pt['adset_spec']['targeting'].get('geo_locations') and pt['adset_spec']['targeting']['geo_locations'].get(node_name):
        return True
    else:
        return False


# def modify_info(pt, delt_name, video_id, country, platform):
#     cur_date = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
#     new_name = re.subn(r"(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2})", cur_date, pt['name'])[0]
#     new_name = re.sub(r'\w* ', (delt_name+' ').upper(), new_name, count=1, flags=0)
#     new_name = new_name.replace('[GA2]', '').replace('[GA1]', '').replace('[GA]', '').replace('TAR_BID', 'VALUE').strip()
#     pt['name'] = new_name
#     pt['adset_spec']['name'] = new_name
#     pt['adset_spec']['campaign_spec']['name'] = new_name
#
#     # 添加国家和平台
#     pt['country'] = country
#     pt['platform'] = platform
#
#     # 1、去掉pt中的bid_amount
#     if pt.get('adset_spec') and pt['adset_spec'].get('bid_amount'):
#         del pt['adset_spec']['bid_amount']
#     # 2、修改bid_strategy为LOWEST_COST_WITHOUT_CAP
#     pt['adset_spec']['bid_strategy'] = 'LOWEST_COST_WITHOUT_CAP'
#
#     # 3、修改optimization_goal为VALUE
#     pt['adset_spec']['optimization_goal'] = 'VALUE'
#
#     # 4、修改daily_budget为10000
#     pt['adset_spec']['daily_budget'] = 10000
#
#     # 修改images_url
#     if pt.get('creative') and pt['creative'].get('object_story_spec') and \
#             pt['creative']['object_story_spec'].get('video_data') and \
#             pt['creative']['object_story_spec']['video_data'].get('image_hash'):
#         del pt['creative']['object_story_spec']['video_data']['image_hash']
#     pt['creative']['object_story_spec']['video_data']['image_url'] = choice(video_url[video_id]['image_url'])
#
#     pt['creative']['object_story_spec']['video_data']['message'] = choice(video_url[video_id]['message'])
#     pt['creative']['object_story_spec']['video_data']['video_id'] = video_id
#     return pt

def modify_infos(pt=None, delt_name=None, creative_medias=None, country=None, platform=None):
    cur_date = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    new_name = re.subn(r"(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2})", cur_date, pt['name'])[0]
    new_name = re.sub(r'\w* ', (delt_name+' ').upper(), new_name, count=1, flags=0)
    new_name = new_name.replace('[GA1]', '').replace('[GA2]', '').replace('[GA]', '').replace('TAR_BID', 'VALUE').strip()
    new_name = new_name + ' [GA4]'
    pt['name'] = new_name
    pt['adset_spec']['name'] = new_name
    pt['adset_spec']['campaign_spec']['name'] = new_name



    # 1、去掉pt中的bid_amount
    if pt.get('adset_spec') and pt['adset_spec'].get('bid_amount'):
        del pt['adset_spec']['bid_amount']
    # 2、修改bid_strategy为LOWEST_COST_WITHOUT_CAP
    pt['adset_spec']['bid_strategy'] = 'LOWEST_COST_WITHOUT_CAP'

    # 3、修改optimization_goal为VALUE
    pt['adset_spec']['optimization_goal'] = 'VALUE'

    # 4、修改daily_budget为10000
    pt['adset_spec']['daily_budget'] = 10000

    if pt.get('creative') and pt['creative'].get('object_story_spec') and \
            pt['creative']['object_story_spec'].get('video_data') and \
            pt['creative']['object_story_spec']['video_data'].get('image_hash'):
        del pt['creative']['object_story_spec']['video_data']['image_hash']

    creative_media = choice(creative_medias)
    message = creative_media['message1']
    if creative_media['message4']:
        message = creative_media['message4']
    elif creative_media['message3']:
        message = creative_media['message3']
    elif creative_media['message2']:
        message = creative_media['message2']

    pt['creative']['object_story_spec']['video_data']['image_url'] = creative_media['urlThumbnail']
    pt['creative']['object_story_spec']['video_data']['message'] = message
    pt['creative']['object_story_spec']['video_data']['video_id'] = creative_media['videoId']

    tpt = {'hash': str(uuid.uuid4()),
           'pt': pt,
           'algo': 'genetic',
           'status': 'available',
           'country': country,
           'platform': platform,
           'delt_name': delt_name,
           'created_at': datetime.datetime.now(tz).strftime('%Y-%m-%dT%H:%M:%S:%sZ')
           }

    return tpt
