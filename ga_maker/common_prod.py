import datetime
import re
import pytz
from random import choice


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


def modify_pt(pt=None, delt_name=None, sign=None, creative_medias=None):
    cur_date = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    new_name = re.subn(r"(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2})", cur_date, pt['name'])[0]
    new_name = re.sub(r'\w* ', (delt_name+' ').upper(), new_name, count=1, flags=0)
    new_name = new_name.replace('[GA1]', '').replace('[GA2]', '').replace('[GA]', '').strip()
    new_name = new_name + ' ' + sign
    pt['name'] = new_name
    pt['adset_spec']['name'] = new_name
    pt['adset_spec']['campaign_spec']['name'] = new_name
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

    return pt
