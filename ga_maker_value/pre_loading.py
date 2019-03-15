
import mongo_prod
import mysql_prod
import time
import os
import datetime
import pytz

tz = pytz.timezone('Asia/Shanghai')
def string_to_datetime(tdate):
    return datetime.datetime.strftime(tdate, "%Y-%m-%d")


def is_update_pre_loading():
    if os.path.exists('./logs/tdate.txt'):
        with open('./logs/tdate.txt', 'r') as f:
            date = f.readline()
            if date > str(string_to_datetime(datetime.datetime.now(tz))):
                return True
            else:
                return False
    return False


def pre_loading_mysql_geo():
    geo_location = mysql_prod.select_geo()
    countries = set(geo_location.country)
    geo = {'name': {}, 'weight': {}}
    for country in countries:
        dct_name = dict()
        dct_weight = dict()
        geo_df = geo_location[geo_location['country'] == country]
        for index in range(len(geo_df)):
            row = geo_df.iloc[index]
            gid = str(row['latitude']) + '_' + str(row['longitude']) + '_' + str(row['radius'])
            dct_name[gid] = row['distance_unit']
            dct_weight[gid] = row['weight']
        geo['name'][country] = dct_name
        geo['weight'][country] = dct_weight
    return geo


def pre_loading_mysql_interests():
    return mysql_prod.select_interests()


def pre_loading_mysql_url():
    return mysql_prod.select_url()


def pre_loading_mongo_ads():

    return mongo_prod.select_ads()


def save_date_to_txt():
    with open('./logs/tdate.txt', 'w') as f:
        f.write(str(string_to_datetime(datetime.datetime.now(tz))))


def pre_main():
    tmp_dict = dict()
    tmp_dict['media_url'] = pre_loading_mysql_url()
    tmp_dict['ads'] = pre_loading_mongo_ads()
    tmp_dict['geo_dict'] = pre_loading_mysql_geo()
    tmp_dict['interests_name'], tmp_dict['interests_weight'] = pre_loading_mysql_interests()
    # save_date_to_txt()
    return tmp_dict


if __name__ == '__main__':
    t = time.time()
    # tmp = pre_main()
    # print(tmp.keys())
    pre_loading_mysql_geo()
    print(time.time()-t)




