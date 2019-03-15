
import pymysql
import pandas as pd
import os

mysql_db_host = os.environ['mysql_db_host']
mysql_db_port = os.environ['mysql_db_port']
mysql_db_user = os.environ['mysql_db_user']
mysql_db_pwd = os.environ['mysql_db_pwd']
mysql_db_name = os.environ['mysql_db_name']
mysql_interests_name = 'dw_dim_interest'

conn = pymysql.connect(host=mysql_db_host, user=mysql_db_user, password=mysql_db_pwd, db=mysql_db_name,
                       port=mysql_db_port)

def select_weigth():
    df_weight = pd.read_sql('select id,name,weight from dw_dim_interest', conn)
    dct_name = dict()
    dct_weight = dict()
    for index in range(len(df_weight)):
        row = df_weight.iloc[index]
        dct_name[row['id']] = row['name']
        dct_weight[row['id']] = row['weight']
    return dct_name, dct_weight


def select_geo_weigth(country=None):
    sql = 'select latitude,longitude,radius,distance_unit,weight from' \
          ' dw_dim_coordinate where country=\'%s\'' % country
    df_weight = pd.read_sql(sql, conn)
    dct_name = dict()
    dct_weight = dict()
    for index in range(len(df_weight)):
        row = df_weight.iloc[index]
        gid = str(row['latitude']) + '_' + str(row['longitude']) + '_' + str(row['radius'])
        dct_name[gid] = row['distance_unit']
        dct_weight[gid] = row['weight']
    return dct_name, dct_weight


def select_url():
    sql = 'select distinct a.videoId,a.urlThumbnail,b.message1,b.message2,b.message3,b.message4,b.weight  from ' \
          'dw_dim_creative_media a join(select * from dw_dim_creative_text)b on a.videoId=b.videoId'
    urls_lst = list()
    urls = pd.read_sql(sql, conn)
    for index in range(len(urls)):
        row = urls.iloc[index]
        urls_lst.append({'videoId': row['videoId'],
                         'urlThumbnail': row['urlThumbnail'],
                         'message1': row['message1'],
                         'message2': row['message2'],
                         'message3': row['message3'],
                         'message4': row['message4'],
                         'weight': row['weight']
                         })
    return urls_lst


if __name__ == '__main__':
    # all_name, all_weight = select_geo_weigth('TW')
    # for aw in all_name:
    #     print(aw)
    print(select_url())
