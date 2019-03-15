

from pymongo import MongoClient
import os
import common_genetic as common


def find_evaluation_before(sdate, edate):
    client = MongoClient(os.environ['mongo_url'], maxPoolSize=200)
    db = client.get_database(os.environ['db_name'])
    # colles_before = db.evaluation.find({'ad_create_at': {'$gte': str(sdate), '$lt': str(edate)}, 'insights.lifetime.pay': {'$gt': 0}}).batch_size(1)
    colles_before = db.evaluation.find({'ad_create_at': {'$gte': str(sdate), '$lt': str(edate)}, 'insights.lifetime.spend': {'$gte': 50}, 'insights.lifetime.pay': {'$gte': 1}}).batch_size(1)
    tmp_evas = list()
    for ct in colles_before:
        if float(ct['insights']['lifetime']['spend'])/float(ct['insights']['lifetime']['pay']) <= 50:
            tmp = common.load_modify_ads(ct)
            if tmp:
                tmp_evas.append(tmp)
    client.close()
    return tmp_evas


def find_evaluation_today(date):
    client = MongoClient(os.environ['mongo_url'], maxPoolSize=200)
    db = client.get_database(os.environ['db_name'])
    colles_today = db.evaluation.find({'ad_create_at': {'$gte': str(date)}, '$or': [{'insights.lifetime.install': {'$gt': 1}},
                                                                                    {'insights.lifetime.spend': {'$gte': 2}},
                                                                                    {'insights.lifetime.pay': {'$gt': 0}}],
                                       'pt': {'$ne': None}}).batch_size(1)
    tmp_evas = list()
    for ct in colles_today:
        tmp = common.load_modify_ads(ct)
        if tmp:
            tmp_evas.append(tmp)
    client.close()
    return tmp_evas


def insert_baits(adses):
    client = MongoClient(os.environ['mongo_url'], maxPoolSize=200)
    db = client.get_database(os.environ['db_name'])
    db.baits.insert_many(adses)
    client.close()


if __name__ == '__main__':
    ads = find_evaluation_before('2018-11-26', '2018-11-28')
    # ads = find_evaluation_today('2018-11-27')
    print(ads)
    print(len(ads))
