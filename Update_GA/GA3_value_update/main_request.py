from flask import Flask
import requests
import mongo_prod

app = Flask(__name__)


def ga_maker():
    res = requests.request(method='GET', url='http://0.0.0.0:9020/ga_maker_value?delt_name=bbt2_kr_s1_ios_tz_a00&size=1000')
    print(res.text)


def get_url():
    out = mongo_prod.test_ga_maker()
    lst = list()
    for ot in set(out):
        print('http://0.0.0.0:9020/ga_maker_value?delt_name=%s&size=100' % ot)


get_url()
