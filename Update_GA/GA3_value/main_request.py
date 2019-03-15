from flask import Flask
import requests

app = Flask(__name__)


def ga_maker():
    res = requests.request(method='GET', url='http://0.0.0.0:9020/ga_maker_value?delt_name=bbt2_kr_s1_ios_tz_a00&size=17')
    print(res.text)


ga_maker()
