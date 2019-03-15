from flask import Flask
import requests

app = Flask(__name__)


def ga_maker():
    try:
        res = requests.request(method='GET', url='http://172.20.150.156:555/ga_maker?delt_name=bbt3_ae_android&size=13')
        print(res.text)
    except:
        res = requests.request(method='GET', url='http://0.0.0.0:9010/ga_maker?delt_name=bbt3_ae_android&size=13')
        print(res.text)


ga_maker()
