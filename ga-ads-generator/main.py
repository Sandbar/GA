from flask import Flask
from flask import request
import os
os.environ['config'] = 'config.ini'
import selector
import ga2_maker as gm
import sub_maker as sm
import json
app = Flask(__name__)


@app.route('/ga_baby')
def hello():
    try:
        delt_name = request.args.get('delivery_name')
        size = int(request.args.get('size'))
        response = app.response_class(
            response=json.dumps(selector.build_baby(delt_name, size)),
            status=200,
            mimetype='application/json'
        )
        return response
    except Exception as e:
        print(str(e))
        return ""


@app.route('/make_ga2')
def make_ga2():
    try:
        delt_name = request.args.get('delivery_name')
        size = int(request.args.get('size'))
        response = app.response_class(
            response=json.dumps(gm.build_baby(delt_name, size)),
            status=200,
            mimetype='application/json'
        )
        return response
    except Exception as e:
        print(str(e))
        return ""


@app.route('/sub_maker')
def sub_maker():
    try:
        delt_name = request.args.get('delivery_name')
        size = int(request.args.get('size'))
        response = app.response_class(
            response=json.dumps(sm.build_baby(delt_name, size)),
            status=200,
            mimetype='application/json'
        )
        return response
    except Exception as e:
        print(str(e))
        return ""


app.run('0.0.0.0', 9010)
