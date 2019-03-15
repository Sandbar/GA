from flask import Flask
from flask import request
import ga4_maker as nga
import json

app = Flask(__name__)


@app.route('/ga_maker', methods=['GET', 'POST'])
def ga_maker():
    try:
        if request.method == 'GET':
            delt_name = request.args.get('delt_name')
            size = int(request.args.get('size'))
            response = app.response_class(
                response=json.dumps(nga.create_pt(delt_name, size)),
                status=200,
                mimetype='application/json'
            )
            return response
        return ""
    except Exception as e:
        print(str(e))
        return ""


app.run('0.0.0.0', 9010)