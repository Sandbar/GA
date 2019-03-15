from flask import Flask
from flask import request
import ga_maker_pts as nga
import pre_loading as pl
import json
import copy
from collections import deque
import time

app = Flask(__name__)

tmp_dict = pl.pre_main()
Tasks_Queue = deque()
Flag_Sign = True
nga.logger.info('ready，go...')


@app.route('/ga_maker_value', methods=['GET', 'POST'])
def ga_maker():
    global Flag_Sign
    global tmp_dict
    if request.method == 'GET':
        # if pl.is_update_pre_loading():
        #     tmp_dict = pl.pre_main()
        try:
            delt_name = request.args.get('delt_name')
            size = int(request.args.get('size'))
            Tasks_Queue.append({'delt_name': delt_name, 'size': size})
            nga.logger.info('add delt_name:%s, size: %d' % (delt_name, size))
        except ValueError as ve:
            nga.logger.info('ValueError, %s' % (str(ve)))
            return str(ve)

        while Flag_Sign and len(Tasks_Queue):
            try:
                t = time.time()
                Flag_Sign = False
                dnsize = Tasks_Queue.popleft()
                if dnsize['size'] > 100:
                    dnsize['size'] = dnsize['size'] - 100
                    Tasks_Queue.append(copy.deepcopy(dnsize))
                    dnsize['size'] = 100
                response = app.response_class(
                    response=json.dumps(nga.create_pt(dnsize['delt_name'], dnsize['size'], copy.deepcopy(tmp_dict))),
                    status=200,
                    mimetype='application/json'
                )
                # print(dnsize['delt_name'], dnsize['size'], 'is over')
                Flag_Sign = True
                # print(time.time()-t)
                nga.logger.info('create time is: %f' % (time.time()-t))
            except Exception as e:
                nga.logger.info('Error, %s' % (str(e)))
                pass
        if Flag_Sign and len(Tasks_Queue) == 0:
            return "over"
        return 'received'
    return "None"


app.run('0.0.0.0', 9020)
