

from flask import Flask
from flask import request
import standard_genetic
import log_maker
import os
import config

app = Flask(__name__)


@app.route('/ga_maker4', methods=['GET'])
def ga_maker4():
    try:
        if request.method == 'GET':
            config.init_config()  ### 配置文件，上传需要删除
            sg = standard_genetic.StandardGenetic()
            sg.main()
            print('OK')
            return "OK "
    except Exception as e:
        print(str(e))
        log_maker.logger.info(str(e))
        return "except"


app.run('0.0.0.0', os.environ['tm_port'])
