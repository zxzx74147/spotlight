import numpy

from flask import Flask
from flask_script import Manager
from flask import request
import os
import json
from xsb.config import WORKSPACE
import datetime
os.chdir(WORKSPACE)


from xsb.demon import RecommandDemon
from xsb.error_code import *
app = Flask(__name__) # Needs defining at file global scope for thread-local sharing
manager = Manager(app)
demon = None
def setup_app(app):
    global demon
    demon = RecommandDemon()

setup_app(app)

class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.integer):
            return int(obj)
        elif isinstance(obj, numpy.floating):
            return float(obj)
        elif isinstance(obj, numpy.ndarray):
            return obj.tolist()
        else:
            return super(MyEncoder, self).default(obj)

@app.route('/rc/g/list', methods=['GET', 'POST'])
def recommend():
    uId = request.args.get('uId')
    offset = request.args.get('offset',0)
    limit = request.args.get('limit', 20)
    if isinstance(offset , str):
        offset = int(offset)
    if isinstance(limit , str):
        limit = int(limit)
    error = dict()
    error['errno'] = ERROR_OK
    error['errmsg'] = ''
    error['usermsg'] = ''

    ret = dict()
    ret['error'] = error
    if not uId:
        error['errno'] = ERROR_PARAM
        error['usermsg'] = 'uid is empty！'
        return json.dumps(ret)
    print("recommend start" + str(datetime.datetime.now()))
    result = demon.rank(uId,offset=offset,limit=limit)
    print("recommend end" + str(datetime.datetime.now()))
    if result:
        ret['rank'] = result
    else:
        error['errno'] = ERROR_UID_NOT_FOUNT
        error['usermsg'] = 'uid not recorded！'
    return json.dumps(ret,cls=MyEncoder)

@app.route('/rc/refresh', methods=['GET', 'POST'])
def rquest_refresh():
    error = dict()
    error['errno'] = ERROR_OK
    error['errmsg'] = ''
    error['usermsg'] = ''

    if not demon.refresh():
        error['errno'] = ERROR_UNKONWN
    ret = dict()
    ret['error'] = error

    return json.dumps(ret)


if __name__ == '__main__':
    app.run(host='0.0.0.0',port=5000)