from flask import Flask, request
import json
from flask_cors import CORS
import pymongo
from timeir import *


client = pymongo.MongoClient()
farasahmDb = client['farasahm']
userCl = farasahmDb['user']


app = Flask(__name__)
CORS(app)

@app.route('/login',methods = ['POST', 'GET'])
def login():
    replay = ''
    msg = ''
    databack = ''
    data = request.get_json()
    user = userCl.find_one({'username':data['username']})
    if user==None or len(user)==0:
        replay = False
        msg = 'نام کاربری موجود نیست'
    else:
        if user['password'] == data['password']:
            replay = True
        else:
            replay = False
            msg = 'رمزعبور اشتباه است'
    dic = {'replay':replay, 'msg':msg, 'databack':databack}
    return json.dumps(dic)

@app.route('/account',methods = ['POST', 'GET'])
def account():
    replay = ''
    msg = ''
    databack = ''
    data = request.get_json()
    user = userCl.find_one({'username':data['username']})
    if user==None or len(user)==0:
        replay = False
    else:
        replay = True
        portfoli = int(user['portfoli'])>=today()
        eft = int(user['eft'])>=today()
        stocks = int(user['stocks'])>=today()
        databack = {'portfoli':portfoli, 'eft':eft, 'stocks':stocks}

    dic = {'replay':replay, 'msg':msg, 'databack':databack}
    return json.dumps(dic)



if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)