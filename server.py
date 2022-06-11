from flask import Flask, request
import json
from flask_cors import CORS
from matplotlib.pyplot import get
import pymongo
from timeir import *
import pandas as pd
from PortfolioAsset import *

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


@app.route('/portfoli/update',methods = ['POST', 'GET'])
def portfoli_update():
    username = request.form.get('username')
    return uploadTradFile(request.files['filetrade'],username)

@app.route('/portfoli/customerlist',methods = ['POST', 'GET'])
def portfoli_customerlist():
    data = request.get_json()
    username = data['username']
    return customerNames(username)

@app.route('/portfoli/customerreview',methods = ['POST', 'GET'])
def portfoli_customerreview():
    data = request.get_json()
    username = data['username']
    customer = data['customer']
    return customerreview(username, customer)

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)