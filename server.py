from dataclasses import replace
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

@app.route('/portfoli/customerupdate',methods = ['POST', 'GET'])
def portfoli_customerupdate():
    data = request.get_json()
    username = data['username']
    return json.dumps(customerNames(username))

@app.route('/portfoli/customerreview',methods = ['POST', 'GET'])
def portfoli_customerreview():
    data = request.get_json()
    username = data['username']
    return json.dumps(customerreview(username))

@app.route('/portfoli/asset',methods = ['POST', 'GET'])
def portfoli_customerasset():
    data = request.get_json()
    username = data['username']
    customer = data['customer']
    df = customerasset(username, customer)
    df = df.reset_index()
    if len(df)>0:
        databack = df.to_dict(orient='records')
        return json.dumps({'replay':True, 'databack':databack, 'msg':''})
    else:
        return json.dumps({'replay':False, 'databack':'', 'msg':'دارایی برای نمایش موجود نیست'})

@app.route('/portfoli/profitability',methods = ['POST', 'GET'])
def portfoli_customerprofitability():
    data = request.get_json()
    username = data['username']
    customer = data['customer']
    dateselect = data['dateselect']
    problem = customerprofitability(username,customer,dateselect)
    print(problem)
    if problem[0]:
        return json.dumps({'replay':False, 'msg':'nobuy', 'databack':problem[1]})
    else:
        return json.dumps({'replay':True, 'msg':'', "databack":''})

@app.route('/portfoli/getallsymbol',methods = ['POST', 'GET'])
def portfoli_getallsymbol():
    databack = getAllSymboll()
    return json.dumps({'databack':databack})

@app.route('/portfoli/updateform',methods = ['POST', 'GET'])
def portfoli_updateform():
    data = request.get_json()
    updateform(data)

    return json.dumps({'ok':'ok'})




if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)