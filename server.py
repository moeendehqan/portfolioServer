import stocks
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
            databack = user['fullName']
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

@app.route('/stocks/update',methods = ['POST', 'GET'])
def stocks_update():
    user = request.form['user']
    Trade =  request.files['Trade']
    Register =  request.files['Register']
    symbol = stocks.getSymbolOfUsername(user)
    if symbol==False:
        return json.dumps({'repaly':False,'msg':'نماد یافت نشد'})
    else:
        return stocks.updateFile(symbol, Trade, Register)
@app.route('/stocks/dataupdate',methods = ['POST', 'GET'])
def stocks_dataupdate():
    data = request.get_json()
    return stocks.dataupdate(data['username'])


@app.route('/stocks/traders',methods = ['POST', 'GET'])
def stocks_traders():
    data = request.get_json()
    return stocks.tradersData(data['username'], int(data['fromDate']), int(data['toDate']), data['side'], data['sorting'])

@app.route('/stocks/infocode',methods = ['POST', 'GET'])
def stocks_infocode():
    data = request.get_json()
    return stocks.infocode(data['username'], data['code'])

@app.route('/stocks/historicode',methods = ['POST', 'GET'])
def stocks_historicode():
    data = request.get_json()
    return stocks.historicode(data['username'], data['code'])

@app.route('/stocks/newbie',methods = ['POST', 'GET'])
def stocks_newbie():
    data = request.get_json()
    return stocks.newbie(data['username'], int(data['fromDate']), int(data['toDate']))

@app.route('/stocks/station',methods = ['POST', 'GET'])
def stocks_station():
    data = request.get_json()
    return stocks.station(data['username'], int(data['fromDate']), int(data['toDate']), data['side'])

@app.route('/stocks/dashbord',methods = ['POST', 'GET'])
def stocks_dashbord():
    data = request.get_json()
    return stocks.dashbord(data['username'])

@app.route('/stocks/sediment',methods = ['POST', 'GET'])
def stocks_sediment():
    data = request.get_json()
    return stocks.sediment(data['username'], data['period'])

@app.route('/stocks/traderlist',methods = ['POST', 'GET'])
def stocks_traderlist():
    data = request.get_json()
    return stocks.traderlist(data['username'])

@app.route('/stocks/detailes',methods = ['POST', 'GET'])
def stocks_detailes():
    data = request.get_json()
    return stocks.detailes(data['username'], data['account'], data['fromDate'], data['toDate'])








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