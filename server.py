import stocks
import etf
from flask import Flask, request
import json
from flask_cors import CORS

import pymongo
from timeir import *
import pandas as pd
import PortfolioAsset

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

@app.route('/fulluser',methods = ['POST', 'GET'])
def fulluser():
    data = request.get_json()
    user = list(userCl.find({'username':data['username']},{'_id':0}))
    return json.dumps(user)


#-------------------------Stocks------------------------------
@app.route('/stocks/update',methods = ['POST', 'GET'])
def stocks_update():
    user = request.form['user']
    daily =  request.files['daily']
    try:registerdaily =  request.files['registerdaily']
    except:registerdaily = False
    symbol = stocks.getSymbolOfUsername(user)
    if symbol==False:
        return json.dumps({'repaly':False,'msg':'نماد یافت نشد'})
    else:
        return stocks.updateFile(symbol, daily, registerdaily)

@app.route('/stocks/dataupdate',methods = ['POST', 'GET'])
def stocks_dataupdate():
    data = request.get_json()
    return stocks.dataupdate(data['username'])

@app.route('/stocks/unavailable',methods = ['POST', 'GET'])
def stocks_unavailable():
    data = request.get_json()
    return stocks.unavailable(data['username'])

@app.route('/stocks/traders',methods = ['POST', 'GET'])
def stocks_traders():
    data = request.get_json()
    return stocks.tradersData(data['username'], int(data['fromDate']), int(data['toDate']))

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

@app.route('/stocks/tablo',methods = ['POST', 'GET'])
def stocks_tablo():
    data = request.get_json()
    return stocks.tablo(data['username'],data['date'])

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

@app.route('/stocks/detailesgetcsv',methods = ['POST', 'GET'])
def stocks_detailesGetCsv():
    data = request.get_json()
    return stocks.detailesGetCsv(data['username'], data['account'], data['fromDate'], data['toDate'])

#---------------------------------etf--------------------------------------------


@app.route('/etf/dashboard',methods = ['POST', 'GET'])
def etf_dashboard():
    data = request.get_json()
    return etf.etf_dashboard(data['username'])

@app.route('/etf/nav',methods = ['POST', 'GET'])
def etf_nav():
    data = request.get_json()
    return etf.etf_nav(data['username'],data['fromDate'],data['toDate'])

@app.route('/etf/volume',methods = ['POST', 'GET'])
def etf_volume():
    data = request.get_json()
    return etf.etf_volume(data['username'],data['fromDate'],data['toDate'])


@app.route('/etf/return',methods = ['POST', 'GET'])
def etf_return():
    data = request.get_json()
    return etf.etf_return(data['username'],data['onDate'],data['target'], data['periodList'])

@app.route('/etf/reserve',methods = ['POST', 'GET'])
def etf_reserve():
    data = request.get_json()
    return etf.etf_reserve(data['username'],data['fromDate'],data['toDate'],data['etfSelect'])


@app.route('/etf/etflist',methods = ['POST', 'GET'])
def etf_etflist():
    data = request.get_json()
    return etf.etf_etflist(data['username'])


@app.route('/etf/allreturn',methods = ['POST', 'GET'])
def etf_allreturn():
    data = request.get_json()
    return etf.etf_allreturn(data['username'],data['onDate'],data['etfSelectm'])




#---------------------------------portfolio--------------------------------------------

@app.route('/portfolio/updatetbs',methods = ['POST', 'GET'])
def portfoli_update():
    username = request.form.get('username')
    return PortfolioAsset.uploadTradFile(request.files['TBS'],username)

@app.route('/portfolio/investorlist',methods = ['POST', 'GET'])
def portfoli_investorlist():
    data = request.get_json()
    return PortfolioAsset.investorlist(data['username'])

@app.route('/portfolio/symbolelist',methods = ['POST', 'GET'])
def portfoli_symbolelist():
    return PortfolioAsset.symbolelist()

@app.route('/portfolio/updatemanual',methods = ['POST', 'GET'])
def portfoli_updatemanual():
    data = request.get_json()
    return PortfolioAsset.updatemanual(data['username'],data['date'],data['invester'],data['side'],data['symbol'],data['price'],data['amunt'])
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)