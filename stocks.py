import time
import json
import pandas as pd
import pymongo
from general import *
import numpy as np
from numpy import mean
import collections 
from flask  import  send_file
import zipfile
from io import StringIO

client = pymongo.MongoClient()
farasahm_db = client['farasahm']

client = pymongo.MongoClient()

def is_register_file(df):
    standard_culomns = ['Account','Fullname','Ispl','Isno','Father','Type','NationalId','Birthday','Serial','Firstname','Lastname']
    cheack_culomns = (df.columns == standard_culomns)
    cheack_culomns = [x*1 for x in cheack_culomns]
    cheack_culomns = mean(cheack_culomns)==1
    return cheack_culomns

def is_trade_file(df):
    standard_culomns = ['Symbol','Date','Time','Volume','Price','Buy_brkr','Sel_brkr','Ticket_no','Cancel','B_account','S_account']
    return collections.Counter(list(df.columns)) == collections.Counter(standard_culomns)

def CodeToNameFast(listcode, symbol):
    symbol_db = client[f'{symbol}_db']
    register_collection = pd.DataFrame(symbol_db['register'].find({},{'_id':0, 'Account':1, 'Fullname':1}))
    register_collection = register_collection.set_index('Account')
    listname = []
    for code in listcode:
        if code in register_collection.index:
            fullname = register_collection['Fullname'][code]
            listname.append(fullname)
        else:
            listname.append(code)
    return listname

def CodeToName(code, symbol):
    symbol_db = client[f'{symbol}_db']
    register_collection = symbol_db['register']
    name = pd.DataFrame(register_collection.find({'Account':code}))
    if len(name)>0:
        name = name['Fullname'][name.index.max()]
    else:
        name = code
    name = name.replace('(سهامی‌خاص)','')
    return name

def getSymbolOfUsername(userName):
    symbol = farasahm_db['user'].find_one({'username':userName})
    if len(symbol)>0:
        return symbol['stocksName']
    else:
        return False

def getSymbolTseOfUsername(userName):
    symbol = farasahm_db['user'].find_one({'username':userName})
    if len(symbol)>0:
        return symbol['stocksSymbol']
    else:
        return False


def updateFile(symbol, daily, registerdaily):
    symbol_db = client[f'{symbol}_db']
    trade_collection = symbol_db['trade']
    register_collection = symbol_db['register']
    balance_collection = symbol_db['balance']
    change_collection = symbol_db['change']
    archive = zipfile.ZipFile(daily, 'r')
    listZip = archive.namelist()
    
    if len(listZip)!=3:
        return json.dumps({'res':False,'msg':'فایل روزانه صحیح نیست'})
    for i in listZip:
        if 'trades' in i:
            dfTrade = archive.read(i)
            if len(dfTrade)>0:
                dfTrade = str(dfTrade,'utf-16')
                dfTrade = StringIO(dfTrade) 
                dfTrade = pd.read_csv(dfTrade, sep='\t')
                if (is_trade_file(dfTrade)==False) or (symbol != dfTrade['Symbol'][dfTrade.index.min()]) or (dfTrade['Date'].max() != dfTrade['Date'].min()):
                    return json.dumps({'res':False,'msg':'محتویات فایل صحیح نیست'})
                cl_trade = pd.DataFrame(trade_collection.find({'Date':int(dfTrade['Date'].max())}))
                if len(cl_trade)>0:
                    trade_collection.delete_many({'Date':int(dfTrade['Date'].max())})
                trade_collection.insert_many(dfTrade.to_dict(orient='records'))

        elif 'registries' in i:
            dfRegister = archive.read(i)
            if len(dfRegister)>0:
                dfRegister = str(dfRegister,'utf-16')
                dfRegister = StringIO(dfRegister) 
                dfRegister = pd.read_csv(dfRegister, sep='\t')
                if is_register_file(dfRegister)==False:
                    return json.dumps({'res':False,'msg':'محتویات فایل صحیح نیست'})
                cl_register = pd.DataFrame(register_collection.find())
                if len(cl_register)>0:
                    cl_register = cl_register.drop(columns='_id')
                dfRegister = dfRegister.append(cl_register)
                dfRegister = dfRegister.drop_duplicates(subset=['Account'])
                register_collection.drop()
                register_collection.insert_many(dfRegister.to_dict(orient='records'))

        elif 'changes' in i:
            dfChange = archive.read(i)
            if len(dfChange)>0:
                dfChange = str(dfChange,'utf-16')
                dfChange = StringIO(dfChange)
                dfChange = pd.read_csv(dfChange, sep='\t')
                if len(dfChange)>0:
                    standard_culomns = ['Symbol','Date','Account','Onh_volume','Byn_volume','Letter_Dat','Letter_No']
                    if (collections.Counter(list(dfChange.columns)) == collections.Counter(standard_culomns) == False) or (symbol != dfChange['Symbol'][dfChange.index.min()]):
                        return json.dumps({'res':False,'msg':'محتویات فایل صحیح نیست'})
                    cl_Change = pd.DataFrame(change_collection.find({'Date':int(dfChange['Date'].max())}))
                    if len(cl_Change)>0:
                        change_collection.delete_many({'Date':int(dfChange['Date'].max())})
                    change_collection.insert_many(dfChange.to_dict(orient='records'))
        else:
            return json.dumps({'res':False,'msg':'محتویات فایل صحیح نیست'})

    if registerdaily != False:
        archive = zipfile.ZipFile(registerdaily, 'r')
        listZip = archive.namelist()
        if len(listZip)!=1:
            return json.dumps({'res':False,'msg':'فایل روزانه صحیح نیست'})
        dfBalance = archive.read(listZip[0]) 
        if len(dfBalance)<0:
            return json.dumps({'res':False,'msg':'محتویات فایل صحیح نیست'})
        dfBalance = str(dfBalance,'utf-16')
        dfBalance = StringIO(dfBalance) 
        dfBalance = pd.read_csv(dfBalance, sep='\t')
        standard_culomns = ['Lastname','Firstname','Account','Active','Birthday','CIRPHO','Date','Father','Isno','Ispl','NationalId','Saham','Sepordeh','Symbol','Type']
        if (collections.Counter(list(dfBalance.columns)) == collections.Counter(standard_culomns) == False):
            return json.dumps({'res':False,'msg':'محتویات فایل صحیح نیست'})
        cl_Balance = pd.DataFrame(balance_collection.find({'Date':int(dfBalance['Date'].max())}))
        if len(cl_Balance):
            balance_collection.delete_many({'Date':int(dfBalance['Date'].max())})
        balance_collection.insert_many(dfBalance.to_dict(orient='records'))
    else:
        if len(dfTrade)>0:
            cl_Balance = pd.DataFrame(balance_collection.find())
            if '_id' in cl_Balance.columns: cl_Balance.drop(columns='_id')
            byg = dfTrade.groupby(by=['B_account']).sum()
            slg = dfTrade.groupby(by=['S_account']).sum()
            grp = byg.join(slg, how='outer', lsuffix='_B', rsuffix='_S')
            grp = grp.replace(np.nan,0)
            grp['Balance'] = grp['Volume_B']-grp['Volume_S']
            grp = grp[['Balance']]
            for i in grp.index:
                AccB = pd.DataFrame(balance_collection.find({'Account':i}))

                if(len(AccB))>0:
                    AccB = AccB[AccB['Date']<dfTrade['Date'].max()]
                    if(len(AccB))>0:
                        AccB = AccB[AccB['Date'] == AccB['Date'].max()]
                        AccB = AccB.drop(columns='_id')
                        AccB['Saham'] = AccB['Saham'] + grp['Balance'][i]
                        AccB['Sepordeh'] = AccB['Sepordeh'] + grp['Balance'][i]
                        balance_collection.insert_one(AccB.to_dict(orient='records')[0])
                    else:
                        AccR = dfRegister[dfRegister['Account']==i]
                        dic = {'Lastname':AccR['Lastname'][AccR.index.max()],
                                'Firstname':AccR['Firstname'][AccR.index.max()] ,
                                'Account':AccR['Account'][AccR.index.max()] ,
                                'Active':np.nan ,
                                'Birthday':AccR['Birthday'][AccR.index.max()] ,
                                'CIRPHO':np.nan  ,
                                'Date': int(dfTrade['Date'].max()) ,
                                'Father':AccR['Father'][AccR.index.max()] ,
                                'Isno':AccR['Isno'][AccR.index.max()] ,
                                'Ispl':AccR['Ispl'][AccR.index.max()] ,
                                'NationalId':int(AccR['NationalId'][AccR.index.max()]) ,
                                'Saham':grp['Balance'][i] ,
                                'Sepordeh':grp['Balance'][i] ,
                                'Symbol':dfTrade['Symbol'][dfTrade.index.min()] ,
                                'Type':str(AccR['Type'][AccR.index.max()])
                        }
                        balance_collection.insert_one(dic)
                else:
                    AccR = dfRegister[dfRegister['Account']==i]
                    dic = {'Lastname':AccR['Lastname'][AccR.index.max()],
                            'Firstname':AccR['Firstname'][AccR.index.max()] ,
                            'Account':AccR['Account'][AccR.index.max()] ,
                            'Active':np.nan ,
                            'Birthday':AccR['Birthday'][AccR.index.max()] ,
                            'CIRPHO':np.nan  ,
                            'Date':int(dfTrade['Date'].max()) ,
                            'Father':AccR['Father'][AccR.index.max()] ,
                            'Isno':AccR['Isno'][AccR.index.max()] ,
                            'Ispl':AccR['Ispl'][AccR.index.max()] ,
                            'NationalId':int(AccR['NationalId'][AccR.index.max()]) ,
                            'Saham':grp['Balance'][i] ,
                            'Sepordeh':grp['Balance'][i] ,
                            'Symbol':dfTrade['Symbol'][dfTrade.index.min()] ,
                            'Type':str(AccR['Type'][AccR.index.max()])
                    }

                    balance_collection.insert_one(dic)


        
    return json.dumps({'res':True,'msg':'اطلاعات با موفقیت ثبت شد'})



def unavailable(username):
    symbol = getSymbolOfUsername(username)
    symbol_db = client[f'{symbol}_db']
    allUpload = [x['Date'] for x in symbol_db['trade'].find({},{'Date':1,'_id':0})]
    allUpload = list(set(allUpload))
    allUpload = [int(x) for x in allUpload]
    alltrade = requests.get(url=f'https://sourcearena.ir/api/?token=6e437430f8f55f9ba41f7a2cfea64d90&name={getSymbolTseOfUsername(username)}&days=1500').json()
    alltrade = [int(x['date'].replace('/','')) for x in alltrade]
    listDate = [x for x in alltrade if x not in allUpload]
    listDate.sort(reverse=True)
    listDate = [str(x)[0:4]+'/'+str(x)[4:6]+'/'+str(x)[6:8] for x in listDate]
    return({'listDate':listDate,'count':len(listDate)})

def tradersDatat(username, fromDate, toDate, side):
    symbol = getSymbolOfUsername(username)
    symbol_db = client[f'{symbol}_db']
    trade_collection = symbol_db['trade']
    if(fromDate==False):
        fromDate = trade_collection.find_one(sort=[("Date", -1)])['Date']
    if(toDate==False):
        toDate = trade_collection.find_one(sort=[("Date", -1)])['Date']
    dftrade = pd.DataFrame(trade_collection.find({ 'Date' : { '$gte' :  min(toDate,fromDate), '$lte' : max(toDate,fromDate)}}))
    dfbalance = pd.DataFrame(symbol_db['balance'].find({'Date':max(toDate,fromDate)}))
    if len(dftrade)<=0:
        return json.dumps({'replay':False, 'msg':'معاملات یافت نشد'})

    dftrade['Value'] = dftrade['Volume'] * dftrade['Price']
    dfBuy = dftrade.groupby(by=['B_account']).sum()[['Volume','Value']]
    dfSel = dftrade.groupby(by=['S_account']).sum()[['Volume','Value']]
    dfBuy['price'] = round(dfBuy['Value']/dfBuy['Volume'],0)
    dfSel['price'] = round(dfSel['Value']/dfSel['Volume'],0)
    df = dfBuy.join(dfSel,rsuffix='_buy',lsuffix='_sel',how='outer')
    dateList = list(set(dftrade['Date']))
    dateList = [str(x)[0:4]+'/'+str(x)[4:6]+'/'+str(x)[6:8] for x in dateList]
    finallPrice = []
    for d in dateList:
        url = f'https://sourcearena.ir/api/?token=6e437430f8f55f9ba41f7a2cfea64d90&name={getSymbolTseOfUsername(username)}&time={d}'
        d_ = requests.get(url).json()
        d_ = int(d_['final_price']) - int(d_['final_price_change'])
        finallPrice.append(d_)
    finallPrice = sum(finallPrice) / len(finallPrice)
    df['rate_sel'] = round(((df['price_sel']/finallPrice)-1)*100,2)
    df['rate_buy'] = round(((df['price_buy']/finallPrice)-1)*100,2)
    df = df.fillna(0)
    df['code'] = df.index
    df.index = [CodeToName(x,symbol) for x in df.index]
    print(df)


def tradersData(username, fromDate, toDate, side):
    tradersDatat(username, fromDate, toDate, side)
    symbol = getSymbolOfUsername(username)
    symbol_db = client[f'{symbol}_db']
    trade_collection = symbol_db['trade']
    if(fromDate==False):
        fromDate = trade_collection.find_one(sort=[("Date", -1)])['Date']
    if(toDate==False):
        toDate = trade_collection.find_one(sort=[("Date", -1)])['Date']
    if side=='buy':
        side = 'B_account'
        unSide = 'S_account'
    else:
        side = 'S_account'
        unSide = 'B_account'
    dftrade = pd.DataFrame(trade_collection.find({ 'Date' : { '$gte' :  min(toDate,fromDate), '$lte' : max(toDate,fromDate)}}))
    dfbalance = pd.DataFrame(symbol_db['balance'].find({'Date':max(toDate,fromDate)}))
    if len(dftrade)<=0:
        return json.dumps({'replay':False, 'msg':'معاملات یافت نشد'})
    else:
        dftrade['Value'] = dftrade['Volume'] * dftrade['Price']
        dfside = dftrade.groupby(by=[side]).sum()
        dfside = dfside[['Volume','Value']]
        dfside['PriceSide'] = dfside['Value']/dfside['Volume']
        dfside['code'] = dfside.index
        dfside.index = [CodeToName(x,symbol) for x in dfside.index]
        dfside = dfside.reset_index()
        dfside = dfside.reset_index()
        dfside.columns = ['id','name','volume','value','PriceSide','code']
        dffinall = pd.DataFrame()
        dffinall['value'] = dfside['value']
        dffinall['PriceSide'] = dfside['PriceSide']
        dffinall['volume'] = dfside['volume']
        dffinall['name'] = dfside['name']
        dffinall['id'] = dfside['id']
        dffinall['code'] = dfside['code']
        dffinall['w'] = (dffinall['volume']/dffinall['volume'].max())+0.1
        dffinall['PriceSide'] = [round(x) for x in dffinall['PriceSide']]
        dffinall['balance'] = '-'
        for i in dffinall.index:
            balance = dfbalance[dfbalance['Account']==dffinall['code'][i]]
            balance = balance['Saham'][balance.index.max()]
            dffinall['balance'].iloc[i] = balance
        dffinall = dffinall.sort_values(by='volume',ascending=False)
        dffinall = dffinall.to_dict('records')
        return json.dumps({'replay':True, 'data':dffinall})

def infocode(username, code):
    symbol = getSymbolOfUsername(username)
    symbol_db = client[f'{symbol}_db']
    cr = symbol_db['register'].find_one({'Account':code})
    broker = pd.DataFrame(symbol_db['trade'].find({'B_account':code}))
    if len(broker)>0:
        brokercode = list(set(list(broker['Buy_brkr'])))
    else:
        brokercode =[]
    broker = pd.DataFrame(symbol_db['trade'].find({'S_account':code}))
    if len(broker)>0:
        brokercode = brokercode + list(set(list(broker['Sel_brkr'])))
    try:info = str(cr['Fullname']).replace('،',' ')  + ' , با کد ملی ' + str(cr['NationalId']) + ' , صادره از ' + str(cr['Ispl']) + ' , متولد ' + str(int(cr['Birthday'])) + '\n' + 'ایستگاه های معاملاتی:' + '\n'
    except:info = str(cr['Fullname']).replace('،',' ')  + ' , با کد ملی ' + str(cr['NationalId']) + ' , صادره از ' + str(cr['Ispl']) + '\n' + 'ایستگاه های معاملاتی:' + '\n'
    brokerName = list(set([farasahm_db['broker'].find_one({'TBKEY':' '+(x.replace(' ',''))})['TBNAME'] for x in brokercode]))
    for i in brokerName:
        info = info + i + ','
    return json.dumps({'replay':True, 'msg':info})


def historicode(username, code):
    symbol = getSymbolOfUsername(username)
    symbol_db = client[f'{symbol}_db']
    alldatetrade = list(set(pd.DataFrame(symbol_db['trade'].find())['Date']))

    dfBalance = pd.DataFrame(symbol_db['balance'].find({'Account':code}))[['Date','Saham']]
    alldatetrade = list(filter(lambda x : x >= dfBalance['Date'].min() , alldatetrade))
    alldatetrade = list(filter(lambda x : x <= dfBalance['Date'].max() , alldatetrade))

    for i in alldatetrade:
        if i not in list(dfBalance['Date']):
            dfBalance = dfBalance.append({'Date':i,'Saham':np.nan}, ignore_index=True)
    dfBalance = dfBalance.sort_values(by='Date').reset_index().drop(columns=['index'])
    dfBalance = dfBalance.fillna(method='ffill')
    dfBalance = dfBalance.where(dfBalance>0, 0)
    dfBalance = dfBalance.to_dict(orient='records')
    return json.dumps({'replay':True,'data':dfBalance})

def newbie(username, fromDate, toDate):
    symbol = getSymbolOfUsername(username)
    symbol_db = client[f'{symbol}_db']
    if(fromDate==False):
        fromDate = symbol_db['trade'].find_one(sort=[("Date", -1)])['Date']
    if(toDate==False):
        toDate = symbol_db['trade'].find_one(sort=[("Date", 1)])['Date']
    dftrade = pd.DataFrame(symbol_db['trade'].find({ 'Date' : { '$gte' :  min(toDate,fromDate), '$lte' : max(toDate,fromDate)}}))
    if len(dftrade)<=0:
        return json.dumps({'replay':False, 'msg':'معاملات یافت نشد'})
    else:
        alldate = list(set(dftrade['Date'].to_list()))
        dfnewtrader = pd.DataFrame(columns=['Date','newvol','newnum','allvol','allnum'])
        for i in alldate:
            dfTraderp = dftrade[dftrade['Date']<i]
            dfTraderl = dftrade[dftrade['Date']==i]
            allgrp = dfTraderl.groupby(by=['B_account']).sum()
            if len(dfTraderp)==0:
                dfnewtrader = dfnewtrader.append({'Date':i, 'newvol':0, 'newnum':0, 'allvol':allgrp['Volume'].sum(), 'allnum':len(allgrp['Volume'])}, ignore_index=True)
            else:
                alloldcode = set(dfTraderp['B_account'])
                dfTraderl['new'] = dfTraderl['B_account'].map( lambda x: 'old' if x in alloldcode else 'new')
                dfTraderl = dfTraderl[dfTraderl['new']!='old']
                newnew = dfTraderl.groupby(by=['B_account']).sum()
                newvolume = newnew['Volume'].sum()
                newnum = len(newnew['Volume'])
                dfnewtrader = dfnewtrader.append({'Date':i, 'newvol':newvolume, 'newnum':newnum , 'allvol':allgrp['Volume'].sum(), 'allnum':len(allgrp['Volume'])}, ignore_index=True)
        dfnewtrader = dfnewtrader.sort_values(by=['Date'] ,ascending=False).reset_index().drop(columns=['index'])
        ToDayNewBie = dfnewtrader[dfnewtrader['Date']==dfnewtrader['Date'].max()]
        ToDayNewBie = ToDayNewBie.to_dict(orient='recodes')
        dfnewtrader['Date'] = [str(x)[0:4]+'/'+str(x)[4:6]+'/'+str(x)[6:8] for x in dfnewtrader['Date']]
        dfnewtrader['numper'] =(dfnewtrader['newnum']/dfnewtrader['allnum'])*10000
        dfnewtrader['numper'] = [int(x)/100 for x in dfnewtrader['numper']]
        dfnewtrader['volper'] = (dfnewtrader['newvol']/dfnewtrader['allvol'])*10000
        dfnewtrader['volper'] = [int(x)/100 for x in dfnewtrader['volper']]
        dfnewtrader = dfnewtrader.to_dict(orient='recodes')

        return json.dumps({'replay':True,'data':dfnewtrader, 'ToDayNewBie':ToDayNewBie})

def station(username, fromDate, toDate, side):
    symbol = getSymbolOfUsername(username)
    symbol_db = client[f'{symbol}_db']
    if(fromDate==False):
        fromDate = symbol_db['trade'].find_one(sort=[("Date", -1)])['Date']
    if(toDate==False):
        toDate = symbol_db['trade'].find_one(sort=[("Date", -1)])['Date']
    dfTrader = pd.DataFrame(symbol_db['trade'].find({ 'Date' : { '$gte' :min(int(toDate),int(fromDate))  , '$lte' :max(int(toDate),int(fromDate)) }}))
    if len(dfTrader)==0:
        return json.dumps({'replay':False, 'msg':'معاملات یافت نشد'})
    else:
        dfTrader['count'] =  1
        dfistgah = dfTrader.groupby(by=side).sum()
        dfistgah = dfistgah.sort_values(by='Volume',ascending=False)
        dfistgah = dfistgah[['Volume','count']].reset_index()
        dfistgah.columns = ['Istgah','Volume','count']
        dfistgah['w'] = (dfistgah['Volume'] / dfistgah['Volume'].max()) + 0.1
        for i in dfistgah.index:
            key = dfistgah['Istgah'][i]
            try:dfistgah['Istgah'][i] = farasahm_db['broker'].find_one({'TBKEY':' '+(key.replace(' ',''))})['TBNAME']
            except:dfistgah['Istgah'][i] = 'نامعلوم'
        dfistgah = dfistgah.to_dict(orient='records')
        return json.dumps({'replay':True,'data':dfistgah})

def dashbord(username):
    symbol = getSymbolOfUsername(username)
    symbol_db = client[f'{symbol}_db']
    lastUpdate = symbol_db['trade'].find_one(sort=[("Date", -1)])['Date']
    lastUpdate = pd.DataFrame(symbol_db['trade'].find({'Date':lastUpdate}))
    topBuy = lastUpdate.groupby(by=['B_account']).sum()[['Volume']]
    topSel = lastUpdate.groupby(by=['S_account']).sum()[['Volume']]
    topBuy = topBuy.sort_values(by=['Volume'],ascending=False).reset_index()
    topSel = topSel.sort_values(by=['Volume'],ascending=False).reset_index()
    topBuy = topBuy[topBuy.index<10]
    topSel = topSel[topSel.index<10]
    topBuy.index = [CodeToName(x, symbol) for x in topBuy['B_account']]
    topSel.index = [CodeToName(x, symbol) for x in topSel['S_account']]
    topBuy['name'] = topBuy.index
    topSel['name'] = topSel.index
    topBuy = topBuy.drop(columns=['B_account'])
    topSel = topSel.drop(columns=['S_account'])
    topBuy = topBuy.to_dict(orient='dict')
    topSel = topSel.to_dict(orient='dict')
    lastUpdate = str(lastUpdate['Date'].max())
    lastUpdateStr = str(lastUpdate)
    lastUpdateStr = lastUpdateStr[0:4]+'/'+lastUpdateStr[4:6]+'/'+lastUpdateStr[6:8]
    return json.dumps({'replay':True,'lastUpdate':lastUpdate,'lastUpdateStr':lastUpdateStr,'topBuy':topBuy,'topSel':topSel})

def tablo(username,date):
    dateStr = str(date)[0:4]+'/'+str(date)[4:6]+'/'+str(date)[6:8]
    req = f'https://sourcearena.ir/api/?token=6e437430f8f55f9ba41f7a2cfea64d90&name={getSymbolTseOfUsername(username)}&time={dateStr}'
    tabloRequest = requests.get(url=req).json()
    return json.dumps(tabloRequest)


def dataupdate(username):
    try:
        symbol = getSymbolOfUsername(username)
        symbol_db = client[f'{symbol}_db']
        dft = pd.DataFrame(symbol_db['trade'].find())
        lastUpdate = str(dft['Date'].max())
        lastUpdate = lastUpdate[0:4]+'/'+lastUpdate[4:6]+'/'+lastUpdate[6:8]
        cuntUpdate = int(len(set(dft['Date'])))
        cuntTrade = len(dft)
        cuntTrader = len(set(list(set(dft['B_account']))+list(set(dft['S_account']))))
        data = {'lastUpdate':lastUpdate, 'cuntUpdate':cuntUpdate,'cuntTrade':cuntTrade, 'cuntTrader':cuntTrader }
        return json.dumps({'replay':True, 'data':data})
    except: return json.dumps({'replay':False})


def sediment(username,period):
    symbol = getSymbolOfUsername(username)
    symbol_db = client[f'{symbol}_db']
    dftrade = pd.DataFrame(symbol_db['trade'].find())
    maxDate = dftrade['Date'].max()
    onPriod = onPeriodDate(maxDate,int(period))
    outTrader = list(set(dftrade[dftrade['Date']>onPriod]['S_account'])) # list(set(list(set(dftrade[dftrade['Date']>onPriod]['S_account']))+list(set(dftrade[dftrade['Date']>onPriod]['B_account']))))
    DfOnPriod = dftrade[dftrade['Date']<=onPriod]
    if len(DfOnPriod)<=0:
        return json.dumps({'replay':False, 'msg':'اطلاعاتی موجود نیست'})
    DfOnPriodBuy = DfOnPriod.groupby(by='B_account').sum()[['Volume']]
    DfOnPriodsel = DfOnPriod.groupby(by='S_account').sum()[['Volume']]
    dftrade = DfOnPriodBuy.join(DfOnPriodsel, lsuffix='_buy', rsuffix='_sel')
    dftrade['out'] = dftrade.index.isin(outTrader)
    dftrade = dftrade[dftrade['out']==False]
    if len(dftrade)<=0:
        return json.dumps({'replay':False, 'msg':'اطلاعاتی موجود نیست'})
    dftrade['balance'] = dftrade['Volume_buy'] - dftrade['Volume_sel']
    dftrade = dftrade[dftrade['balance']>0][['balance']].reset_index()
    if len(dftrade)<=0:
        return json.dumps({'replay':False, 'msg':'اطلاعاتی موجود نیست'})
    sumSediment =  dftrade['balance'].sum()
    countSediment = len(dftrade)
    dftrade['name'] = [CodeToName(x, symbol) for x in dftrade['B_account']]
    dftrade['w'] = (dftrade['balance']/dftrade['balance'].max())+0.1
    dftrade = dftrade.sort_values(by='w',ascending=False)
    dftrade = dftrade.to_dict(orient='records')
    return json.dumps({'replay':True,'countSediment':countSediment,'sumSediment':sumSediment, 'data':dftrade})

def traderlist(username):
    symbol = getSymbolOfUsername(username)
    symbol_db = client[f'{symbol}_db']
    dftrader = pd.DataFrame(symbol_db['register'].find())
    dftrader = dftrader[['Fullname','Account']]
    dftrader = dftrader.to_dict(orient='records')
    return json.dumps({'replay':True, 'data':dftrader})

def detailes(username, account, fromDate, toDate):
    symbol = getSymbolOfUsername(username)
    symbol_db = client[f'{symbol}_db']
    dftrader =pd.DataFrame()
    try:
        dftrader = pd.DataFrame(symbol_db['trade'].find({'S_account':account}))
        dftrader['Volume'] = dftrader['Volume']*-1
    except:pass
    try:
        dftrader = dftrader.append(pd.DataFrame(symbol_db['trade'].find({'B_account':account})))
    except:pass
    if fromDate!=False:
        dftrader = dftrader[dftrader['Date']>=int(fromDate)]
    if toDate!=False:
        dftrader = dftrader[dftrader['Date']<=int(toDate)]
    dftrader = dftrader[['Volume','Price','B_account','S_account','Date','Time']]

    dftrader = dftrader.sort_values(by=['Date','Time'], ascending=[True,True]).reset_index()
    avgprcb = dftrader[dftrader['B_account']==account]
    if len(avgprcb):
        avgprcb['value'] = avgprcb['Volume'] * avgprcb['Price']
        avgprcb = avgprcb['value'].sum() / avgprcb['Volume'].sum()
    else:
        avgprcb = 0

    avgprcs = dftrader[dftrader['S_account']==account]
    if len(avgprcs)>0:
        avgprcs['value'] = (avgprcs['Volume'] * avgprcs['Price']) 
        avgprcs = avgprcs['value'].sum() / avgprcs['Volume'].sum()
    else:
        avgprcs = 0
    dftrader['B_account'] = CodeToNameFast(list(dftrader['B_account']), symbol)
    dftrader['S_account'] = CodeToNameFast(list(dftrader['S_account']), symbol)
    dftrader['Date'] = [str(x)[0:4]+'/'+str(x)[4:6]+'/'+str(x)[6:8] for x in dftrader['Date']]
    balance = pd.DataFrame(symbol_db['balance'].find({'Account':account}))
    balance = balance[balance['Date']==balance['Date'].max()]
    balance = balance['Saham'][balance.index.max()]
    shortData = {'vol':int(balance), 'avgprcb':int(avgprcb), 'avgprcs': int(avgprcs)}
    dftrader = dftrader.to_dict(orient='records')
    print(dftrader)
    return json.dumps({'replay':True, 'data':dftrader, 'shortData':shortData})

'''
username='test1'
onDate = False

symbol = getSymbolOfUsername(username)
symbol_db = client[f'{symbol}_db']
trade_collection = symbol_db['trade']
if(onDate==False):
    fromDate = trade_collection.find_one(sort=[("Date", -1)])['Date']


dfBalance = pd.DataFrame(symbol_db['balance'].find({},{'_id':0,'Account':1,'Saham':1,'Date':1}))
dfBalance = dfBalance.pivot_table(index='Date', columns='Account')
dfBalance = dfBalance['Saham']
dfBalance = dfBalance.sort_index()
dfBalance = dfBalance.fillna(method='ffill')
miss = dfBalance.isnull().sum()
miss = miss>miss.mean()
miss = miss[miss==True]
dfBalance = dfBalance.drop(columns=miss.index)
dfBalance = dfBalance.fillna(0)
miss = dfBalance.std()
miss = miss<miss.mean()/5
miss = miss[miss==True]
dfBalance = dfBalance.drop(columns=miss.index)
dfBalance = dfBalance.corr(method='pearson')
f = []
for c in dfBalance.columns:
    dfBalance[c][c] = 0
    re = dfBalance[c]
    re = re.sort_values()
    re = re[3:]
    print(re)
'''