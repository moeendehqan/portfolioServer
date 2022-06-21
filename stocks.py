import json
import pandas as pd
import pymongo
from general import *
import numpy as np
from numpy import mean, sort
import collections 
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

def updateFile(symbol, Trade, Register):
    symbol_db = client[f'{symbol}_db']
    trade_collection = symbol_db['trade']
    register_collection = symbol_db['register']
    balance_collection = symbol_db['balance']
    TradeType = Trade.filename.split('.')[-1]
    RegisterType = Register.filename.split('.')[-1]
    if TradeType == 'xlsx':
        dfTrade = pd.read_excel(Trade)
    elif TradeType == 'csv':
        dfTrade = pd.read_csv(Trade, encoding='utf-16', sep='\t')
    else:
        return json.dumps({'res':False,'msg':'نوع فایل معاملات مجاز نیست'})

    if RegisterType == 'xlsx':
        dfRegister = pd.read_excel(Register)
    elif RegisterType == 'csv':
        dfRegister = pd.read_csv(Register, encoding='utf-16', sep='\t')
    else:
        return json.dumps({'res':False,'msg':'نوع فایل رجیستر مجاز نیست'})
    if is_trade_file(dfTrade)==False:
        return json.dumps({'res':False,'msg':'محتویات فایل معاملات صحیح نیست'})
    if symbol != dfTrade['Symbol'][1]:
        return json.dumps({'res':False,'msg':'محتویات فایل معاملات صحیح نیست'})
    if dfTrade['Date'].max() != dfTrade['Date'].min():
        return json.dumps({'res':False,'msg':'محتویات فایل معاملات صحیح نیست'})
    if is_register_file(dfRegister)==False:
        return json.dumps({'res':False,'msg':'محتویات فایل رجیستر صحیح نیست'})
    cl_trade = pd.DataFrame(trade_collection.find({'Date':int(dfTrade['Date'].max())}))
    if len(cl_trade)>0:
        trade_collection.delete_many({'Date':int(dfTrade['Date'].max())})
    trade_collection.insert_many(dfTrade.to_dict(orient='records'))
    cl_register = pd.DataFrame(register_collection.find())
    if len(cl_register)>0:
        cl_register = cl_register.drop(columns='_id')
    dfRegister = dfRegister.append(cl_register)
    dfRegister = dfRegister.drop_duplicates(subset=['Account'])
    register_collection.drop()
    register_collection.insert_many(dfRegister.to_dict(orient='records'))
    cl_balance = pd.DataFrame(balance_collection.find({'Date':int(dfTrade['Date'].max())}))
    if len(cl_balance)>0:
        balance_collection.delete_many({'Date':int(dfTrade['date'].max())})
    blnc_buy =  dfTrade.groupby('B_account').sum()
    blnc_sel =  dfTrade.groupby('S_account').sum()
    blnc_buy = blnc_buy.reset_index()[['B_account','Volume']]
    blnc_buy = blnc_buy.set_index('B_account')
    blnc_sel = blnc_sel.reset_index()[['S_account','Volume']]
    blnc_sel = blnc_sel.set_index('S_account')
    dfBalance = blnc_buy.join(blnc_sel, lsuffix='_B',rsuffix= '_S', how='outer')
    dfBalance = dfBalance.reset_index()
    dfBalance['date'] = dfTrade['Date'].max()
    dfBalance = dfBalance.replace(np.nan, 0)
    dfBalance['Balance'] = dfBalance['Volume_B'] - dfBalance['Volume_S']
    balance_collection.insert_many(dfBalance.to_dict(orient='records'))
    return json.dumps({'res':True,'msg':'اطلاعات با موفقیت ثبت شد'})

def tradersData(username, fromDate, toDate, side, sorting):
    symbol = getSymbolOfUsername(username)
    symbol_db = client[f'{symbol}_db']
    trade_collection = symbol_db['trade']
    if(fromDate==False):
        fromDate = trade_collection.find_one(sort=[("Date", -1)])['Date']
    if(toDate==False):
        toDate = trade_collection.find_one(sort=[("Date", -1)])['Date']
    if side=='buy':
        side = 'B_account'
    else:
        side = 'S_account'
    dftrade = pd.DataFrame(trade_collection.find({ 'Date' : { '$gte' :  min(toDate,fromDate), '$lte' : max(toDate,fromDate)}}))
    dfbalance = pd.DataFrame(symbol_db['balance'].find({ 'date' : { '$gte' :  min(toDate,fromDate), '$lte' : max(toDate,fromDate)}}))

    
    if len(dftrade)<=0:
        
        return json.dumps({'replay':False, 'msg':'معاملات یافت نشد'})

    else:
        dftrade['Value'] = dftrade['Volume'] * dftrade['Price']
        dfside = dftrade.groupby(by=[side]).sum()
        dfside = dfside[['Volume','Value']]
        dfside['Price'] = dfside['Value']/dfside['Volume']
        dfside['code'] = dfside.index
        dfside.index = [CodeToName(x,symbol) for x in dfside.index]
        if sorting == 'zvol':
            dfside = dfside.sort_values(by=['Volume'],ascending=False)
        elif sorting == 'avol':
            dfside = dfside.sort_values(by=['Volume'],ascending=True)
        elif sorting == 'aprc':
            dfside = dfside.sort_values(by=['Price'],ascending=True)
        elif sorting == 'zprc':
            dfside = dfside.sort_values(by=['Price'],ascending=False)
        dfside = dfside.reset_index()
        dfside = dfside.reset_index()
        dfside.columns = ['id','name','volume','value','price','code']
        dffinall = pd.DataFrame()
        dffinall['value'] = dfside['value']
        dffinall['price'] = dfside['price']
        dffinall['volume'] = dfside['volume']
        dffinall['name'] = dfside['name']
        dffinall['id'] = dfside['id']
        dffinall['code'] = dfside['code']
        dffinall['w'] = (dffinall['volume']/dffinall['volume'].max())+0.1
        dffinall['price'] = [round(x) for x in dffinall['price']]
        dffinall['balance'] = '-'
        for i in dffinall.index:
            balance = dfbalance[dfbalance['index']==dffinall['code'][i]]
            balance = balance[balance['date']==balance['date'].max()]['Balance'].values[0]
            dffinall['balance'][i] = balance

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
    print('-'*10)
    print(alldatetrade)
    dfBalance = pd.DataFrame(symbol_db['balance'].find({'index':code})).drop(columns=['_id','Volume_B','Volume_S','index'])
    alldatetrade = list(filter(lambda x : x >= dfBalance['date'].min() , alldatetrade))
    alldatetrade = list(filter(lambda x : x <= dfBalance['date'].max() , alldatetrade))

    for i in alldatetrade:
        if i not in list(dfBalance['date']):
            dfBalance.loc[dfBalance.index.max()+1]=[i,0]
    dfBalance = dfBalance.sort_values(by='date').reset_index().drop(columns=['index'])
    dfBalance['cum'] = dfBalance['Balance'].cumsum()
    dfBalance = dfBalance.drop(columns=['Balance'])
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
        dfnewtrader = dfnewtrader.sort_values(by=['Date']).reset_index().drop(columns=['index'])
        #dfnewtrader = dfnewtrader[dfnewtrader.index<30]
        dfnewtrader = dfnewtrader.to_dict(orient='recodes')
        return json.dumps({'replay':True,'data':dfnewtrader})

def station(username, fromDate, toDate, side):
    symbol = getSymbolOfUsername(username)
    symbol_db = client[f'{symbol}_db']
    if(fromDate==False):
        fromDate = symbol_db['trade'].find_one(sort=[("Date", -1)])['Date']
    if(toDate==False):
        toDate = symbol_db['trade'].find_one(sort=[("Date", 1)])['Date']
    dfTrader = pd.DataFrame(symbol_db['trade'].find({ 'Date' : { '$gte' :toDate  , '$lte' :fromDate }}))
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

            #dfistgah['Istgah'][i] = farasahm_db['broker'].find_one({'TBKEY':' '+(i.replace(' ',''))})['TBNAME']
            try:dfistgah['Istgah'][i] = farasahm_db['broker'].find_one({'TBKEY':' '+(key.replace(' ',''))})['TBNAME']
            except:dfistgah['Istgah'][i] = 'نامعلوم'
        print(dfistgah)
        dfistgah = dfistgah.to_dict(orient='recodes')
        return json.dumps({'replay':True,'data':dfistgah})

def dashbord(username):
    power = requests.get(url=f'http://sourcearena.ir/api/?token=6e437430f8f55f9ba41f7a2cfea64d90&power={getSymbolTseOfUsername(username)}&days=30').json()
    return json.dumps({'replay':True,'power':power})

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
    day = int(str(maxDate)[6:8])
    mon = int(str(maxDate)[4:6])-period
    year =int(str(maxDate)[0:4])
    if mon<=0:
        onPriod = int(str(year-1)+str(mon+12)+str(day))
    else:
        onPriod = int(str(year)+str(mon)+str(day))
    
    outTrader = list(set(dftrade[dftrade['Date']>onPriod]['S_account']))
    keepTrader = dftrade[dftrade['Date']<=onPriod]
    keepTrader['out'] = keepTrader['S_account'].isin(outTrader)
    keepTrader = keepTrader[keepTrader['out']==False]
    keepTrader['Value'] = keepTrader['Price'] * keepTrader['Volume']
    keepTrader['ValueDate'] = keepTrader['Volume'] * keepTrader['Date']
    keepTrader = keepTrader.groupby(by='B_account').sum()
    keepTrader['Price'] = keepTrader['Value'] / keepTrader['Volume']
    keepTrader['Date'] = keepTrader['ValueDate']/keepTrader['Volume']
    keepTrader = keepTrader.reset_index()
    keepTrader = keepTrader[['B_account','Volume']]
    keepTrader['Date'] = [str(x)[0:4]+'/'+str(x)[4:6]+'/'+str(x)[6:8] for x in keepTrader['Date']]
    keepTrader['Price'] =[int(x) for x in keepTrader['Price']]
    keepTrader = keepTrader.to_dict(orient='records')


    print(keepTrader)

    data = ([outTrader])
    return json.dumps({'data':keepTrader})

