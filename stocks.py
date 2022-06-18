import json
import pandas as pd
import pymongo
from general import *
import numpy as np
from numpy import mean, sort

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
    cheack_culomns = (df.columns == standard_culomns)
    cheack_culomns = [x*1 for x in cheack_culomns]
    cheack_culomns = mean(cheack_culomns)==1
    return cheack_culomns
    
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
        if symbol not in dfTrade['Symbol']:
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

def tradersData(username, fromDate, toDatye, side):
    symbol = getSymbolOfUsername(username)
    symbol_db = client[f'{symbol}_db']
    trade_collection = symbol_db['trade']
    if(fromDate==False):
        fromDate = trade_collection.find_one(sort=[("Date", -1)])['Date']
    if(toDatye==False):
        toDatye = trade_collection.find_one(sort=[("Date", 1)])['Date']
    if side=='buy':
        side = 'B_account'
    else:
        side = 'S_account'
    dftrade = pd.DataFrame(trade_collection.find({ 'Date' : { '$gte' :  min(toDatye,fromDate), '$lte' : max(toDatye,fromDate)}}))
    if len(dftrade)<=0:
        
        return json.dumps({'replay':False, 'msg':'معاملات یافت نشد'})

    else:
        dftrade['Value'] = dftrade['Volume'] * dftrade['Price']
        dfside = dftrade.groupby(by=[side]).sum()
        dfside = dfside[['Volume','Value']]
        dfside['Price'] = dfside['Value']/dfside['Volume']
        dfside['code'] = dfside.index
        dfside.index = [CodeToName(x,symbol) for x in dfside.index]
        dfside = dfside.sort_values(by=['Volume'],ascending=False)
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
    info = str(cr['Fullname']).replace('،',' ')  + ' , با کد ملی ' + str(cr['NationalId']) + ' , صادره از ' + str(cr['Ispl']) + ' , متولد ' + str(int(cr['Birthday'])) + '\n' + 'ایستگاه های معاملاتی:' + '\n'
    brokerName = list(set([farasahm_db['broker'].find_one({'TBKEY':' '+x})['TBNAME'] for x in brokercode]))
    for i in brokerName:
        info = info + i + ','
    return json.dumps({'replay':True, 'msg':info})


def historicode(username, code):
    symbol = getSymbolOfUsername(username)
    symbol_db = client[f'{symbol}_db']
    alldatetrade = [symbol_db['trade'].find_one(sort=[("Date", pymongo.ASCENDING)])['Date'],
                    symbol_db['trade'].find_one(sort=[("Date", pymongo.DESCENDING)])['Date']]
    dfBalance = pd.DataFrame(symbol_db['balance'].find({'index':code})).drop(columns=['_id','Volume_B','Volume_S','index'])
    alldatetrade = list(filter(lambda x : x >= dfBalance['date'].min() , alldatetrade))
    alldatetrade = list(filter(lambda x : x <= dfBalance['date'].max() , alldatetrade))
    for i in alldatetrade:
        if i not in list(dfBalance['date']):
            dfBalance.loc[dfBalance.index.max()+1]=[i,0]
    dfBalance = dfBalance.sort_values(by='date').reset_index().drop(columns=['index'])
    dfBalance['cum'] = dfBalance['Balance'].cumsum()
    dfBalance['ww'] = dfBalance['cum']/(max([abs(x) for x in dfBalance['cum']]))
    dfBalance = dfBalance.drop(columns=['Balance'])
    dfBalance = dfBalance.to_dict(orient='records')
    return json.dumps({'replay':True,'data':dfBalance})