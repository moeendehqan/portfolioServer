
import pymongo
import json
import pandas as pd
from numpy import mean, sort
import numpy as np

def is_register_file(df):
    standard_culomns = ['Account','Fullname','Ispl','Isno','Father','Type','NationalId','Birthday','Serial','Firstname','Lastname']
    cheack_culomns = (df.columns == standard_culomns)
    cheack_culomns = [x*1 for x in cheack_culomns]
    cheack_culomns = mean(cheack_culomns)==1
    return cheack_culomns

def is_trade_file(df):
    standard_culomns = ['Symbol','Date','Time','Volume','Price','Buy_brkr','Sel_brkr','Ticket_no','Cancel','B_account','S_account']
    cheack_culomns = (df.columns == standard_culomns)
    print(cheack_culomns)
    cheack_culomns = [x*1 for x in cheack_culomns]
    cheack_culomns = mean(cheack_culomns)==1
    return cheack_culomns

client = pymongo.MongoClient()
farasahm_db = client['farasahm']

client = pymongo.MongoClient()


symbol = 'دویسات'
Trade = pd.read_excel(r'C:/New folder/trade.xlsx')
Register = pd.read_excel(r'C:/New folder/reg.xlsx')

def updateFile(symbol, Trade, Register):
    symbol_db = client[f'{symbol}_db']
    trade_collection = symbol_db['trade']
    register_collection = symbol_db['register']
    balance_collection = symbol_db['balance']


    dfRegister = Register
    dfTrade = Trade

    if is_trade_file(dfTrade)==False:
        if symbol not in dfTrade['Symbol']:
            return json.dumps({'res':False,'msg':'content trade'})
    if is_register_file(dfRegister)==False:
        return json.dumps({'res':False,'msg':'content register'})
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




print(updateFile(symbol, Trade, Register))