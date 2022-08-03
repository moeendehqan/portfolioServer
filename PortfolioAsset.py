
from dataclasses import replace
import json
import math
import pandas as pd
import pymongo
from general import *
import numpy as np


client = pymongo.MongoClient()
portfolio = client['portfolio']


def uploadTradFile(file,username):
    user = client['farasahm']['user'].find_one({'username':username},{'_id':0})
    if user['mainaccount'] != 'noting':
        user = client['farasahm']['user'].find_one({'username':user['mainaccount']},{'_id':0})
    inp = pd.read_excel(file,header=3)
    requisiteColumns = ['نام','نام خانوادگی','نماد','حجم','کد بورسی','نام سهم']
    checkRequisiteColumns = len([x for x in requisiteColumns if x in inp.columns]) == 6
    if checkRequisiteColumns==False:
        return json.dumps({'replay':False, 'msg':'تمام ستون های مورد نیاز موجود نیست'})
    inp = inp[requisiteColumns]
    inp = inp[inp['نام']!='جمع کل']
    inp['سرمایه گذار'] = inp['نام'].replace("'","") + inp['نام خانوادگی']
    inp['کد بورسی'] = [x.replace("*","") for x in inp['کد بورسی']]
    inp = inp[['سرمایه گذار','کد بورسی','حجم','نماد','نام سهم']]
    missColumns = inp.isnull().sum().sum()
    if missColumns > 0:
        return json.dumps({'replay':False, 'msg':'برخی سلول ها خالی هستند'})
    InvCntAdd =  len(set(inp['کد بورسی']))
    if InvCntAdd>user['portfoliolimitinv']:
        return json.dumps({'replay':False, 'msg':'تعداد سرمایه گذار ها بیش از حد مجاز است'})
    inp.columns = ['inv','code','volume','symbol','stocks']
    inp['act'] = 'buy'
    inp['status'] = 'Check'
    inp['date'] = ''
    inp['price'] = ''
    comdoct = pd.DataFrame(portfolio[user['username']+'_'+'trad'].find({'status':'comformtion'},{'_id':0}))
    if len(comdoct)==0:
        inp = inp.to_dict(orient='records')
        portfolio[user['username']+'_'+'trad'].insert_many(inp)
        return CheckDoctOne(username)
    
    comdoct = comdoct.groupby(['code','symbol']).sum()
    inp = inp.set_index(['code','symbol'])
    df = comdoct.join(inp,rsuffix='_cheack', lsuffix='_comform',how='outer').reset_index()
    df['volume_comform'] = [float(x) for x in df['volume_comform']]
    df['volume_cheack'] = [float(x) for x in df['volume_cheack']]
    for i in df.index:
        if str(df['volume_comform'][i]) == 'nan': df['volume_comform'][i] = 0
        if str(df['volume_cheack'][i]) == 'nan': df['volume_cheack'][i] = 0
    df['volume_comform'] = df['volume_cheack'] - df['volume_comform']
    df = df[df['volume_comform']!=0]
    if len(df)==0:
        return CheckDoctOne(username)
    df = df.drop(columns=['volume_cheack'])
    df.columns = ['code', 'symbol', 'volume', 'inv', 'stocks', 'act', 'status','date', 'price']
    df['date'] = 0
    df['status'] = 'Check'
    df['price'] = 0
    for i in df.index:
        if df['volume'][i] > 0: df['act'][i] = 'buyinc'
        else: df['act'][i] = 'sel'
        if str(df['inv'][i]) == 'nan': 
            df['inv'][i] = portfolio[user['username']+'_'+'trad'].find_one({'symbol':df['symbol'][i]})['inv']
            df['stocks'][i] = portfolio[user['username']+'_'+'trad'].find_one({'symbol':df['symbol'][i]})['stocks']
    df = df.to_dict(orient='records')
    portfolio[user['username']+'_'+'trad'].insert_many(df)
    return CheckDoctOne(username)

def CheckDoctOne(username):
    user = client['farasahm']['user'].find_one({'username':username},{'_id':0})
    if user['mainaccount'] != 'noting':
        user = client['farasahm']['user'].find_one({'username':user['mainaccount']},{'_id':0})
    df = list(portfolio[user['username']+'_'+'trad'].find({"status":"Check"},{'_id':0}))
    if len(df)==0:
        return json.dumps({'replay':True, 'df':None, 'len':None})
    return json.dumps({'replay':True, 'df':df[0], 'len':len(df)})
    
def confdoct(username,stocks,act,price,date):
    df = stocks
    df['act']=act
    df['price']=price
    df['date']=date
    df['status']='comformtion'
    print(df)
    portfolio[username+'_'+'trad'].delete_one({'inv':df['inv'],'code':df['code'],'volume':df['volume'],'symbol':df['symbol'],'status':'Check'})
    portfolio[username+'_'+'trad'].insert_one(df)
    return json.dumps({'replay':True})

def deldoct(username,stocks):
    portfolio[username+'_'+'trad'].delete_one(stocks)
    return json.dumps({'replay':True})

def investorlist(username):
    inv = pd.DataFrame(portfolio[username+'_'+'trad'].find({},{'_id':0, 'inv':1, 'code':1})).drop_duplicates()
    inv.columns = ['code','name']
    inv= inv.to_dict(orient='records')
    return json.dumps({'df':inv})

def symbolelist():
    url = 'https://sourcearena.ir/api/?token=6e437430f8f55f9ba41f7a2cfea64d90&all&type=2'
    responset = pd.DataFrame(requests.get(url=url).json())[['name','full_name']]
    url = 'https://sourcearena.ir/api/?token=6e437430f8f55f9ba41f7a2cfea64d90&closed_symbols'
    try:
        responset = responset.append(pd.DataFrame(requests.get(url=url).json())[['name','full_name']]).to_dict(orient='records')
    except:
        responset = responset.to_dict(orient='records')
    return json.dumps(responset)

def updatemanual(username,date,investername,invester,side,fullname,symbol,price,amunt):
    try:
        personal = portfolio[username+'_'+'trad'].find_one({'کد بورسی':invester})
        dic = {'date':date}
        dic['inv'] = invester
        dic['code'] = investername
        dic['volume'] = amunt
        dic['symbol'] = symbol
        dic['stocks'] = fullname
        dic['act'] = side
        dic['act'] = side
        dic['status'] = 'comformtion'
        dic['price'] = price
        portfolio[username+'_'+'trad'].insert_one(dic)
        return json.dumps({'replay':True})
    except:
        return json.dumps({'replay':False})

def asset(username, invester, date):
    cl = pd.DataFrame(portfolio[username+'_'+'trad'].find({'code':invester,'status':'comformtion'},{'_id':0}))
    df = cl
    name = df['inv'][0]
    print(df)
    df['volume'] = [int(x) for x in df['volume']]
    df['price'] = [int(x) for x in df['price']]
    df['value'] = df['volume'] * df['price']
    df = df.groupby(by=['symbol','act','stocks']).sum().reset_index()
    dfb = df[df['act']=='buy']
    dfs = df[df['act']=='sel']
    dfb['price'] = dfb['value'] / dfb['volume']
    dfs['price'] = dfs['value'] / dfs['volume']
    df = dfb.set_index('stocks').join(dfs.set_index('stocks'),rsuffix='_s',lsuffix='_b',how='left').fillna(0)
    df['balance'] = df['volume_b'] - df['volume_s']
    df = df[df['balance']>=0]
    df['finalprice'] = 0
    df['inds'] = 0
    url='https://sourcearena.ir/api/?token=6e437430f8f55f9ba41f7a2cfea64d90&all&type=0'
    market = pd.DataFrame(requests.get(url=url).json())
    for s in df.index:
        sn = df['symbol_b'][s].replace('1','')
        marketO = market[market['name']==sn]
        if len(marketO)>0:
            df['finalprice'][s] = int(list(marketO['final_price'])[0])
            df['inds'][s] = (list(marketO['industry'])[0])
        else:
            url = f'https://sourcearena.ir/api/?token=6e437430f8f55f9ba41f7a2cfea64d90&name={s}'
            finall = requests.get(url=url).json()
            if finall!=None:
                df['finalprice'][s] = int(finall['final_price'])
                df['inds'][s] = (finall['type'])
            else:
                df['finalprice'][s] = 0
                df['inds'][s] = 0
    df['valuebalance'] = df['finalprice'] * df['balance']
    df['profit'] = (df['valuebalance'] + df['value_s']) - df['value_b']
    dfindc = df.groupby('inds').sum()[['valuebalance']].reset_index()
    dfindc['ofPortfo'] = dfindc['valuebalance'] / dfindc['valuebalance'].sum()
    dfindc = dfindc.to_dict(orient='records')
    df = df[['symbol_b','price_b','finalprice','balance','profit','valuebalance']]
    df.columns = ['symbol','PriceBuy','final_price','Balance','Profit','ValueBalance']
    df['ofPortfo'] = df['ValueBalance'] / df['ValueBalance'].sum()
    df = df.to_dict(orient='records')
    return json.dumps({'name':name,'indsGroup':dfindc,'assetInvester':df})



def revenue(username, invester, date):
    investor_df = pd.DataFrame(portfolio[username+'_'+'trad'].find({'کد بورسی':invester},{'_id':0}))
    TradeInvester = investor_df
    name = TradeInvester['عنوان مشتری'][0] #send
    if len(date)==6:
        TradeInvester = TradeInvester[TradeInvester['تاریخ معامله عددی']<=int(date)]
    TradeInvester['تعداد'] =[int(str(x).replace(',','')) for x in  TradeInvester['تعداد']]
    TradeInvester['ارزش معامله'] = [int(str(x).replace(',','')) for x in  TradeInvester['ارزش معامله']]
    TradeInvester = TradeInvester.groupby(by=['نوع معامله','نماد']).sum()[['تعداد','ارزش معامله']]
    TradeInvester = TradeInvester.reset_index()
    dfB = TradeInvester[TradeInvester['نوع معامله']=='خرید'].drop(columns='نوع معامله')
    dfS = TradeInvester[TradeInvester['نوع معامله']=='فروش'].drop(columns='نوع معامله')
    TradeInvester = dfB.set_index('نماد').join(dfS.set_index('نماد'), lsuffix='_B', rsuffix='_S', how='outer').reset_index().replace(np.nan,0)
    TradeInvester.columns = ['symbol','AmuntBuy','ValueBuy','AmuntSel','ValueSel']
    TradeInvester['Balance'] = TradeInvester['AmuntBuy'] - TradeInvester['AmuntSel']
    noInserBuy = TradeInvester[TradeInvester['Balance']<0]
    noInserBuy = noInserBuy[['symbol','Balance']]
    noInserBuy['Balance'] = noInserBuy['Balance']*-1
    noInserBuy = noInserBuy.to_dict(orient='records') #send
    TradeInvester = TradeInvester[TradeInvester['Balance']>=0]
    if len(TradeInvester)==0 and len(noInserBuy)>0:return json.dumps({'replay':True, 'noInserBuy':noInserBuy, 'TradeInvester':False, 'name':name})
    if len(TradeInvester)==0 and len(noInserBuy)==0:return json.dumps({'replay':True, 'noInserBuy':False, 'TradeInvester':False, 'name':name})
    TradeInvester['full_name'] = ''
    TradeInvester['inds'] = ''
    TradeInvester['market'] = ''
    TradeInvester['final_price'] = ''
    for i in TradeInvester.index:
        symbol = TradeInvester['symbol'][i].replace('1','')
        url = f'https://sourcearena.ir/api/?token=6e437430f8f55f9ba41f7a2cfea64d90&name={symbol}'
        req = requests.get(url=url).json()
        TradeInvester['full_name'][i] = req['full_name']
        TradeInvester['inds'][i] = req['type']
        TradeInvester['market'][i] = req['market']
        TradeInvester['final_price'][i] = int(req['final_price'])
    TradeInvester['ValueBalance'] = TradeInvester['Balance'] * TradeInvester['final_price']
    TradeInvester['PriceBuy'] = round(TradeInvester['ValueBuy'] /TradeInvester['AmuntBuy'])
    TradeInvester['PriceSel'] = round(TradeInvester['ValueSel'] /TradeInvester['AmuntSel'])
    TradeInvester['profit'] = (TradeInvester['ValueBalance'] + TradeInvester['ValueSel'])- TradeInvester['ValueBuy']
    TradeInvester = TradeInvester.fillna(0)
    TradeInvester = TradeInvester.to_dict(orient='records')
    if len(TradeInvester)>0 and len(noInserBuy)>0: return json.dumps({'replay':True, 'noInserBuy':noInserBuy, 'TradeInvester':TradeInvester, 'name':name})
    if len(TradeInvester)>0 and len(noInserBuy)==0: return json.dumps({'replay':True, 'noInserBuy':False, 'TradeInvester':TradeInvester, 'name':name})
