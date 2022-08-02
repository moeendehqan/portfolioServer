
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
    print(df.columns)
    df['date'] = ''
    df['status'] = 'Check'
    df['price'] = ''
    for i in df.index:
        if df['volume'][i] > 0: df['act'][i] = 'buyinc'
        else: df['act'][i] = 'sel'
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
    portfolio[username+'_'+'trad'].delete_one({'inv':df['inv'],'code':df['code'],'volume':df['volume'],'symbol':df['symbol'],'status':'Check'})
    portfolio[username+'_'+'trad'].insert_one(df)
    return json.dumps({'replay':True})

def deldoct(username,stocks):
    portfolio[username+'_'+'trad'].delete_one(stocks)
    return json.dumps({'replay':True})










def investorlist(username):
    inv = pd.DataFrame(portfolio[username+'_'+'trad'].find({},{'_id':0, 'inv':1, 'code':1})).drop_duplicates()
    print(inv)
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
        dic['inv'] = investername
        dic['code'] = invester
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
    investor_df = pd.DataFrame(portfolio[username+'_'+'trad'].find({'کد بورسی':invester},{'_id':0}))
    assetInvester = investor_df
    name = assetInvester['عنوان مشتری'][0]
    if len(date)==6:
        assetInvester = assetInvester[assetInvester['تاریخ معامله عددی']<=int(date)]
    assetInvester['تعداد'] =[int(str(x).replace(',','')) for x in  assetInvester['تعداد']]
    assetInvester['ارزش معامله'] = [int(str(x).replace(',','')) for x in  assetInvester['ارزش معامله']]
    assetInvester = assetInvester.groupby(by=['نوع معامله','نماد']).sum()[['تعداد','ارزش معامله']]
    assetInvester = assetInvester.reset_index()
    dfB = assetInvester[assetInvester['نوع معامله']=='خرید'].drop(columns='نوع معامله')
    dfS = assetInvester[assetInvester['نوع معامله']=='فروش'].drop(columns='نوع معامله')
    assetInvester = dfB.set_index('نماد').join(dfS.set_index('نماد'), lsuffix='_B', rsuffix='_S', how='outer').reset_index().replace(np.nan,0)
    assetInvester.columns = ['symbol','AmuntBuy','ValueBuy','AmuntSel','ValueSel']
    assetInvester['Balance'] = assetInvester['AmuntBuy'] - assetInvester['AmuntSel']
    noInserBuy = assetInvester[assetInvester['Balance']<0]
    noInserBuy = noInserBuy[['symbol','Balance']]
    noInserBuy['Balance'] = noInserBuy['Balance']*-1
    noInserBuy = noInserBuy.to_dict(orient='records')
    assetInvester = assetInvester[assetInvester['Balance']>0]
    if len(assetInvester)==0 and len(noInserBuy)>0:return json.dumps({'replay':True, 'noInserBuy':noInserBuy, 'assetInvester':False, 'indsGroup':False, 'name':name})
    if len(assetInvester)==0 and len(noInserBuy)==0:return json.dumps({'replay':True, 'noInserBuy':False, 'assetInvester':False, 'indsGroup':False, 'name':name})
    assetInvester['full_name'] = ''
    assetInvester['inds'] = ''
    assetInvester['market'] = ''
    assetInvester['state'] = ''
    assetInvester['final_price'] = ''
    for i in assetInvester.index:
        symbol = assetInvester['symbol'][i].replace('1','')
        url = f'https://sourcearena.ir/api/?token=6e437430f8f55f9ba41f7a2cfea64d90&name={symbol}'
        req = requests.get(url=url).json()
        assetInvester['inds'][i] = req['type']
        assetInvester['market'][i] = req['market']
        assetInvester['state'][i] = req['state']
        assetInvester['final_price'][i] = int(req['final_price'])
    assetInvester['ValueBalance'] = assetInvester['Balance'] * assetInvester['final_price']
    assetInvester['PriceBuy'] = 0
    for i in assetInvester.index:
        BuySymbol = investor_df[investor_df['نماد']==assetInvester['symbol'][i]]
        BuySymbol = investor_df[investor_df['نوع معامله']=='خرید'].sort_values(by=['تاریخ معامله عددی','ارزش معامله'],ascending=False)[['تاریخ معامله عددی','تعداد','ارزش معامله']]
        BuySymbol['CumAmunt'] = BuySymbol['تعداد'].cumsum()
        BuySymbol['balance'] = assetInvester['Balance'][i]
        BuySymbol['AB'] = BuySymbol['CumAmunt']<=BuySymbol['balance']
        BuySymbol['CAB'] = (BuySymbol['AB'].shift()-BuySymbol['AB']).fillna(1)
        BuySymbol['CAB'] = ((BuySymbol['CAB'] * BuySymbol['AB']) + BuySymbol['CAB'])==1
        BuySymbol['FullValue'] = BuySymbol['AB'] * BuySymbol['ارزش معامله']
        BuySymbol['HalfValue'] = (BuySymbol['CAB'] * BuySymbol['ارزش معامله'])*(BuySymbol['balance']/BuySymbol['CumAmunt'])
        assetInvester['PriceBuy'][i] = round((BuySymbol['FullValue'].sum() + BuySymbol['HalfValue'].sum()) / assetInvester['Balance'][i],0)
    assetInvester['Profit'] = (assetInvester['final_price'] - assetInvester['PriceBuy']) * assetInvester['Balance']
    assetInvester['ofPortfo'] =(assetInvester['ValueBalance']/assetInvester['ValueBalance'].sum())*100
    assetInvester = assetInvester.fillna(0)
    indsGroup = assetInvester.groupby('inds').sum()['ofPortfo']
    indsGroup = indsGroup.reset_index().to_dict(orient='records')
    assetInvester = assetInvester.to_dict(orient='records')
    if len(assetInvester)>0 and len(noInserBuy)>0: return json.dumps({'replay':True, 'noInserBuy':noInserBuy, 'assetInvester':assetInvester, 'indsGroup':indsGroup, 'name':name})
    if len(assetInvester)>0 and len(noInserBuy)==0: return json.dumps({'replay':True, 'noInserBuy':False, 'assetInvester':assetInvester, 'indsGroup':indsGroup, 'name':name})



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
