
import json
import pandas as pd
import pymongo
from general import *
import numpy as np


client = pymongo.MongoClient()
portfolio = client['portfolio']


def uploadTradFile(file,username):
    '''
    این تابع برای بروز رسانی معاملات به صورت گروهی و توسط فایلی که توسط سامانه ارائه میشود  میباشد
    file: فایل آپلود شده در فرانت به با فرمت اکسلی
    username: نام کاربری که اقدام به بارگذاری کرده
    '''
    print(file)
    inp = pd.read_excel(file)
    requisiteColumns = ['تاریخ معامله','نوع معامله','کد بورسی','شناسه ملی','عنوان مشتری','نام شعبه','نام شعبه مشتری','نماد','نماد سپرده گذاری','تعداد','قیمت','ارزش معامله','گروه مشتری']
    checkRequisiteColumns = len([x for x in requisiteColumns if x in inp.columns]) == 13
    if checkRequisiteColumns:
        inp = inp[requisiteColumns]
        inp['شناسه ملی'] = inp['شناسه ملی'].fillna(0)
        inp['عنوان مشتری'] = inp['عنوان مشتری'].fillna(inp['کد بورسی'])
        inp['نام شعبه'] = inp['نام شعبه'].fillna('-')
        inp['نام شعبه مشتری'] = inp['نام شعبه مشتری'].fillna('-')
        inp['نماد سپرده گذاری'] = inp['نماد سپرده گذاری'].fillna(inp['نماد'])
        inp['گروه مشتری'] = inp['گروه مشتری'].fillna('-')
        inp['تاریخ معامله عددی'] = [x.replace('/','') for x in inp['تاریخ معامله']]
        inp['تاریخ معامله عددی'] = [int(x) for x in inp['تاریخ معامله عددی']]
        missColumns = inp.isnull().sum().sum()
        if missColumns == 0:
            date = set(inp['تاریخ معامله'])
            for i in date:
                checkDuplicate = len(list(portfolio[username+'_'+'trad'].find({'تاریخ معامله':i})))>0
                if checkDuplicate:
                    portfolio[username+'_'+'trad'].delete_many({'تاریخ معامله':i})
            portfolio[username+'_'+'trad'].insert_many(inp.to_dict(orient='records'))
            theresponse = True
            msg = 'اطلاعات بروز شد'
        else:
            theresponse = False
            msg='برخی سلول ها خالی هستند'
    else:
        theresponse = False
        msg = 'تمام ستون های مورد نیاز موجود نیست'
    return json.dumps({'replay':theresponse, 'msg':msg, 'databack':''})


def investorlist(username):
    inv = pd.DataFrame(portfolio[username+'_'+'trad'].find({},{'_id':0, 'کد بورسی':1, 'عنوان مشتری':1})).drop_duplicates()
    inv.columns = ['code','name']
    inv= inv.to_dict(orient='records')
    return json.dumps({'df':inv})

def symbolelist():
    url = 'https://sourcearena.ir/api/?token=6e437430f8f55f9ba41f7a2cfea64d90&all&type=2'
    responset = pd.DataFrame(requests.get(url=url).json())[['name']]
    url = 'https://sourcearena.ir/api/?token=6e437430f8f55f9ba41f7a2cfea64d90&closed_symbols'
    try:responset = responset.append(pd.DataFrame(requests.get(url=url).json())[['name']]).to_dict(orient='record')
    except:responset = responset.to_dict(orient='record')
    return json.dumps(responset)

def updatemanual(username,date,invester,side,symbol,price,amunt):
    try:
        personal = portfolio[username+'_'+'trad'].find_one({'کد بورسی':invester})
        lastSymbol = portfolio[username+'_'+'trad'].find_one({'نماد':symbol})
        if lastSymbol==None: lastSymbol = portfolio[username+'_'+'trad'].find_one({'نماد':symbol+'1'})
        dic = {'تاریخ معامله':str(date)[0:4]+'/'+str(date)[4:6]+'/'+str(date)[6:8]}
        if side=='buy': dic['نوع معامله'] = 'خرید'
        else: dic['نوع معامله'] = 'فروش'
        dic['کد بورسی'] = invester
        dic['شناسه ملی'] = personal['شناسه ملی']
        dic['عنوان مشتری'] = personal['عنوان مشتری']
        dic['نام شعبه'] = personal['نام شعبه']
        dic['نام شعبه مشتری'] = personal['نام شعبه مشتری']
        dic['نام شعبه مشتری'] = personal['نام شعبه مشتری']
        dic['نماد'] = lastSymbol['نماد']
        dic['نماد سپرده گذاری'] = lastSymbol['نماد سپرده گذاری']
        dic['تعداد'] = amunt
        dic['قیمت'] = price
        dic['ارزش معامله'] = int(price)*int(amunt)
        dic['گروه مشتری'] = personal['گروه مشتری']
        dic['گروه مشتری'] = personal['گروه مشتری']
        dic['تاریخ معامله عددی'] = date
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
    if len(TradeInvester)==0 and len(noInserBuy)>0:return json.dumps({'replay':True, 'noInserBuy':noInserBuy, 'TradeInvester':False, 'indsGroup':False, 'name':name})
    if len(TradeInvester)==0 and len(noInserBuy)==0:return json.dumps({'replay':True, 'noInserBuy':False, 'TradeInvester':False, 'indsGroup':False, 'name':name})
    TradeInvester['full_name'] = ''
    TradeInvester['inds'] = ''
    TradeInvester['market'] = ''
    TradeInvester['state'] = ''
    TradeInvester['final_price'] = ''
    for i in TradeInvester.index:
        symbol = TradeInvester['symbol'][i].replace('1','')
        url = f'https://sourcearena.ir/api/?token=6e437430f8f55f9ba41f7a2cfea64d90&name={symbol}'
        req = requests.get(url=url).json()
        TradeInvester['full_name'][i] = req['full_name']
        TradeInvester['inds'][i] = req['type']
        TradeInvester['market'][i] = req['market']
        TradeInvester['state'][i] = req['state']
        TradeInvester['final_price'][i] = int(req['final_price'])
    TradeInvester['ValueBalance'] = TradeInvester['Balance'] * TradeInvester['final_price']
    TradeInvester['PriceBuy'] = round(TradeInvester['ValueBuy'] /TradeInvester['AmuntBuy'])
    TradeInvester['PriceSel'] = round(TradeInvester['ValueSel'] /TradeInvester['AmuntSel'])
    print(TradeInvester)

    return json.dumps({'o':'o'})