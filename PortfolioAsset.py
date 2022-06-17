from asyncio.windows_events import NULL
from cmath import nan
import json
from matplotlib.style import available
import pandas as pd
import pymongo
from general import *
import numpy as np


client = pymongo.MongoClient()
portfolio = client['farasahm']


def uploadTradFile(file,username):
    '''
    این تابع برای بروز رسانی معاملات به صورت گروهی و توسط فایلی که توسط سامانه ارائه میشود  میباشد
    file: فایل آپلود شده در فرانت به با فرمت اکسلی
    username: نام کاربری که اقدام به بارگذاری کرده
    '''
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


def customerNames(usename):
    customerNames = pd.DataFrame(portfolio[usename+'_'+'trad'].find())
    if len(customerNames)>0:
        df = pd.DataFrame(portfolio[usename+'_'+'trad'].find())
        df['تاریخ معامله عددی'] = [int(x) for x in df['تاریخ معامله عددی']]
        theresponse = True
        msg = ''
        customerNames = list(set(df['عنوان مشتری']))
        numTrade = len(df)
        lastUpdate = str(df['تاریخ معامله عددی'].max())
        lastUpdate = lastUpdate[0:4]+'/'+lastUpdate[4:6]+'/'+lastUpdate[6:8]
        return {'replay':theresponse, 'msg':msg, 'databack':{'customerNames':customerNames, 'numTrade':numTrade, 'lastUpdate':lastUpdate}}
    else:
        theresponse = False
        msg = 'مشتری تعریف نشده'
        data = ''
    return {'replay':theresponse, 'msg':msg, 'databack':data}

def customerreview(usename):
    customerNames = pd.DataFrame(portfolio[usename+'_'+'trad'].find())
    if len(customerNames)>0:
        replay = True
        msg = ''
        df = pd.DataFrame(portfolio[usename+'_'+'trad'].find())
        df['nc'] = df['کد بورسی'] + ' - ' + df['عنوان مشتری'] 
        df = df[['nc','نماد','تعداد','ارزش معامله']]
        df = df.set_index('نماد').join(getPriceAllSymbol())
        df = df.dropna()
        
        customerNames = list(set(df['nc']))
        data = {
            'customerNames':customerNames,
        }
        return n #{'replay':replay, 'msg':msg, 'databack':data}
    else:
        replay = False
        msg = 'مشتری تعریف نشده'
        data = ''
    return {'replay':replay, 'msg':msg, 'databack':data}


def customerreviewf(username, customer,dateselect):
    df = pd.DataFrame(pd.DataFrame(portfolio[username+'_'+'trad'].find({'عنوان مشتری':customer})))
    if dateselect!=0:
        print(dateselect)
        df = df[df['تاریخ معامله عددی']<=int(dateselect)]
    profileCustomer = {
        'شناسه ملی':str(df['شناسه ملی'][0]),
        'کد بورسی':str(df['کد بورسی'][0]),
        'عنوان مشتری':str(df['عنوان مشتری'][0]),
        'نام شعبه':str(df['نام شعبه'][0]),
        'نام شعبه مشتری':str(df['نام شعبه مشتری'][0])
        }
    df = df.drop(columns=['_id','کد بورسی','شناسه ملی','عنوان مشتری','نام شعبه','نام شعبه مشتری','نماد سپرده گذاری','گروه مشتری','تاریخ معامله'])
    dfb = df[df['نوع معامله']=='خرید'].drop(columns=['نوع معامله','قیمت','تاریخ معامله عددی'])
    dfb = ClearDf(dfb,['تعداد', 'ارزش معامله'])
    dfb = dfb.groupby('نماد').sum()
    dfs = df[df['نوع معامله']=='فروش'].drop(columns=['نوع معامله','قیمت','تاریخ معامله عددی'])
    dfs = ClearDf(dfs,['تعداد', 'ارزش معامله'])
    dfs = dfs.groupby('نماد').sum()
    df = dfb.join(dfs, lsuffix='_فروش', rsuffix='_خرید', how='outer')
    df = df.replace(np.nan,0)
    df['قیمت_خرید'] = df['ارزش معامله_خرید'] / df['تعداد_خرید']
    df['قیمت_فروش'] = df['ارزش معامله_فروش'] / df['تعداد_فروش']
    df['تعداد'] = df['تعداد_خرید'] - df['تعداد_فروش']
    df['ارزش معامله'] = df['ارزش معامله_خرید'] - df['ارزش معامله_فروش']
    df = df.replace(np.nan,0)
    return df


def customerasset(username, customer):
    df = customerreviewf(username, customer, 0)
    df = df[df['تعداد']>0]
    if len(df)>0:
        df['قیمت_بازار'] = [int(getLivePriceSymbol(x)) for x in df.index]
        df['بازدهی'] = ((df['قیمت_بازار']/df['قیمت_خرید'])-1)*100
        df['بازدهی'] = [str(round(x,2))+'%' for x in df['بازدهی']]
    return df


def customerprofitability(username, customer,dataselect):
    df = customerreviewf(username, customer,dataselect)
    dffalse = df[df['تعداد']<0]
    if len(dffalse)>0:
        dffalse['تعداد'] = [int(x) for x in dffalse['تعداد']]
        dffalse = dffalse['تعداد']
        dffalse = dffalse.reset_index()
        return [True,dffalse.to_dict(orient='records')]
    else:
        return [False, {}]

def updateform(data):
    username = data['username']
    datenamber = data['date']
    date = str(datenamber)
    date = date[0:4]+'/'+date[4:6]+'/'+date[6:]
    side = data['side']
    customer = data['customer']
    symbol = data['symbol']+'1'
    amunt = data['amunt']
    price = data['price']
    otherdata = portfolio[username+'_trad'].find_one({'عنوان مشتری':customer})
    code = otherdata['کد بورسی']
    idnation = otherdata['شناسه ملی']
    nameBranch = otherdata['نام شعبه مشتری']
    value = int(amunt)*int(price)
    gruopCustomer = otherdata['گروه مشتری']
    portfolio[username+'_trad'].insert_one({
        'تاریخ معامله':date,
        'نوع معامله':side,
        'کد بورسی':code,
        'شناسه ملی':idnation,
        'عنوان مشتری':customer,
        'نام شعبه':'Manual',
        'نام شعبه مشتری':nameBranch,
        'نماد':symbol,
        'نماد سپرده گذاری':symbol,
        'تعداد':amunt,
        'قیمت':price,
        'ارزش معامله':value,
        'گروه مشتری':gruopCustomer,
        'تاریخ معامله عددی':datenamber
    })

