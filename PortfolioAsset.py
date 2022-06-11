import json
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
    '''این تابع برای بازگردانندن نام مشتریان کاربر میباشد
    username: نام کاربری که که درخواست نام مشتریان را کرده
    '''
    customerNames = pd.DataFrame(portfolio[usename+'_'+'trad'].find())
    if len(customerNames)>0:
        theresponse = True
        msg = ''
        data = list(set(pd.DataFrame(portfolio[usename+'_'+'trad'].find())['عنوان مشتری']))
    else:
        theresponse = False
        msg = 'مشتری تعریف نشده'
        data = ''
    return json.dumps({'replay':theresponse, 'msg':msg, 'databack':data})


def customerreview(username, customer):
    df = pd.DataFrame(pd.DataFrame(portfolio[username+'_'+'trad'].find({'عنوان مشتری':customer})))
    print(df)
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
    nobuy = list(df[df['تعداد_خرید']==0].index)
    if len(nobuy)>0:
        msg = 'این مشتری تعدادی فروش بدون خرید داشته'
        replay = False
        databack = nobuy
        return json.dumps({'msg':msg, 'replay':replay, 'databack':databack, 'code':'NoBuy'})
    else:
        return json.dumps({'msg':'بدون مشکل', 'replay':True, 'databack':'', 'code':'ok'})

