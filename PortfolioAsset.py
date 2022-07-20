
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
    responset = responset.append(pd.DataFrame(requests.get(url=url).json())[['name']]).to_dict(orient='record')
    return json.dumps(responset)