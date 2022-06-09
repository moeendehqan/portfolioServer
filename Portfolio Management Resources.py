import json
import pandas as pd
import pymongo

client = pymongo.MongoClient()
portfolio = client['portfolio']


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
        inp['تاریخ معامله عددی'] = inp['تاریخ معامله'].replace('/','')
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
            msg='بازگشت خطا به دلیل خالی بودن برخی سلول ها'
    else:
        theresponse = False
        msg = 'بازگشت خطا به دلیل نبودن تمام ستون های مورد نیاز'

    return json.dump({'theresponse':theresponse, 'msg':msg, 'data':''})


def customerNames(usename):
    '''این تابع برای بازگردانندن نام مشتریان کاربر میباشد
    username: نام کاربری که که درخواست نام مشتریان را کرده
    '''
    customerNames = pd.DataFrame(portfolio[username+'_'+'trad'].find())
    if len(customerNames)>0:
        theresponse = True
        msg = ''
        data = list(set(pd.DataFrame(portfolio[username+'_'+'trad'].find())['عنوان مشتری']))
    else:
        theresponse = False
        msg = 'مشتری تعریف نشده'
        data = ''
    return json.dumps({'theresponse':theresponse, 'msg':msg, 'data':data})


username = 'hakimian'
customerName = 'حمید مولائی فرد'
fromDate =''
toDate = '1401/03/18'
'''
این تابع  وضعیت یک مشتری را باز میگرداند
'''