from ntpath import join
from h11 import Data
from matplotlib.pyplot import axes
import numpy as np
import pymongo
import json
import pandas as pd
from flask import jsonify

client = pymongo.MongoClient()
etf_db = client['etf']
farasahm_db = client['farasahm']

def getSymbolOfUsername(userName):
    symbol = farasahm_db['user'].find_one({'username':userName})
    if len(symbol)>0:
        return symbol['etfSymbol']
    else:
        return False

def Day_list():  
    yaer = ['1399','1400','1401','1402']
    mon = ['01','02','03','04','05','06','07','08','09','10','11','12']
    day = []
    listday= []
    for x in range(32):
        if x != 0:
            if len(str(x))==1:
                day.append('0'+str(x))
            else:
                day.append(str(x))
    for y in yaer:
        for m in mon:
            if int(m)<7:
                for d in day:
                    listday.append(y+m+d)
            if int(m)>=7 and int(m) != 12:
                for d in day[:-1]:
                    listday.append(y+m+d)
            if int(m) == 12 and y!='1399':
                for d in day[:-2]:
                    listday.append(y+m+d)
            if int(m) == 12 and y=='1399':
                for d in day[:-1]:
                    listday.append(y+m+d)
    listday = [int(x) for x in listday]
    return listday



def etf_nav(username,fromDate,toDate):
    symbol = getSymbolOfUsername(username)
    if(fromDate==False):
        fromDate = etf_db[f'{symbol}_collection'].find_one(sort=[("dateInt", -1)])['dateInt']
    if(toDate==False):
        toDate = etf_db[f'{symbol}_collection'].find_one(sort=[("dateInt", 1)])['dateInt']
    df = pd.DataFrame(etf_db[f'{symbol}_collection'].find({ 'dateInt' : { '$gte' :  min(int(toDate),int(fromDate)), '$lte' : max(int(toDate),int(fromDate))}}))
    if len(df)==0:
        return json.dumps({'replay':False,'msg':'اطلاعاتی موجودی نیست'})
    df['deffNav'] = df['final_price'] - df['nav']
    df['RatedeffNav'] = (round(((df['final_price'] / df['nav'])-1)*10000))/100
    df['RatedeffNav'] = df['RatedeffNav'].replace(np.inf,0)
    df = df.sort_values(by=['dateInt'],ascending=False)
    df = df[['date','final_price','close_price_change_percent','nav','deffNav','RatedeffNav','dateInt']]
    df = df.to_json(orient='records')
    return json.dumps({'replay':True, 'data':df})

def etf_volume(username,fromDate,toDate):
    symbol = getSymbolOfUsername(username)
    if (fromDate==False) and (toDate==False):
        limitLen = True
    else:
        limitLen:False

    if(fromDate==False):
        fromDate = etf_db[f'{symbol}_collection'].find_one(sort=[("dateInt", -1)])['dateInt']
    if(toDate==False):
        toDate = etf_db[f'{symbol}_collection'].find_one(sort=[("dateInt", 1)])['dateInt']
    df = pd.DataFrame(etf_db[f'{symbol}_collection'].find())[['date','dateInt','trade_volume']]
    df['Avg30'] = df['trade_volume'].rolling(30).mean()
    df['Avg7'] = df['trade_volume'].rolling(7).mean()
    df = df[df['dateInt']<max(int(fromDate),int(toDate))]
    df = df[df['dateInt']>min(int(fromDate),int(toDate))]
    df = df.sort_values(by=['dateInt'],ascending=True)
    if limitLen:
        df = (df[-30:])
    return df.to_json(orient='records')


def etf_return(username,onDate,target,periodList):
    symbol = getSymbolOfUsername(username)
    psPeriod = list(farasahm_db['etflist'].find({'نماد':symbol}))[0][' دوره تقسیم سود ']
    if psPeriod=='ندارد':
        df = pd.DataFrame(etf_db[f'{symbol}_collection'].find({},{ 'dateInt':1, '_id':0,'final_price':1})).set_index('dateInt')
        day_list = Day_list()
        day_list =  filter(lambda x: x >= df.index.min(), day_list)
        day_list =  filter(lambda x: x <= df.index.max(), day_list)
        df = df.join(pd.DataFrame(index=day_list), how='right')
        df = df.sort_index(ascending=True)
        df['final_price'] = df['final_price'].fillna(method='ffill')
        df = df.dropna()
        df = df.reset_index()
        print(periodList)
        for i in periodList:
            df[f'{i}'] = (df['final_price']/df['final_price'].shift(int(i)))-1
        if onDate==False:
            df = df[df.index==df.index.max()]
        else:
            df = df[df['index']==int(onDate)]
            if len(df)==0:
                return json.dumps({'replay':False, 'msg':'اطلاعاتی موجود نیست'})
        dic = df.to_dict(orient='records')[0]
        del dic['index']
        del dic["final_price"]
        df = pd.DataFrame(dic.items(),columns=['period','ptp'])
        df['periodint'] = [365/int(x) for x in periodList]
        df['yearly'] = round(((((df['ptp']+1)**df['periodint'])-1)*100),2)
        df['ptp'] = [round((x*100),2) for x in df['ptp']]
        df['diff'] = df['yearly'] - float(target)
    else:
        df = pd.DataFrame(etf_db[f'{symbol}_collection'].find({},{ 'dateInt':1, '_id':0,'close_price_change_percent':1})).set_index('dateInt')
        day_list = Day_list()
        day_list =  filter(lambda x: x >= df.index.min(), day_list)
        day_list =  filter(lambda x: x <= df.index.max(), day_list)
        df = df.join(pd.DataFrame(index=day_list), how='right')
        df = df.sort_index(ascending=True)
        df = df.where(df>0,0)
        df['close_price_change_percent'] = (df['close_price_change_percent']/100)+1
        if onDate==False:
            df = df[df.index<=df.index.max()]
        else:
            df = df[df['index']<=int(onDate)]
            if len(df)==0:
                return json.dumps({'replay':False, 'msg':'اطلاعاتی موجود نیست'})
        df = df.reset_index()
        dic ={}
        for i in periodList:
            d = list(df[df.index>df.index.max()-i]['close_price_change_percent'])
            if len(d)==i:
                r = (np.prod(d))**(365/i)
                dic[f'{i}'] = [int((np.prod(d)-1)*10000)/100, np.nan, int((r-1)*10000)/100, round((int((r-1)*10000)/100)-target,2)]
            else:
                dic[f'{i}'] = [np.nan, np.nan, np.nan, np.nan]
        df = pd.DataFrame.from_dict(dic,orient='index')
        df = df.reset_index()
        df.columns = ['period','ptp','periodint','yearly','diff']
    
    print(df)

    return jsonify({'replay':True,'data':df.to_json(orient='records')})

def etf_reserve(username, fromDate, toDate, etfSelect):
    symbol = getSymbolOfUsername(username)
    dfbase = pd.DataFrame(etf_db[f'{symbol}_collection'].find({},{ 'dateInt':1, '_id':0,'date':1,'reserve':1}))
    if (fromDate==False) and (toDate==False):
        limitLen = True
    else:
        limitLen:False

    if(fromDate==False):
        fromDate = etf_db[f'{symbol}_collection'].find_one(sort=[("dateInt", 1)])['dateInt']
    if(toDate==False):
        toDate = etf_db[f'{symbol}_collection'].find_one(sort=[("dateInt", -1)])['dateInt']
    fromDate = int(fromDate)
    toDate =int(toDate)
    dfbase = dfbase[dfbase['dateInt']>=(fromDate)]
    dfbase = dfbase[dfbase['dateInt']<=(toDate)]
    dfbase = dfbase.set_index('dateInt')
    if len(dfbase)==0:
        return json.dumps({'replay':False, 'msg':'اطلاعاتی موجود نیست'})
    if(etfSelect==None):
        if limitLen:
            dfbase = (dfbase[-30:])
        
        AllStocks = list(farasahm_db['etflist'].find({'نماد':symbol},{'_id':0,'تعداد واحد':1}))[0]['تعداد واحد']

        dfbase['reserve'] = round(((dfbase['reserve'] / int(AllStocks))*100),0)
        return json.dumps({'replay':True,'data':{'base':True, 'basename':symbol, 'sub':False, 'df':dfbase.to_json(orient='records')}})
    else:
        dfsub = pd.DataFrame(etf_db[f'{etfSelect}_collection'].find({},{ 'dateInt':1, '_id':0,'date':1,'reserve':1}))
        dfsub = dfsub[dfsub['dateInt']>=(fromDate)]
        dfsub = dfsub[dfsub['dateInt']<=(toDate)]
        dfsub = dfsub.set_index('dateInt')
        AllStocks = list(farasahm_db['etflist'].find({'نماد':symbol},{'_id':0,'تعداد واحد':1}))[0]['تعداد واحد']
        dfbase['reserve'] = round(((dfbase['reserve'] / int(AllStocks))*100),0)
        AllStocks = list(farasahm_db['etflist'].find({'نماد':etfSelect},{'_id':0,'تعداد واحد':1}))[0]['تعداد واحد']
        dfsub['reserve'] = round(((dfsub['reserve'] / int(AllStocks))*100),0)
        df = dfbase.join(dfsub,lsuffix='B', rsuffix='S', how='left')
        if limitLen:
            df = (df[-30:])
        return json.dumps({'replay':True,'data':{'base':True, 'basename':symbol,'sub':True, 'subname':etfSelect, 'df':df.to_json(orient='records')}})



def etf_etflist(username):
    symbol = getSymbolOfUsername(username)
    etfList = [{'symbol':x['نماد'],'name':x['نام صندوق']} for x in farasahm_db['etflist'].find() if x['نماد'] != symbol]
    return json.dumps(etfList)


def etf_dashboard(username):
    symbol = getSymbolOfUsername(username)
    lastDay = etf_db[f'{symbol}_collection'].find_one(sort=[("dateInt", -1)])
    del lastDay['_id']
    return json.dumps(lastDay)

def etf_allreturn(username, onDate, etfSelect):
    periodList = [1,14,30,90,180,365]
    symbol = getSymbolOfUsername(username)
    psPeriod = list(farasahm_db['etflist'].find({'نماد':symbol}))[0][' دوره تقسیم سود ']
    etfSelect.append(symbol)
    etfSelect = list(set(etfSelect))
    dff = pd.DataFrame()
    for i in etfSelect:
        psPeriod = list(farasahm_db['etflist'].find({'نماد':i}))[0][' دوره تقسیم سود ']
        if psPeriod=='ندارد':
            df = pd.DataFrame(etf_db[f'{i}_collection'].find({},{ 'dateInt':1, '_id':0,'final_price':1})).set_index('dateInt')
            day_list = Day_list()
            day_list =  filter(lambda x: x >= df.index.min(), day_list)
            day_list =  filter(lambda x: x <= df.index.max(), day_list)
            df = df.join(pd.DataFrame(index=day_list), how='right')
            df = df.sort_index(ascending=True)
            df['final_price'] = df['final_price'].fillna(method='ffill')
            df = df.dropna()
            df = df.reset_index()

            for j in periodList:
                df[f'{j}'] = (df['final_price']/df['final_price'].shift(int(j)))-1
            if onDate==False:
                df = df[df.index==df.index.max()]
            else:
                df = df[df['index']==int(onDate)]
            if len(df)>0:
                dic = df.to_dict(orient='records')[0]
                del dic['index']
                del dic["final_price"]
                df = pd.DataFrame(dic.items(),columns=['period','ptp'])
                df['periodint'] = [365/int(x) for x in periodList]
                df['yearly'] = round(((((df['ptp']+1)**df['periodint'])-1)*100),2)
                df['ptp'] = [round((x*100),2) for x in df['ptp']]
                df = df[['period','yearly']]

                df = pd.pivot_table(df,columns='period')
                df.index = [i.replace(' ','')]
                dff = dff.append(df)
        else:
            df = pd.DataFrame(etf_db[f'{i}_collection'].find({},{ 'dateInt':1, '_id':0,'close_price_change_percent':1})).set_index('dateInt')
            day_list = Day_list()
            day_list =  filter(lambda x: x >= df.index.min(), day_list)
            day_list =  filter(lambda x: x <= df.index.max(), day_list)
            df = df.join(pd.DataFrame(index=day_list), how='right')
            df = df.sort_index(ascending=True)
            df = df.where(df>0,0)
            df['close_price_change_percent'] = (df['close_price_change_percent']/100)+1
            if onDate==False:
                df = df[df.index<=df.index.max()]
            else:
                df = df[df['index']<=int(onDate)]
                if len(df)==0:
                    return json.dumps({'replay':False, 'msg':'اطلاعاتی موجود نیست'})
            df = df.reset_index()
            dic ={}
            for j in periodList:
                d = list(df[df.index>df.index.max()-j]['close_price_change_percent'])
                if len(d)==j:
                    r = (np.prod(d))**(365/j)
                    dic[f'{j}'] = [int((np.prod(d)-1)*10000)/100, np.nan, int((r-1)*10000)/100]
                else:
                    dic[f'{j}'] = [np.nan, np.nan, np.nan]
            df = pd.DataFrame.from_dict(dic,orient='index')
            df = df.reset_index()
            df.columns = ['period','ptp','periodint','yearly']
            df = df[['period','yearly']]
            df = pd.pivot_table(df,columns='period')
            print(df)
            df.index = [i.replace(' ','')]
            dff = dff.append(df)

    if len(dff)==0:
        return json.dumps({'replay':False, 'msg':'اطلاعاتی موجود نیست'})
    else:
        dff = dff.reset_index()
        dff = dff.to_dict(orient='records')
        return json.dumps({'replay':True, 'data':dff})