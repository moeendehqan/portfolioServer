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
    df['deffNav'] = df['final_price'] - df['nav']
    df['RatedeffNav'] = (round(((df['final_price'] / df['nav'])-1)*10000))/100
    df['RatedeffNav'] = df['RatedeffNav'].replace(np.inf,0)
    df = df.sort_values(by=['dateInt'],ascending=False)
    df = df[['date','final_price','close_price_change_percent','nav','deffNav','RatedeffNav','dateInt']]
    df = df.to_json(orient='records')
    return df

def etf_volume(username,fromDate,toDate):
    symbol = getSymbolOfUsername(username)
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
    return df.to_json(orient='records')


def etf_return(username,onDate,target):
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
        print(df)
        df['1'] = (df['final_price']/df['final_price'].shift(1))-1
        df['7'] = (df['final_price']/df['final_price'].shift(7))-1
        df['14'] = (df['final_price']/df['final_price'].shift(14))-1
        df['30'] = (df['final_price']/df['final_price'].shift(30))-1
        df['60'] = (df['final_price']/df['final_price'].shift(60))-1
        df['180'] = (df['final_price']/df['final_price'].shift(180))-1
        df['365'] = (df['final_price']/df['final_price'].shift(365))-1
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
        df['periodint'] = [365/1,365/7,365/14,365/30,365/60,365/180,365/365]
        df['yearly'] = round(((((df['ptp']+1)**df['periodint'])-1)*100),2)
        df['ptp'] = [round((x*100),2) for x in df['ptp']]
        df['diff'] = df['yearly'] - int(target)
    return jsonify({'replay':True,'data':df.to_json(orient='records')})
