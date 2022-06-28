
import requests
import pandas as pd
import pymongo
import timeir
import numpy as np
import schedule
import time

client = pymongo.MongoClient()
farasahm_db = client['farasahm']
etf_db = client['etf']
token = '6e437430f8f55f9ba41f7a2cfea64d90'
url = 'https://sourcearena.ir/api/'

def PipeData():
    print('start')
    listEtfSymbol = [x['نماد'] for x in farasahm_db['etflist'].find({},{'نماد':1,"_id":0})]
    for i in listEtfSymbol:
        name = i
        print(name)
        history = pd.DataFrame(requests.get(url=url, params={'token':token, 'name':name,'days':365}).json())
        stockHolder  = requests.get(url=url, params={'token':token, 'stockholder':i,'change':'true'}).json()
        stockHolder = [x['possessions'] for x in stockHolder if 'رزرو' in x['name']]
        if len(stockHolder)>0:
            stockHolder = pd.DataFrame(stockHolder[0])
            stockHolder['date'] = [timeir.dateToStndr(x) for x in stockHolder['date']]
            stockHolder.columns=['date','reserve']
            history = history.set_index('date').join(stockHolder.set_index('date'),how='left')
        else:
            history = history.set_index('date')
            history['reserve'] = np.nan
        nav = requests.get(url=url, params={'token':token, 'nav':name}).json()['nav']

        history['dateInt'] = [int(x.replace('/','')) for x in history.index]
        history = history.sort_values(by=['dateInt'])
        history = history.reset_index()
        history['nav'] = 0
        history['nav'][history.index.max()] = nav
        history['reserve'] = history['reserve'].fillna(method='backfill')
        history['miss'] = [x==None for x in history['trade_volume']]
        history = history[history['miss']==False]
        history = history.drop(columns=['instance_code','miss'])
        for i in ['close_price','final_price','first_price','highest_price','lowest_price','trade_volume','trade_number','trade_value','reserve','dateInt','nav']:
            history[i] = [int(x) for x in history[i]]
        history['close_price_change_percent'] = [float(x.replace('%','')) for x in history['close_price_change_percent']]   
        collection = etf_db[f'{name}_collection']
        try:
            lastUpdate = max([x['dateInt'] for x in collection.find({},{'dateInt':1,'_id':0})])
            history = history[history['dateInt']>lastUpdate]
            if len(history)>0:
                history = history.to_dict(orient='records')
                collection.insert_many(history)
        except:
            history = history.to_dict(orient='records')
            collection.insert_many(history)

PipeData()
'''schedule.every().day.at("14:00").do(PipeData)

while True:
    schedule.run_pending()
    #time.sleep(60)'''