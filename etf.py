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

def etf_nav(username,fromDate,toDate):
    symbol = getSymbolOfUsername(username)
    if(fromDate==False):
        fromDate = etf_db[f'{symbol}_collection'].find_one(sort=[("Date", -1)])['Date']
    if(toDate==False):
        toDate = etf_db[f'{symbol}_collection'].find_one(sort=[("Date", -1)])['Date']
    df = pd.DataFrame(etf_db[f'{symbol}_collection'].find())
    df['deffNav'] = df['final_price'] - df['nav']
    df['RatedeffNav'] = (round(((df['final_price'] / df['nav'])-1)*10000))/100
    df['RatedeffNav'] = df['RatedeffNav'].replace(np.inf,0)
    df = df.sort_values(by=['dateInt'],ascending=False)
    df = df[['date','final_price','close_price_change_percent','nav','deffNav','RatedeffNav','dateInt']]
    df = df.to_json(orient='records')
    return df
