import numpy as np
import pymongo
import json
import pandas as pd


client = pymongo.MongoClient()
etf_db = client['etf']
farasahm_db = client['farasahm']

def getSymbolOfUsername(userName):
    symbol = farasahm_db['user'].find_one({'username':userName})
    if len(symbol)>0:
        return symbol['etfSymbol']
    else:
        return False

def etf_nav(username):
    symbol = getSymbolOfUsername(username)
    df = pd.DataFrame(etf_db[f'{symbol}_collection'].find())
    df = df.to_dict(orient='records')
    return json.dumps({'o':df})
