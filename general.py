import pandas as pd
import requests


def ClearDf(df,columns):
    dff = df
    for c in columns:
        dff[c] = [float(str(x).replace(',','')) for x in dff[c]]
    return dff


def getLivePriceSymbol(symbol):
    url = 'https://sourcearena.ir/api/'
    token = '6e437430f8f55f9ba41f7a2cfea64d90'
    name = symbol.replace('1','')
    data = requests.get(url=url, params={'token':token,'name':name}).json()
    return data['final_price']


def getAllSymboll():
    url = 'https://sourcearena.ir/api/?token=6e437430f8f55f9ba41f7a2cfea64d90&all&type=0'
    data = requests.get(url=url).json()
    data = [x['name'] for x in data]
    return data

def getPriceAllSymbol():
    url = 'https://sourcearena.ir/api/?token=6e437430f8f55f9ba41f7a2cfea64d90&all&type=0'
    data = requests.get(url=url).json()
    df = pd.DataFrame(data)[['name','final_price']]
    df['name'] = df['name']+'1'
    df = df.set_index('name')
    return df