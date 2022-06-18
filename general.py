import pandas as pd
import requests
import codecs

def ClearDf(df,columns):
    dff = df
    for c in columns:
        dff[c] = [float(str(x).replace(',','')) for x in dff[c]]
    return dff

def xmlToDfTrade(p):
    try:
        l = []
        f = codecs.open(p,'r','UTF-16')
        for i in f:
            row = i.replace('<item>','').replace('</item>','')
            row = row.split('> <')
            rowful = []
            for r in row:
                fild = r.split('>')[1].split('<')[0]
                rowful.append(fild)
            l.append(rowful)
        df = pd.DataFrame(columns=['Symbol','Volume','Price','Buy_brkr','Sel_brkr','Ticket_no','Cancel','B_account','S_account','Date','Time'],data=l)
    except:
        df = pd.DataFrame(columns=['Symbol','Volume','Price','Buy_brkr','Sel_brkr','Ticket_no','Cancel','B_account','S_account','Date','Time'],data=[])
    return df

def xmlToDfRegister(p):
    try:
        l = []
        f = codecs.open(p,'r','UTF-16')
        for i in f:
            row = i.replace('<item>','').replace('</item>','')
            row = row.split('> <')
            rowful = []
            for r in row:
                fild = r.split('>')[1].split('<')[0]
                rowful.append(fild)
            l.append(rowful)
        df = pd.DataFrame(columns=['Account','Fullname','Ispl','Isno','Father','Type','NationalId','Birthday','Serial','Firstname','Lastname'],data=l)

    except:
        df = pd.DataFrame(columns=['Account','Fullname','Ispl','Isno','Father','Type','NationalId','Birthday','Serial','Firstname','Lastname'],data=[])
    return df

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
