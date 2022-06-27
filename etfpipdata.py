from gc import collect
import requests
import pandas as pd
import pymongo
import timeir

client = pymongo.MongoClient()
farasahm_db = client['farasahm']
token = '6e437430f8f55f9ba41f7a2cfea64d90'
url = 'https://sourcearena.ir/api/'

listEtfSymbol = [x['نماد'] for x in farasahm_db['etflist'].find({},{'نماد':1,"_id":0})]
for i in listEtfSymbol[:1]:
    print(i)
    history = pd.DataFrame(requests.get(url=url, params={'token':token, 'name':i,'days':10}).json())
    stockHolder  = requests.get(url=url, params={'token':token, 'stockholder':i,'change':'true'}).json()
    stockHolder = [x['possessions'] for x in stockHolder if 'رزرو' in x['name']]
    stockHolder = pd.DataFrame(stockHolder[0])
    print(history)
    print(stockHolder)


