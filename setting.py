import json
import pandas as pd
import pymongo

client = pymongo.MongoClient()
farasahmDb = client['farasahm']
userCl = farasahmDb['user']

def dataaccount(username):
    user = list(userCl.find({'username':username},{'_id':0}))[0]

    subaccountlist = list(userCl.find({'mainaccount':username},{'_id':0}))
    subaccountlist = [{'username':x['username'], 'fullName':x['fullName'], 'LogoAddres':x['LogoAddres']} for x in subaccountlist]
    return json.dumps({'sublist':subaccountlist, 'sub':user['subaccount']})