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

def edtiGetData(username):
    user = list(userCl.find({'username':username},{'_id':0}))
    print(user)
    return json.dumps(user)

def add(main,sub,name,password):
    usersub = list(userCl.find({'username':sub}))
    if len(usersub)>0:
        return json.dumps({'replay':False,'msg':'نام کاربری تکراری است'})
    usersub = list(userCl.find({'username':main},{'_id':0}))[0]
    usersub['username'] = sub
    usersub['password'] = password
    usersub['fullName'] = name
    usersub['fullName'] = name
    usersub['setting'] = False
    usersub['mainaccount'] = main
    usersub['subaccount'] = 0
    userCl.insert_one(usersub)
    return json.dumps({'replay':True})

def edit(sub,name,password):
    userCl.update_one({'username':sub},{'$set':{'fullName':name,'password':password}})
    return json.dumps({'replay':True})

def acc(username):
    user = userCl.find_one({'username':username},{'_id':0})
    print(user['saportfolioupdate'])
    return json.dumps(user)
    
def change(data):
    userCl.delete_one({'username':data['username']})
    userCl.insert_one(data)
    return json.dumps({'replay':True})