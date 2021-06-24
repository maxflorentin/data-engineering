# -*- coding: utf-8 -*-
# !/usr/bin/python3
import os
import requests
import pandas as pd
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ZONDA_DIR = os.getenv('/Users/a309423')

url = "https://api.santander.ar.bsch" 
_client_id = 'c75a83a1-27ad-4fc8-b98f-44cd9032eb51'
_client_secret = 'sN7kO0rK6aJ5jU4jO1nB5yJ5oM7tE4fP6lP1jW7dO6fJ6jN7uO'
token = ''
jwttoken = ''

def getToken():

    global token
    global jwttoken
    
    payload = "grant_type=client_credentials&client_id={}&client_secret={}&scope=branches".format(_client_id, _client_secret)
    
    headers = {
        'content-type': "application/x-www-form-urlencoded",
        'accept': "application/json"
        }
    conn = requests.request("POST", url+"/santander/prod/oauth-provider/oauth2/token", data=payload, headers=headers, verify=False)
    data = conn.json()
    token = data["access_token"]
    jwttoken = data["jwt-token"]
    
    return token , jwttoken

getToken()

#print(token)
#print(jwttoken)

def getData(_client_id,token,jwttoken):
    
    headers = {
        'Accept': "application/json",
        'x-ibm-client-id': _client_id,
        'authorization': "Bearer "+token,
        'x-authorization': "Bearer "+jwttoken
        } 
    '''    
    conn = requests.request("GET", url+"/santander/prod/bus/api/branches?limit=1&offset=500", headers=headers, verify=False)
    data = conn.json()
    df = pd.json_normalize(data['results'])

    df.to_csv(r'/Users/a309423/sucursales.csv', index=False, header=True)
    #print("file: {} successfully generated".format(file_name))
    print('Listo sucus!')
    '''
    all_adress = []
    for idsucu in range(1,432):
        
        conn = requests.request("GET", url+"/santander/prod/bus/api/addresses/"+str(idsucu), headers=headers, verify=False)
        data = conn.json()
        all_adress.append(data)
        
    df = pd.json_normalize(all_adress)

    df.to_csv(r'/Users/a309423/sucursales_adress.csv', index=False, header=True)
        #print("file: {}successfully generated".format(file_name))
    print(df)

print(getData(_client_id, token, jwttoken))