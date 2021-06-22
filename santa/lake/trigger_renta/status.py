# -*- coding: utf-8 -*-
# !/usr/bin/python3
from __future__ import unicode_literals
import json
import requests
import urllib3
import os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def controlmstatus(job_name):
    # Parametria Control M
    baseurl = 'https://lxctrlmsrv01.ar.bsch:8443/automation-api/'
    username = os.getenv('USER')
    password = os.getenv('CREDENCIAL_TABLEAU')
    job_status = ''

    # Función que autentica contra servidor de Control M
    # Los parametros de entrada son la URL del servidor, el usuario y el password
    # Devuelve Token que debe ser utilizado para consultar el resto de los endpoints
    def login(baseurl, username, password):
        loginurl = baseurl + 'session/login'
        body = json.loads('{ "password": "' + password + '", "username": "' + username + '"}')
        request = requests.post(loginurl, json=body, verify=False)
        response = json.loads(request.text)

        token = ''

        try:
            token = response['token']
        except:
            token = 'Login Failed'

        return token

    # Función utilizada para buscar el estado de ejecución de un job
    # Los parametros de entrada son el token de autenticación y el id del job en cuestión
    # Devuelve JSON con el resultado de la operación
    def getjobstatus(token, job_name):
        authorization = json.loads('{"Authorization": "Bearer ' + token + '"}')
        jobstatusurl = baseurl + 'run/jobs/status?limit=1&jobname=' + job_name
        request = requests.get(jobstatusurl, headers=authorization, verify=False)
        response = json.loads(request.text)

        return response
        
    # Función utilizada para terminar la sesion en controlm
    # Se requiere el token de autenticacion como parametro
    # Devuelve JSON con el resultado de la operacion
    def logout(token):
        logouturl = baseurl + 'session/logout'
        authorization = json.loads('{"Authorization": "Bearer ' + token + '"}')
        request = requests.post(logouturl, headers=authorization, verify=False)
        response = json.loads(request.text)

        return response
    
    try:
        token = login(baseurl, username, password)

        try:
            status = getjobstatus(token, job_name)
            # date_status = status["statuses"][0]["status"]
            job_status = status["statuses"]  # [0]["status"]
            print(status)
            # if job_status == 'Ended OK':
            #     return True
            # else:
            #     raise Exception('status not Ended OK') 
        
        except ValueError:
            print("Job not ended OK or Waiting conditions")

        logout(token)       

    except ValueError as e:
        print('error = %s' % e)  
        
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--job_name', metavar='job_name', required=True, help='job_name')
    args = parser.parse_args()

    controlmstatus(job_name=args.job_name)
