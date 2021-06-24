# -*- coding: utf-8 -*-
# !/usr/bin/python3
from __future__ import unicode_literals
import json
import requests
import urllib3
import os
import ssl

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def controlmoperator(job_name):
    # Parametria Control M
    baseurl = 'https://lxctrlmsrv01.ar.bsch:8443/automation-api/'
    username = os.getenv('SERVICE_USER')
    password = os.getenv('CREDENCIAL_TABLEAU')

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

    # Función que ejecuta y despliega un job sobre el servidor de Control M
    # Los parametros de entrada son el token de autenticación y el path del archivo con la definición del job
    # Devuelve JSON con estado de ejecución
    def runjob(token, file_path):
        authorization = json.loads('{"Authorization": "Bearer ' + token + '"}')
        file = [('jobDefinitionsFile', ('Jobs.json', open(file_path, 'rb'), 'application/json'))]
        request = requests.post(baseurl + '/run', files=file, headers=authorization, verify=False)
        response = json.loads(request.text)

        return response

    # Función que ejecuta un job desplegado sobre el servidor de Control M
    # Los parametros de entrada son el token de autenticación, el servidor de control m, la carpeta que contiene el job y el jobid
    # Devuelve JSON con estado de ejecución
    def runorder(token, controlm_server, folder, jobid):
        authorization = json.loads('{"Authorization": "Bearer ' + token + '"}')
        runorderurl = baseurl + 'run/order/'
        body = json.loads('{"ctm":"' + controlm_server + '","folder":"' + folder + '","jobs":"' + jobid + '"}')
        print(runorderurl)
        request = requests.post(runorderurl, headers=authorization, json=body, verify=False)
        response = json.loads(request.text)

        return response

    # Función que ejecuta jobs de control m que fueron holdeados o programados para ejecutar con posterioridad
    # Los parametros de entrada son el token de autenticación y el id del job en cuestión
    # Devuelve JSON con el resultado de la operación
    def runjobnow(token, job_id):
        authorization = json.loads('{"Authorization": "Bearer ' + token + '"}')
        jobstatusurl = baseurl + 'run/job/' + job_id + '/runNow'
        print(jobstatusurl)
        request = requests.post(jobstatusurl, headers=authorization, verify=False)
        response = json.loads(request.text)

        return response

    # Función utilizada para ejecutar nuevamente un job de control m
    # Los parametros de entrada son el token de autenticación y el id del job en cuestión
    # Devuelve JSON con el resultado de la operación
    def rerunjob(token, job_id):
        authorization = json.loads('{"Authorization": "Bearer ' + token + '"}')
        body = {
            "cleanup": True,
            "recaptureAbend": "",
            "recaptureConditionCode": "",
            "stepAdjustment": True,
            "restartParmMemberName": ""
        }

        jobstatusurl = baseurl + 'run/job/' + job_id + '/rerun'
        print(jobstatusurl)
        request = requests.post(jobstatusurl, headers=authorization, json=body, verify=False)
        response = json.loads(request.text)

        return response

    # Función utilizada para buscar el estado de ejecución de un job
    # Los parametros de entrada son el token de autenticación y el id del job en cuestión
    # Devuelve JSON con el resultado de la operación
    def getjobstatus(token, job_name):
        authorization = json.loads('{"Authorization": "Bearer ' + token + '"}')
        jobstatusurl = baseurl + 'run/jobs/status?limit=1&jobname=' + job_name
        request = requests.get(jobstatusurl, headers=authorization, verify=False)
        response = json.loads(request.text)

        return response

    # Función utilizada para recuperar el log de un job
    # Los parametros de entrada son el token de autenticación, el id del job en cuestión y el numero de ejecucion
    # Devuelve JSON con el resultado de la operación
    def getjoblog(token, job_id, runNo):
        authorization = json.loads('{"Authorization": "Bearer ' + token + '"}')
        jobstatusurl = baseurl + 'run/job/' + job_id + '/log?runNo=' + runNo
        print(jobstatusurl)
        request = requests.get(jobstatusurl, headers=authorization, verify=False)
        response = request.text

        return response

    # Función utilizada para recuperar el output de un job
    # Los parametros de entrada son el token de autenticación, el id del job en cuestión y el numero de ejecucion
    # Devuelve JSON con el resultado de la operación
    def getjoboutput(token, job_id, runNo):
        authorization = json.loads('{"Authorization": "Bearer ' + token + '"}')
        jobstatusurl = baseurl + 'run/job/' + job_id + '/output?runNo=' + runNo
        print(jobstatusurl)
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

    def free(token, job_id):
        authorization = json.loads('{"Authorization": "Bearer ' + token + '"}')
        jobstatusurl = baseurl + 'run/job/' + job_id + '/free'
        print(jobstatusurl)
        request = requests.post(jobstatusurl, headers=authorization, verify=False)
        response = json.loads(request.text)

        return response

    def confirm(token, job_id):
        authorization = json.loads('{"Authorization": "Bearer ' + token + '"}')
        jobstatusurl = baseurl + 'run/job/' + job_id + '/confirm'
        print(jobstatusurl)
        request = requests.post(jobstatusurl, headers=authorization, verify=False)
        response = json.loads(request.text)

        return response

    try:

        token = login(baseurl, username, password)
        print('Successfully login')

        try:
            status = getjobstatus(token, job_name)
            job_id = status["statuses"][0]["jobId"]
            print('Job to execute: ' + job_name)
            print('Job ID: ' + job_id)

            status = status["statuses"][0]["status"]
            print('Last Status: '+status)

            if status in ['Wait User', 'Wait Confirm']:
                confirm = confirm(token, job_id)
                print(confirm)

            if status in ['Ended OK', 'Ended Not OK']:
                rerunjob = rerunjob(token, job_id)
                print(rerunjob)

            if status in ['Wait Condition', 'Wait Resource', 'Wait Submission']:
                runjobnow = runjobnow(token, job_id)
                print(runjobnow)

        except:
            print("Job "+job_name+" Not Found")

        logout(token)
        print('Successfully logged out')

    except ValueError as e:
        print('error = %s' % e)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--job_name', metavar='job_name', required=True, help='job_name')
    args = parser.parse_args()

    controlmoperator(job_name=args.job_name)
