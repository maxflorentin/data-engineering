# -*- coding: utf-8 -*-
# !/usr/bin/python3

import os
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
import json
import urllib3


class ZondaControlmOperator(BaseOperator):
    template_fields = ['job_name']

    @apply_defaults
    def __init__(self, job_name, *args, **kwargs):
        """
        Constructor
        """
        super(ZondaControlmOperator, self).__init__(*args, **kwargs)
        self.job_name = job_name

    def controlmoperator(self, job_name):

        # Parametria Control M
        baseurl = 'https://lxctrlmsrv01.ar.bsch:8443/automation-api/'
        username = 'srvcengineerbides'
        password = 'Kl4b3b5r102k19'

        proxies = {
            "http": "http://proxy.ar.bsch:8080",
            "https": "http://proxy.ar.bsch:8080",
        }

        # Función que autentica contra servidor de Control M
        # Los parametros de entrada son la URL del servidor, el usuario y el password
        # Devuelve Token que debe ser utilizado para consultar el resto de los endpoints
        def login(baseurl, username, password, q):
            loginurl = baseurl + 'session/login'
            body = {"password": password, "username": username}
            request = requests.post(loginurl, json=body, verify=False)
            response = json.loads(request.text)
            print(response)

            token = ''

            try:
                token = response['token']
            except:
                token = 'Login Failed'

            q.put(token)

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
            print(jobstatusurl)
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

            import multiprocessing as mp

            q = mp.Queue()
            args = (baseurl, username, password, q)
            mp.Process(target=login, args=args).start()

            token = q.get()
            print(token)

        except ValueError as e:
            print('error = %s' % e)

    def execute(self, context):
        self.controlmoperator(self.job_name)

        """
        Execute controlmoperator Task
        Integracion de zonda para la ejecucion de jobs de control m
        :param context:
        :return:
        """

