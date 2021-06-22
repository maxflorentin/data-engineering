#!/usr/bin/python3

import os
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
import xmltodict
import time

password = str(os.environ["CREDENCIAL_TABLEAU"])


class ZondaTableauUpdateOperator(BaseOperator):
    template_fields = ['url', 'content_url']

    @apply_defaults
    def __init__(self, url, content_url, *args, **kwargs):
        """
        Constructor
        """
        super(ZondaTableauUpdateOperator, self).__init__(*args, **kwargs)
        self.url = url
        self.content_url = content_url

    def tableau_update(self):

        try:
            auth = '''<tsRequest> \
                 <credentials name="srvcengineerbi" password="{}" > \
                 <site contentUrl="{}" />  \
                 </credentials>  \
                 </tsRequest>'''.format(password, self.content_url)

            # tableau authetication
            auth = requests.post("http://tableau.santander.com.ar/api/3.5/auth/signin", data=auth)
            xmlAuth = auth.text
            print(xmlAuth)
            authDict = xmltodict.parse(xmlAuth)
            accessToken = authDict['tsResponse']['credentials']['@token']
            siteID = authDict['tsResponse']['credentials']['site']['@id']
            userID = authDict['tsResponse']['credentials']['user']['@id']
            print("Este es el siteId {}".format(siteID))
            # Trae el listado de extractos.
            print('refrescando extractos')
            body = '''<tsRequest> \
                 </tsRequest>'''
            Extracts = self.url
            getExtracts = Extracts.format(siteID)
            getExtractPost = requests.post(getExtracts, data=Extracts)
            headers = {"X-Tableau-Auth": "{}".format(accessToken), }
            extractXMLcode = requests.post(getExtracts, headers=headers, data=body).text
            extractos = xmltodict.parse(extractXMLcode)
            print((extractos))
            jobID = None
            while jobID == None:
                print('esperando...')
                time.sleep(20)

                jobID = extractos.get('tsResponse').get('job').get('@id')

            print(jobID)
            jobIDList = []
            jobIDList.append(jobID)
            print(jobIDList)

            # Trae info del job ejecutado-- El Job comenzó.
            print('El Job comenzó a correr %s' % jobID)
            Job = "http://tableau.santander.com.ar/api/3.5/sites/{0}/jobs/{1}"
            getJob = Job.format(siteID, jobID)
            print(getJob)
            getJobGet = requests.get(getJob, data=Job)
            headers = {"X-Tableau-Auth": "{}".format(accessToken), }
            Jobx = requests.get(getJob, headers=headers)
            JobXMLcode = requests.get(getJob, headers=headers).content
            print(JobXMLcode)
            jobs = xmltodict.parse(JobXMLcode)
            jobProgress = jobs.get('tsResponse').get('job').get('@progress')
            #jobs['tsResponse']['job']['@progress']
            # jobFinishCode =jobs['tsResponse']['job']['@finishCode']
            print('progreso %s' % jobProgress)
            # print('Terminó %s' %jobFinishCode)
            time.sleep(5)
            print('comienza el if')
            while jobProgress == '0':
                print('aun no terminó, corre de nuevo')
                time.sleep(10)
                Job = "http://tableau.santander.com.ar/api/3.5/sites/{0}/jobs/{1}"
                getJob = Job.format(siteID, jobID)
                getJobGet = requests.get(getJob, data=Job)
                headers = {"X-Tableau-Auth": "{}".format(accessToken), }
                Jobx = requests.get(getJob, headers=headers)
                JobXMLcode = requests.get(getJob, headers=headers).content
                jobs = xmltodict.parse(JobXMLcode)
                jobProgress = jobs.get('tsResponse').get('job').get('@progress')
                #jobs['tsResponse']['job']['@progress']
                # jobFinishCode =jobs['tsResponse']['job']['@finishCode']
                print('progreso %s' % jobProgress)
                # print( 'terminó bien %s' %jobFinishCode)
                if jobProgress == '100':
                    print('terminó.')
                    jobFinishCode = jobs.get('tsResponse').get('job').get('@finishCode')
                    #jobs['tsResponse']['job']['@finishCode']
                    print(jobFinishCode)

        except ValueError as e:
            print('error = %s' % e)

    def execute(self, context):
        self.tableau_update()

        """
        Execute ZondaTableauUpdateOperator Task
        añadir descripcion despues
        :param context:
        :return:
        """
