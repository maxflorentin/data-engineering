# -*- coding: utf-8 -*-
# !/usr/bin/python3

import os
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
import json
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

baseurl = "https://platform-dev-zonda-zagw.apps.ocp.ar.bsch/api/v1/"

class ZondaEventsOperator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(ZondaEventsOperator, self).__init__(*args, **kwargs)
        
        self.text = kwargs.get('text')
        self.conf = kwargs.get('conf')
        self.display_conf = True
                
        
    def get_zonda_platform_token():
        
        url = baseurl+"auth/token"
        payload = {}
        headers = {
            'Authorization': 'Basic YTMwOTQyMzpDYW1pMjI2Ng=='
            }

        request = requests.request("GET", url, headers=headers, data=payload, verify=False)
        response = json.loads(request.text)
        token = ''
        
        try:
            token = response['token']
        except:
            token = 'Login Failed'
        
        print(token)
    
    def __repr__(self):
        """
        Object string representation
        :return: str representation of this object
        """
        return str(self)

print(ZondaEventsOperator.get_zonda_platform_token())