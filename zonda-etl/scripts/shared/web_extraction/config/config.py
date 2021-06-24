from os import environ as env
import json


class ProductionConfig:
    PROXYS = json.loads( env.get( 'PROXYS',
                                                 """{
        "http": "http://proxy.ar.bsch:8080",
        "https": "http://proxy.ar.bsch:8080"
    }""" ) )
