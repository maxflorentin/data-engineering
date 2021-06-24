import os

import pandas as pd
import json
from datetime import date
import datetime


from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession, Row, HiveContext, functions
import argparse

ZONDA_DIR = os.getenv('ZONDA_DIR')

parser = argparse.ArgumentParser(description='Procesa predicciones')
parser.add_argument('-f','--fecha',required = True,type = str,
                    help='Fecha')

args = parser.parse_args()
fecha = args.fecha

# Consulta los 3 dias anteriores
ano = int(fecha[:4])
mes = int(fecha[5:7])
dia = int(fecha[8:])
tres_dias = datetime.timedelta(days=3)
fecha = datetime.date(ano,mes,dia)
fecha = fecha - tres_dias
fecha = fecha.strftime("%Y-%m-%d")


print(fecha)
sc = SparkContext(master='yarn-client').getOrCreate()
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

df = spark.sql("""
select * from analytics.watson_intent_clasificados
where partition_date >= date_format('{}', 'yyyy-MM-dd')
""".format(fecha))

df = df.toPandas()
print('[INFO]: Cantidad de registros: ',df.shape[0])
df['confidence_watson'] = df.confidence_watson.astype(float)
df = df.drop_duplicates(subset= ['message'])

df['Acierto'] = ['SI' if (c_conf > 0.86) & (w_conf > 0.82) & (coin == 'SI') else 'NO'
                 for c_conf, w_conf, coin in zip(df.confidence_clasificador,df.confidence_watson,df.coinciden)]

df['Error'] = ['SI' if  (w_conf < 0.39) & (coin == 'NO') else 'NO'
               for w_conf, coin in zip(df.confidence_watson,df.coinciden)]

df['tamaño_mensaje'] = df.message.apply(lambda x: len(str(x)))fass

today = date.today()
day = today.strftime("%Y%m%d")
filename = 'predicciones_watson_'+ day + '.csv'
path = ZONDA_DIR + '/temp/'
df.to_csv(path+filename, index= False, sep = '|', encoding='utf-16')
print('[INFO]: Proceso finalizado')