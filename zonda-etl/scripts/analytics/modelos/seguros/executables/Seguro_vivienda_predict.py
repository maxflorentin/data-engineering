import pandas as pd
import joblib 
import numpy as np
import json
import sklearn
import Transformers2
import sys    
import os  

from UtilConexiones import conectar_oracle, consulta_base_to_pandas
import yaml


def main():

  import logging
  script_name =  'log_' + os.path.basename(sys.argv[0][0:-3])
  logging.basicConfig(filename = script_name + '.log', level = logging.INFO,
                      format = '%(asctime)s:%(levelname)s:%(message)s')

  logging.info('Comienzo de ejecucion'.format())

  #------CREAR UNA SESIÓN DE SPARK-----------
  from pyspark import SparkContext, SQLContext , SparkConf
  from pyspark.sql import SparkSession, Row, HiveContext

  with open("/santander/bi-corp/sandbox/bi/config/seguros/yml/config_file_predict_vivienda.yaml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)
  logging.info('Se leyo archivo de configuracion'.format())
  
  config = SparkConf().set('spark.kryoserializer.buffer.max',200).set('spark.executor.memory', '8g')
  sc = SparkContext(master=cfg['spk_master'],conf=config).getOrCreate()
  spark = SparkSession.builder.enableHiveSupport().getOrCreate()

  logging.info('Se conecto a spark'.format())

  fd = open(cfg['query_raw_data'], 'r')
  query_raw_data = fd.read()
  fd.close()

  df = spark.sql(query_raw_data).toPandas()


  #------SEPARAR IDs
  col_id = ['ANIOMES', 'NUP']
  df_id = df.loc[:, col_id].copy()
  df = df.drop(col_id, axis=1)

  def meses_a_aniomes(meses):
    anio = int(np.floor((meses-1)/12))
    mes = meses - (anio * 12)
    return anio*100+mes

  df_id.loc[:,'ANIOMES'] = df_id['ANIOMES'].apply(lambda x:  meses_a_aniomes((int(np.floor(x /100)) * 12 + (x%100) ) +2))    
  
  #-----CARGA PIPELINE----
  pre_proc_pipeline = joblib.load(cfg['pipeline'])
  logging.info('Se cargo el pipeline')

  #-----CARGA MODELO REGRESION LOGISTICA---
  model = joblib.load(cfg['model'])
  logging.info('Se cargo el modelo')

  #-----PRE-PROCESAMIENTO DE DATOS
  logging.info('Comienzo de pre-procesamiento')

  df = pre_proc_pipeline.transform(df)

  logging.info('Fin de pre-procesamiento')

  #-----PREDICT
  logging.info('Comienzo de predict')

  model_cols = joblib.load(cfg['model_cols'])

  preds = model.predict_proba(df[model_cols])[:, 1]
  preds = pd.Series(data=preds,name='prediccion').round(5)

  logging.info('Fin de predict')

  #-----UNIR IDs CON PREDICCIONES
  df_pred = pd.concat([df_id, preds], axis=1)

  #----ESCRIBIR LA SALIDA EN SPARK-----
  df = spark.createDataFrame(df_pred[['NUP','prediccion','ANIOMES']])


  #------------------------------------
  logging.info('Comienzo de carga de resultados en hive')
  
  #------------------ esto 
  table_name = cfg['db_name'] + '.' + cfg['tbl_name'] 
  df.write.partitionBy('ANIOMES').saveAsTable(name=table_name,format='parquet',mode='append')
  
  logging.info('Fin de carga de resultados en spark')
  
  

  logging.info('Proceso terminado')


if __name__== "__main__":
  main()
    

