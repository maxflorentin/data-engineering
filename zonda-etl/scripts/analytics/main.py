# coding=utf-8
import UtilStruct
from UtilTextPreProcesing import Documentos
from UtilEmbedding import MeanEmbeddingVectorizer1
import pandas as pd
from gensim.models import Word2Vec
import pickle
from joblib import dump, load
import numpy as np
import sys
import yaml
import json
import os

stop_words = UtilStruct.STOP_WORDS + UtilStruct.STOP_WORDS_AD_HOC
dicc_tarjeta = UtilStruct.dicc_tarjeta
dicc_prestamo = UtilStruct.dicc_prestamo
dicc_debito = UtilStruct.dicc_debito
dicc_credito = UtilStruct.dicc_credito
dicc_transferencia = UtilStruct.dicc_transferencia
dicc_consulta = UtilStruct.dicc_consulta
dicc_numero = UtilStruct.dicc_numero
dicc_extraccion = UtilStruct.dicc_extraccion
dicc_problema = UtilStruct.dicc_problema
dicc_renovacion = UtilStruct.dicc_renovacion

ZONDA_DIR = os.getenv('ZONDA_DIR')

def main(fecha):
    # *****************************************
    # -------------CONFIGURACION-----------
    # *****************************************

    with open(ZONDA_DIR+"/repositories/zonda-etl/scripts/analytics/conf.yaml", 'r') as ymlfile:
        conf = yaml.load(ymlfile)

    # *****************************************
    # ------CREAR UNA SESION DE SPARK-----------
    # *****************************************

    from pyspark import SparkContext, SQLContext
    from pyspark.sql import SparkSession, Row, HiveContext

    sc = SparkContext(master='yarn-client').getOrCreate()
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    # *****************************************
    # -------------QUERY-----------------------
    # *****************************************

    fd = open(conf['query_input'], 'r')
    query = str(fd.read())
    fd.close()
    query = str(query).format(str(fecha))

    # *****************************************
    # -------CONSULTA MOVIMIENTOS DE LA FECHA------
    # *****************************************

    df = spark.sql(query)
    print(df.count())

    from pyspark.sql import Window
    from pyspark.sql import Row, functions as F

    split_col = F.split(df['intent'], '_')
    df = df.withColumn('tipo', split_col.getItem(0))

    w = Window.partitionBy('idgenesys').orderBy('fechahora', F.col('emisortipo').desc())
    df = df.drop_duplicates()
    df = df.withColumn("prev_message", F.lag(df.message).over(w))
    df = df.withColumn("prev_emisortipo", F.lag(df.emisortipo).over(w))
    df = df.where((df.emisortipo == 'AGENT')
                  & (df.prev_message.isNotNull())
                  & (df.intent != 'null')
                  & (df.prev_emisortipo == 'CLIENT')
                  & (df.options == '[]')
                  & (df.tipo != 'CHIT')
                  &(df.fuepossiblequestions == 'False')
            	  &(df.fuesuggestion_topics == 'False')
             	  &(df.fueoptions == 'False'))
    df = df.select('idgenesys', 'fechahora', 'prev_message', 'intent', 'confidence')

    df_test = df.toPandas()

    print('[INFO]: Los datos fueron cargados')
    print(df_test.shape[0])

    # *****************************************
    # -----CARGA WORD EMBEDDING ENTRENADO----
    # *****************************************
    word_model = Word2Vec.load(ZONDA_DIR + "/repositories/zonda-etl/scripts/analytics/word2vec1703_400D.model")

    # *****************************************
    # -----CARGA MODELO REGRESION LOGISTICA---
    # *****************************************
    model = load(ZONDA_DIR + '/repositories/zonda-etl/scripts/analytics/model_RLog_1703.joblib')

    # *****************************************
    # ------GENTERA LOS DOCUMENTOS--------
    # *****************************************
    text_test = Documentos(df_test.prev_message, stop_words=stop_words)

    # *****************************************
    # -----CORREGIR PALABRAS MAL ESCRITO----
    # *****************************************
    docs_corregidos = []
    for doc in text_test.docs:
        docs_corregidos.append([UtilStruct.corregir(word) for word in doc])
    text_test.docs = docs_corregidos

    print('[INFO]: Calculando promedio de vectores...')
    # **********************************************************************
    # -----CALCULA EL PROMEDIO DE LOS VECTORES DE PALABRAS POR DOCUMENTO-----
    # *********************************************************************
    mean_vec_tr = MeanEmbeddingVectorizer1(word_model)
    doc_test = mean_vec_tr.transform(list(text_test.docs))
    X2pred = pd.DataFrame(doc_test)

    # *****************************************
    # ------PALABRAS DESCONOCIDAS---------------
    # *****************************************
    """with open('not_in_embedding.json') as f:
    data = json.load(f)

  data[fecha] = mean_vec_tr.not_in_embedding

  with open('not_in_embedding.json', 'w') as f:
    json.dump(data, f)"""

    print('[INFO]: Realizando predicciones...')
    # *****************************************
    # ---------- PREDICCIONES------------------
    # *****************************************
    y_pred_proba_test = model.predict_proba(X2pred)
    dict_intents = {k: v for k, v in enumerate(model.classes_)}
    df_test['intent_pred'] = np.apply_along_axis(np.argmax, 1, y_pred_proba_test)
    df_test['intent_pred'] = df_test.intent_pred.map(dict_intents)
    df_test['text_preds'] = np.apply_along_axis(np.max, 1, y_pred_proba_test)
    df_test['coinciden'] = ['SI' if x == y else 'NO' for x, y in zip(df_test.intent, df_test.intent_pred)]
    df_test.columns = ['idgenesys', 'partition_date', 'message', 'intent_watson', 'confidence_watson',
                       'intent_clasificador', 'confidence_clasificador', 'coinciden']

    df_test = df_test[['idgenesys', 'message', 'intent_watson', 'confidence_watson',
                       'intent_clasificador', 'confidence_clasificador', 'coinciden', 'partition_date']]

    # Es necesario transformar el campo fecha hora a strig que va a ser usado
    # como particion
    df_test['partition_date'] = df_test.partition_date.apply(lambda x: str(x)[0:10])

    print('[INFO]: Escribiendo Resultados...')
    # *****************************************
    # ----ESCRIBIR LA SALIDA EN SPARK---------
    # *****************************************
    df = spark.createDataFrame(df_test)
    df.createOrReplaceTempView("mytempTable")

    # *****************************************
    # ------CREACION DE LA TABLA---------------
    # *****************************************
    """spark.sql(create table analytics.watson_intent_clasificados_prueba
  (
  idgenesys string,
  message string, 
  intent_watson string, 
  confidence_watson string, 
  intent_clasificador string, 
  confidence_clasificador double, 
  coinciden string) 
  partitioned by (partition_date string) STORED AS PARQUET TBLPROPERTIES ('parquet.compression'='SNAPPY'))"""

    # ------------------------------------

    fd = open(conf['query_output'], 'r')
    query = str(fd.read())
    fd.close()

    spark.sql(" SET hive.exec.dynamic.partition.mode=nonstrict ")
    spark.sql(query)

    print('[INFO]: Proceso Terminado')


if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(sys.argv[1])


