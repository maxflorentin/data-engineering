import pandas as pd
import numpy as np
import joblib
import time
from Busqueda_binaria import *
import argparse


def parse_arguments():
    ap = argparse.ArgumentParser()

    ap.add_argument("-f", "--fecha", required=True,
                    help="fecha de ejecucion en formato YYYY-MM-dd usado para filtrar la informacion")

    return ap.parse_args()


def main(args):
    # import logging
    # logging.basicConfig(filename = 'seguros_generacion_archivos.log', level = logging.INFO,
    #                  format = '%(asctime)s:%(levelname)s:%(message)s')

    # logging.info('Comienzo de ejecucion'.format())

    fecha = args.fecha

    def seteo_fechas(fecha_ejecucion):
        import datetime
        from dateutil.relativedelta import relativedelta

        one_month = relativedelta(months=1)
        one_day = relativedelta(days=1)

        dic_resultados = {}

        fecha_ejec = datetime.date(int(fecha_ejecucion[0:4]), int(fecha_ejecucion[5:7]), int(fecha_ejecucion[8:]))
        data_date_part = datetime.date(int(fecha_ejecucion[0:4]), int(fecha_ejecucion[5:7]), 1)
        dic_resultados['fecha_prediccion'] = str(data_date_part + (one_month))
        dic_resultados['aniomes_prediccion'] = dic_resultados['fecha_prediccion'][0:4] + dic_resultados[
                                                                                             'fecha_prediccion'][5:7]
        return dic_resultados

    # ------CREAR UNA SESIÓN DE SPARK-----------
    from pyspark import SparkContext, SQLContext, SparkConf
    from pyspark.sql import SparkSession, Row, HiveContext

    dic_resultados = seteo_fechas(fecha)

    print(dic_resultados['fecha_prediccion'])
    print(dic_resultados['aniomes_prediccion'])

    config = SparkConf().set('spark.kryoserializer.buffer.max', 200).set('spark.executor.memory', '8g')
    sc = SparkContext(master='yarn-client', conf=config).getOrCreate()
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    df = spark.sql('''
        select * from analytics.mm_predict_seguros_vivienda
        where aniomes = {}
        '''.format(dic_resultados['aniomes_prediccion'])).toPandas()

    df.NUP = df.NUP.apply(lambda x: str(x).rjust(8, '0'))
    df.prediccion = df.prediccion.apply(lambda x: round(x, 6))

    name = "/aplicaciones/bi/zonda/repositories/zonda-etl/scripts/analytics/modelos/seguros/scores_vivienda_validacion_rangos.pkl"
    df_rangos = joblib.load(name)
    df_rangos.columns = ['cuantil', 'min', 'max']

    l = []
    l = df_rangos['min'].to_list() + df_rangos['max'].to_list()
    l = list(set(l))
    l.sort()

    tree = NumArray(l)

    df['cuantil'] = df.prediccion.apply(lambda x: abs(asigna_cuantil_tree(x, tree) - 18))

    df_prop_vivienda = df[['NUP', 'ANIOMES', 'cuantil']].copy()

    del df

    df = spark.sql('''
        select * from analytics.mm_predict_seguros_auto
        where aniomes = {}
        '''.format(dic_resultados['aniomes_prediccion'])).toPandas()

    df.NUP = df.NUP.apply(lambda x: str(x).rjust(8, '0'))
    df.prediccion = df.prediccion.apply(lambda x: round(x, 6))

    name = "/aplicaciones/bi/zonda/repositories/zonda-etl/scripts/analytics/modelos/seguros/scores_autos_validacion_rangos.pkl"
    df_rangos = joblib.load(name)
    df_rangos.columns = ['cuantil', 'min', 'max']

    l = []
    l = df_rangos['min'].to_list() + df_rangos['max'].to_list()
    l = list(set(l))
    l.sort()

    tree = NumArray(l)

    df['cuantil'] = df.prediccion.apply(lambda x: abs(asigna_cuantil_tree(x, tree) - 18))

    df_prop_auto = df[['NUP', 'ANIOMES', 'cuantil']].copy()

    del df

    df_seg_auto = spark.sql('''
        select * from analytics.mm_predict_segmentacion_seguros_auto
        where aniomes = {}
        '''.format(dic_resultados['aniomes_prediccion'])).toPandas()

    df_seg_vivienda = spark.sql('''
        select * from analytics.mm_predict_segmentacion_seguros_vivienda
        where aniomes = {}
        '''.format(dic_resultados['aniomes_prediccion'])).toPandas()

    # Generacion de archivos
    fecha_str = fecha.replace('-', '')
    df_prop_vivienda.to_csv('/aplicaciones/bi/zonda/temp/seguros/archivo_propension_viviendas_{}.csv'.format(fecha_str), index=False, sep=';')
    df_prop_auto.to_csv('/aplicaciones/bi/zonda/temp/seguros/archivo_propension_autos_{}.csv'.format(fecha_str), index=False, sep=';')
    df_seg_auto.to_csv('/aplicaciones/bi/zonda/temp/seguros/archivo_segmentacion_autos_{}.csv'.format(fecha_str), index=False, sep=';')
    df_seg_vivienda.to_csv('/aplicaciones/bi/zonda/temp/seguros/archivo_segmentacion_viviendas_{}.csv'.format(fecha_str), index=False, sep=';')

    # logging.info('Proceso terminado')


if __name__ == "__main__":
    arguments = parse_arguments()
    main(arguments)
