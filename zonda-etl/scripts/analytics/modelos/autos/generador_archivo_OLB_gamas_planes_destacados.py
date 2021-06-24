import joblib
import pandas as pd
import math
import datetime
from dateutil.relativedelta import relativedelta
import os
import logging
import argparse
from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.sql import SparkSession, Row, HiveContext

# Nota: todos los campos que queden en el archivo final en '0' significa que por el momento no se utilizan ... solo los dejamos porque este csv que se genera, desde OLB, lo llevan a una tablaa (así ya queda fija la estructura)

ind = 0
ZONDA_DIR = os.getenv('ZONDA_DIR')


class style():
    BLACK = '\033[30m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'
    UNDERLINE = '\033[4m'
    RESET = '\033[0m'


def print_in_color(l, color=None):
    global ind
    if color:
        print(color, *l)
        return
    if ind:
        color = style.YELLOW
        ind = 0
    else:
        color = style.GREEN
        ind = 1
    print(color, *l)


def parse_arguments():
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--fecha", required=True,
                    help="fecha de ejecucion en formato YYYY-MM-dd usado para filtrar la informacion")
    return ap.parse_args()


def seteo_de_parametros(fecha_ejecucion):
    one_month = relativedelta(months=1)
    one_day = relativedelta(days=1)

    dic_resultados = {}

    fecha_ejec = datetime.date(int(fecha_ejecucion[0:4]), int(fecha_ejecucion[5:7]), int(fecha_ejecucion[8:]))
    data_date_part = datetime.date(int(fecha_ejecucion[0:4]), int(fecha_ejecucion[5:7]), 1)
    dic_resultados['fecha_str'] = str(fecha_ejec).replace('-', '')
    dic_resultados['fecha_str2'] = fecha_ejec.isoformat()
    dic_resultados['fecha_ejecucion'] = fecha_ejecucion
    dic_resultados['fecha_ini_1m_para_atras'] = str(fecha_ejec - (one_month)).replace('-', '')
    dic_resultados['fecha_fin_1m_para_atras'] = str((fecha_ejec - one_day)).replace('-', '')
    dic_resultados['fecha_1d_ant_ejecucion'] = (fecha_ejec - one_day).strftime('%Y-%m-%d')

    return dic_resultados


def get_info_de_planes_x_gama(gama, df_merge):
    df_matching_planes = df_merge[df_merge.gama == gama]
    dic = {}
    dic['str_ramo_prod_plan'] = ['{}|{}|{}'.format(row["codigo_ramo"], row["codigo_producto"], row["codigo_plan"])
                                 for index, row in df_matching_planes.iterrows()]
    dic['destacado_mas_vendido'] = df_matching_planes.destacado_mas_vendido.tolist()
    return dic


def main(args):
    # -------FILE RESULTADOS--------
    f_name = 'segmentacion_auto_ordenamiento_gamas_'

    # ------FECHA-----------
    fecha = args.fecha
    dic_fechas = seteo_de_parametros(fecha)

    # ------LOG-----------
    # script_name = os.path.basename(sys.argv[0][0:-3])--> por el job puede cambiar
    script_name = 'generador_archivo_OLB_gamas_planes_destacados'
    fname = 'log_' + script_name + '.log'

    logging.basicConfig(filename=fname, level=logging.INFO,
                        format='%(asctime)s:%(levelname)s:%(message)s')
    logging.info('-----------------------------------------------------------------------------------')
    logging.info('-----------------------------------------------------------------------------------')
    logging.info('Comienzo de ejecucion'.format())

    # ------ CREO UNA SESIÓN DE SPARK -----------
    config = SparkConf().set('spark.kryoserializer.buffer.max', 400) \
        .set('spark.executor.memory', '8g') \
        .set('spark.rpc.message.maxSize', 512) \
        .set('spark.yarn.queue', 'root.cdsw')

    context = SparkContext(conf=config).getOrCreate()

    session = SparkSession.builder \
        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport().getOrCreate()

    # dict orden gamas
    dic_orden_gamas = joblib.load(ZONDA_DIR + '/repositories/zonda-etl/scripts/analytics/modelos/autos/dict_orden_gamas.pkl')
    print_in_color(['dic_orden_gamas \n', dic_orden_gamas])

    # ------ PLANES MÁS VENDIDOS -----------
    fd = open(ZONDA_DIR + '/repositories/zonda-etl/scripts/analytics/modelos/autos/query_q_planes_en_un_periodo.hql', 'r')
    query_q_planes = fd.read().format(fecha_ini=dic_fechas['fecha_ini_1m_para_atras'],
                                      fecha_fin=dic_fechas['fecha_fin_1m_para_atras'])
    fd.close()
    logging.info('query_q_planes toma info para las fechas entre {}-{}'.format(dic_fechas['fecha_ini_1m_para_atras'],
                                                                               dic_fechas['fecha_fin_1m_para_atras']))

    df_q_planes = session.sql(query_q_planes).toPandas()
    print_in_color(['df_q_planes.head() \n', df_q_planes.head()])
    print_in_color(['df_q_planes.shape \n', df_q_planes.shape])

    df_q_planes.columns = ["codigo_ramo", "codigo_producto", "codigo_plan", "Q_PLANES"]

    # ------ PLANES - GAMAS -----------
    fd = open(ZONDA_DIR + '/repositories/zonda-etl/scripts/analytics/modelos/autos/query_get_plan_gama.hql', 'r')
    query_plan_gama = fd.read().format(dic_fechas['fecha_ejecucion'])
    fd.close()
    logging.info('query_plan_gama toma info para la fecha {}'.format(dic_fechas['fecha_ejecucion']))

    df_plan_gama = session.sql(query_plan_gama).toPandas()
    print_in_color(['df_plan_gama.head() \n', df_plan_gama.head()])
    print_in_color(['df_plan_gama.shape \n', df_plan_gama.shape])
    print_in_color(['df_plan_gama.gama.value_counts() \n', df_plan_gama.gama.value_counts()])

    df_plan_gama = df_plan_gama.astype({"gama": int, "codigo_ramo": int})

    # ------ PASAMOS A 4 GAMAS -----------
    df_plan_gama['gama'] = [x - 1 if x != 1 else 1 for x in df_plan_gama['gama']]  # IMPORTANTE!
    print_in_color(['PASAMOS A 4 GAMAS \n'])
    print_in_color(['df_plan_gama.head() \n', df_plan_gama.head()])
    print_in_color(['df_plan_gama.gama.value_counts() \n', df_plan_gama.gama.value_counts()])

    # ------ MERGEO PARA TENER EL NIVEL DE DESTACADO DE CADA PLAN EN CUANTO A Q ALTAS -----------
    df_merge = pd.merge(df_plan_gama, df_q_planes, on=["codigo_ramo", "codigo_producto", "codigo_plan"], how='left')
    logging.info('Se hizo el merge para obtener el nivel de destacado de cada plan acorde a q altas'.format())

    print_in_color(['df_merge.head() \n', df_merge.head()])

    df_merge = df_merge.sort_values(by=['Q_PLANES'], ascending=False)
    print_in_color(['df_merge.head() [SORTED BY Q_PLANES] \n', df_merge.head()])

    df_merge.reset_index(drop=True, inplace=True)

    print_in_color(['df_merge[~df_merge.Q_PLANES.isna()].tail() \n', df_merge[~df_merge.Q_PLANES.isna()].tail()])
    print_in_color(['df_merge[df_merge.Q_PLANES.isna()].head() \n', df_merge[df_merge.Q_PLANES.isna()].head()])

    i_first_nan = df_merge.index[df_merge['Q_PLANES'].isna()][0]
    print_in_color(['i_first_nan \n', i_first_nan])

    lis = [index + 1 if not math.isnan(row['Q_PLANES']) else i_first_nan + 1 for index, row in df_merge.iterrows()]

    df_merge['destacado_mas_vendido'] = lis
    print_in_color(['df_merge.head() [CON DESTACADO] \n', df_merge.head()])

    # ------ DF FINAL -----------
    columns = ['gama_cliente', 'gama_plan', 'ramo_prod_plan', 'orden', 'destacado1', 'destacado2', 'destacado3']

    df_final = pd.DataFrame(columns=columns)

    dict_planes_x_gama = {}
    for k in dic_orden_gamas.keys():
        dict_planes_x_gama[k] = get_info_de_planes_x_gama(k, df_merge)

    for gama in dic_orden_gamas.keys():
        for subgama in dic_orden_gamas[gama]:
            df_aux = pd.DataFrame(columns=columns)
            df_aux.ramo_prod_plan = dict_planes_x_gama[subgama]['str_ramo_prod_plan']
            df_aux.orden = 0
            df_aux.destacado1 = dict_planes_x_gama[subgama]['destacado_mas_vendido']
            df_aux.destacado2 = 0
            df_aux.destacado3 = 0
            df_aux.gama_cliente = gama  # hardcodeo la gama_cliente actual
            df_aux.gama_plan = subgama  # hardcodeo la gama_plan actual
            df_final = df_final.append(df_aux, ignore_index=True)

    print_in_color(['df_final.head() [CON DESTACADO] \n', df_final.head()])
    logging.info('Se creó el df_final y tiene {} rows'.format(df_final.shape))

    # ------ CSV -----------
    df_final['fecha'] = dic_fechas['fecha_str2']

    base_name = ZONDA_DIR + '/outbound/' + f_name + '{}.csv'
    print(base_name)

    df_final.to_csv(base_name.format(datetime.datetime.now().strftime('%Y%m%d')), index=False, sep=';')

    logging.info('FIN'.format())


if __name__ == "__main__":
    arguments = parse_arguments()
    main(arguments)