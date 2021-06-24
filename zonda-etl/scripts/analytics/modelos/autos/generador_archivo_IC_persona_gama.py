import pandas as pd
import argparse
import datetime
from dateutil.relativedelta import relativedelta
import logging
import os
from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.sql import SparkSession, Row, HiveContext
from pyspark.sql import functions as f

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
    

def print_in_color(l, color = None):
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
    

def seteo_de_parametros(fecha_ejecucion):
    one_month = relativedelta(months=1)
    one_day = relativedelta(days=1)

    dic_resultados = {}

    fecha_ejec = datetime.date(int(fecha_ejecucion[0:4]),int(fecha_ejecucion[5:7]),int(fecha_ejecucion[8:]))
    data_date_part = datetime.date(int(fecha_ejecucion[0:4]),int(fecha_ejecucion[5:7]),1)
    dic_resultados['fecha_ejecucion'] = fecha_ejecucion
    dic_resultados['fecha_prediccion'] = str(data_date_part + (one_month))
    dic_resultados['aniomes_prediccion'] = dic_resultados['fecha_prediccion'][0:4] + dic_resultados['fecha_prediccion'][5:7]
    dic_resultados['vigencia_pred_fecha_ini'] = str(data_date_part + one_month)
    dic_resultados['vigencia_pred_fecha_fin'] = str(data_date_part + one_month * 2 - one_day)
    
    return dic_resultados


def parse_arguments():
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--fecha", required=True,
                    help="fecha de ejecucion en formato YYYY-MM-dd usado para filtrar la informacion")    
    return ap.parse_args()


def main(args):
    # -------FILE RESULTADOS--------
    f_name = 'archivo_segmentacion_autos_'
    
    #------FECHA-----------
    fecha = args.fecha
    dic_resultados = seteo_de_parametros(fecha)
    
    #------LOG-----------
    # script_name = os.path.basename(sys.argv[0][0:-3]) --> por el job puede cambiar
    script_name = 'generador_archivo_IC_persona_gama'
    logname = 'log_' + script_name + '.log'
    logging.basicConfig(filename = logname, level = logging.INFO,
                      format = '%(asctime)s:%(levelname)s:%(message)s')
    logging.info('-----------------------------------------------------------------------------------')
    logging.info('-----------------------------------------------------------------------------------')
    logging.info('Comienzo de ejecucion'.format())
    
    
    #------CREAR UNA SESIÓN DE SPARK-----------
    """
    config = SparkConf().set('spark.kryoserializer.buffer.max', 400) \
        .set('spark.executor.memory', '16g') \
        .set('spark.rpc.message.maxSize', 512) \
        .set('spark.yarn.queue', 'root.cdsw')

    context = SparkContext(conf=config).getOrCreate()
    """
    spark = SparkContext.getOrCreate()
    spark_sql = SQLContext.getOrCreate(spark)
    session = SparkSession.builder \
        .enableHiveSupport().getOrCreate()
    print_in_color(["Se conecto a spark"])

    def get_config_data(spark_sql, config_file_path):
        config_raw = spark_sql.read.option("multiline", "true").text(config_file_path)
        config_raw.printSchema()
        config_raw.show()
        config_raw_row = config_raw.agg(
            f.concat_ws(" ", f.collect_list(config_raw["value"])).alias("config")).distinct().collect()
        config_raw_str = config_raw_row[0].__getitem__("config")
        return config_raw_str
    
    print_in_color(["fecha pred", dic_resultados['aniomes_prediccion']])   

    
    #------TRAER DATA PREDS GAMAS-----------
    # get query
    fd = get_config_data(spark_sql, '/santander/bi-corp/sandbox/bi/config/autos/queries/query_get_predicciones_persona_gama.hql')
    query_data = fd.format(dic_resultados['aniomes_prediccion'])
    logging.info('query_get_predicciones_persona_gama toma info para aniomes = {}'.format(dic_resultados['aniomes_prediccion']))
        
    df = session.sql(query_data)
    df.createOrReplaceTempView("query_data")

    t = 1
    df_final = pd.DataFrame()
    for i in range(1, 10000000):
        row_nr_from = t
        row_nr_to = row_nr_from + 600000
        queri = "select * from query_data where row_num between {} and {}".format(row_nr_from, row_nr_to)
        print(queri)
        df_temp = session.sql(queri).toPandas()
        df_final.append(df_temp)
        print(df_final)
        t = row_nr_to + 1

    df = df_final
    print_in_color(["Se cargó la data de predicciones de gamas"])        
    print_in_color(["df.head = \n", df.head()])    
    print_in_color(["df.shape = \n", df.shape])        
    
    # agregamos col fecha con inicio de periodo de vigencia de las predicciones
    df['ANIOMES'] = dic_resultados['aniomes_prediccion']

    
    #------TRAER DATA SCORES-----------
    # get query
    fd = get_config_data(spark_sql, '/santander/bi-corp/sandbox/bi/config/autos/queries/query_get_scores_propension.hql')
    query_data = fd.format(dic_resultados['aniomes_prediccion'])
    logging.info('query_get_scores_propension toma info para aniomes = {}'.format(dic_resultados['aniomes_prediccion']))
  
    df_scores = session.sql(query_data).toPandas()
    df_scores.columns = ['NUP', 'score', 'aniomes']
    
    print_in_color(["Se cargó la data de scores"])        
    print_in_color(["df_scores.head = \n", df_scores.head()])    
    print_in_color(["df_scores.shape = \n", df_scores.shape])        
        
    
    #--------MERGE-----------
    # tests
    df_merge_right = pd.merge(left=df, right=df_scores, on='NUP', how='right') 
    print_in_color([  "df_merge_right prediccion Nan  = \n",   df_merge_right[df_merge_right['prediccion'].isna()].head(20)  ])    
    print_in_color(["df_merge_right.shape = \n", df_merge_right.shape])        
 
    df_merge_left = pd.merge(left=df, right=df_scores, on='NUP', how='left')   
    print_in_color([  "df_merge_left score Nan head = \n",    df_merge_left[df_merge_left['score'].isna()].head(20)  ])     
    print_in_color(["df_merge_left.shape = \n", df_merge_left.shape])        

    
    # TODO: IMPORTANT dejar esto inner y borrar los 2 de arriba con sus prints  
    df_merge = pd.merge(left=df, right=df_scores, on='NUP', how='inner') 
    print_in_color(["Se hizo el merge"]) 
    df_merge.NUP = df_merge.NUP.apply(lambda x: str(x).rjust(8,'0'))
    print_in_color(["df_merge.head = \n", df_merge.head()])     
    print_in_color(["df_merge.shape = \n", df_merge.shape])        
    
    df_merge = df_merge.sort_values(by=['score'], ascending=False)
    print_in_color(["Se hizo el sort by scores"])        
    print_in_color(["df_merge.head = \n", df_merge.head()])        
        
    rows = int(df_merge.shape[0] * 0.5)
    print(rows)
    df_merge = df_merge.head(rows)
    print_in_color(["Se hizo la selección del 50%"])        
    print_in_color(["df_merge.shape = \n", df_merge.shape])        
        

    #--------CSV-----------
    fecha_str = dic_resultados['vigencia_pred_fecha_ini'].replace('-','')

    base_name = ZONDA_DIR + '/outbound/' + f_name + '{}.csv'
    print(base_name)

    df_merge[['NUP', 'prediccion', 'ANIOMES']].to_csv(base_name.format(fecha_str), index=False, sep=';')
    print_in_color(l = [""], color = style.RESET)    
    logging.info('Fin. Se dispone del archivo correspondiente'.format())


if __name__== "__main__":
    arguments = parse_arguments()
    main(arguments)

