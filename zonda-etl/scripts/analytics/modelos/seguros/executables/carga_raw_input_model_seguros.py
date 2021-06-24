import pandas as pd
import numpy as np
import json
import argparse
import sys    
import os    

from UtilConexiones import conectar_oracle, consulta_base_to_pandas
import yaml


def parse_arguments():
    ap = argparse.ArgumentParser()

    ap.add_argument("-f", "--fecha", required=True,
                    help="fecha de ejecucion en formato YYYY-MM-dd usado para filtrar la informacion")
    
    return ap.parse_args()



def main(args):

  import logging
  script_name =  'log_' + os.path.basename(sys.argv[0][0:-3])
  logging.basicConfig(filename = script_name + '.log', level = logging.INFO,
                      format = '%(asctime)s:%(levelname)s:%(message)s')

  logging.info('Comienzo de ejecucion')

  def seteo_de_parametros(fecha_ejecucion):
    import datetime
    from dateutil.relativedelta import relativedelta

    def last_day_of_month(any_day, current_day):
      next_month = any_day.replace(day=28) + datetime.timedelta(days=4)  # this will never fail
      if ((next_month - datetime.timedelta(days=next_month.day)) > current_day):
        ret_date = current_day
      else:
        ret_date = next_month - datetime.timedelta(days=next_month.day)
      return ret_date

    one_month = relativedelta(months=1)
    one_day = relativedelta(days=1)

    dic_resultados = {}

    fecha_ejec = datetime.date(int(fecha_ejecucion[0:4]), int(fecha_ejecucion[5:7]), int(fecha_ejecucion[8:]))
    data_date_part = datetime.date(int(fecha_ejecucion[0:4]), int(fecha_ejecucion[5:7]), 1)
    dic_resultados['fecha_ejecucion'] = fecha_ejecucion
    dic_resultados['fecha_ini_mes_act'] = str(data_date_part)

    sdate = (data_date_part - (one_month * 2))
    edate = data_date_part

    delta = edate - sdate

    s = []
    for i in range(delta.days + 1):
      day = sdate + datetime.timedelta(days=i)
      s.append(str(last_day_of_month(day, edate)))

    s = list(set(s))

    dic_resultados['fecha_ini_mes_2m_ant'] = str(data_date_part - (one_month * 2))
    dic_resultados['fecha_fin_mes_act'] = str((data_date_part + (one_month)) - one_day)

    dic_resultados['fecha_mora'] = "('" + "','".join(s) + "')"

    f_2d_ant = fecha_ejec - 2 * one_day
    if f_2d_ant.weekday() > 4:
      f_2d_ant = f_2d_ant - 2 * one_day
    else:
      None
    dic_resultados['fecha_ejecucion_2d_ant'] = str(f_2d_ant)

    return dic_resultados

  # ------CREAR UNA SESIÓN DE SPARK-----------
  from pyspark import SparkContext, SQLContext, SparkConf
  from pyspark.sql import SparkSession, Row, HiveContext
  from pyspark.sql import functions as f

  config = SparkConf().set('spark.kryoserializer.buffer.max', 200)
  sc = SparkContext(conf=config).getOrCreate()
  spark = SparkSession.builder.enableHiveSupport().getOrCreate()
  spark_sql = SQLContext.getOrCreate(sc)

  def get_config_data(spark_sql, config_file_path):
    config_raw = spark_sql.read.option("multiline", "true").text(config_file_path)
    config_raw.printSchema()
    config_raw.show()
    config_raw_row = config_raw.agg(
      f.concat_ws(" ", f.collect_list(config_raw["value"])).alias("config")).distinct().collect()
    config_raw_str = config_raw_row[0].__getitem__("config")
    return config_raw_str

  logging.info('Se conecto a spark')

  cfg = json.loads(get_config_data(spark_sql, "/santander/bi-corp/sandbox/bi/config/seguros/yml/config_file_raw_input.json"))
  #cfg = yaml.load(ymlfile)
  
  logging.info('Se leyo archivo de configuracion')  
    
  fechas = seteo_de_parametros(args.fecha)

  query_emp = get_config_data(spark_sql, cfg['query_emp'])

  #query_mora = get_config_data(spark_sql, cfg['query_mora'].format(fechas['fecha_mora'].replace('-','')))
    
  query_cotizaciones = get_config_data(spark_sql, cfg['query_cotizaciones']).format(fechas['fecha_ini_mes_2m_ant'],fechas['fecha_ejecucion'])

  query_persona = get_config_data(spark_sql, cfg['query_persona']).format(fechas['fecha_ejecucion_2d_ant'].replace('-',''))

  #------------CONSULTA DATOS LAKE------------------

  print(logging.info('Comienzo de lectura de datos Lake'))

  df_empleados_santander = spark.sql(query_emp).toPandas()
  df_mora = spark.sql('SELECT num_persona,\
        max(ind_demora_1_30) as FLAG_MORA_30D, \
        max(ind_demora_31_60) as FLAG_MORA_60D, \
        max(ind_demora_61_90) as FLAG_MORA_90D, \
        max(case when ind_demora_91_120 + ind_demora_121_150 + ind_demora_151_180 + ind_demora_mas_180 >0 then 1 else 0 end) as FLAG_MORA_MAS_90 \
        FROM santander_business_risk_arda.contratos \
        WHERE data_date_part in {} \
        AND num_persona is not null \
        GROUP BY num_persona'.format(fechas['fecha_mora'].replace('-',''))).toPandas()
  df_cotizaciones = spark.sql(query_cotizaciones).toPandas()
  df_persona = spark.sql("select num_persona,cod_estado_civil_cliente,sexo_cliente,cod_tip_ocupacion_cliente,cod_actividad \
                          from santander_business_risk_arda.personas \
                          where data_date_part = '{}' and num_persona is not null".format(fechas['fecha_ejecucion_2d_ant'].replace('-',''))).toPandas()


  print(logging.info('Fin de lectura de datos Lake'))

  #-------------QUERY-------------------------------
  query_cli = get_config_data(spark_sql, cfg['query_cli']).format(fechas['fecha_ini_mes_2m_ant'],fechas['fecha_ini_mes_2m_ant'])

  #-------------QUERY-------------------------------
  query_dom = get_config_data(spark_sql, cfg['query_domicilio'])

  #-------------QUERY-------------------------------
  query_cons = get_config_data(spark_sql, cfg['query_cons_rub']).format(fechas['fecha_ini_mes_2m_ant'],fechas['fecha_ejecucion'])

    #--------- CARGA PARAMETROS---------
  with get_config_data(spark_sql, cfg['params_ora']) as read_file:
      param = json.load(read_file)

  #---------CONEXIÓN RIO61--------------------

  print(logging.info('Comienzo de lectura de datos en RIO61'))
    
  con = conectar_oracle(cfg['db_ora_61'],param)
    
  df = consulta_base_to_pandas(query_cli,con)  

  print(logging.info('Fin de lectura de datos en RIO61'))

  #---------CONEXIÓN RIO30--------------------
  
  print(logging.info('Comienzo de lectura de datos en RIO30'))
    
  con = conectar_oracle(cfg['db_ora_30'],param)
    
  df_dom = consulta_base_to_pandas(query_dom,con)  

  df_cons = consulta_base_to_pandas(query_cons,con)

  print(logging.info('Fin de lectura de datos en RIO30'))
  

  ##--------------------- MANIPULACION DE DATOS ---------------------------------------------------------------  
    
  #------Agregado de marca de empleado

  print(logging.info('Comienzo manipulacion de datos'))
    
  df_empleados_santander.loc[:,'num_persona'] = df_empleados_santander['num_persona'].astype(int)
  df_empleados_santander['cod_periodo'] = df_empleados_santander['cod_periodo'].astype(int)

  df.loc[:,'NUP'] = df['NUP'].astype(int)
  df['ANIOMES'] = df['ANIOMES'].astype(int)
  df['NROMES'] = df['NROMES'].astype(int)

  
  df = pd.merge(left = df, right = df_empleados_santander, left_on = ['NUP','ANIOMES']
                      , right_on= ['num_persona','cod_periodo'], how = 'left')
  df.loc[df.Marca_Empleado_Santander.isna(),'Marca_Empleado_Santander'] = 0

  df = df.drop(columns=['num_persona','cod_periodo'])
  
  
  df_dom.loc[:,'PENUMPER'] = df_dom['PENUMPER'].astype(int)
  df_cons.loc[:,'PENUMPER'] = df_cons['PENUMPER'].astype(int)
  df_mora.loc[:,'num_persona'] = df_mora['num_persona'].astype(int)
  df_persona.loc[:,'num_persona'] = df_persona['num_persona'].astype(int)
  df_cotizaciones.loc[:,'ivct_nu_nup'] = df_cotizaciones['ivct_nu_nup'].astype(int)
  
  
  df_merge = pd.merge(left = df, right = df_dom, left_on = ['NUP'], right_on = ['PENUMPER'], how = 'left')
  df_merge = df_merge.drop(columns=['PENUMPER'])

  df_merge = pd.merge(left = df_merge, right = df_mora, left_on = ['NUP'], right_on = ['num_persona'], how = 'left')
  df_merge = df_merge.drop(columns=['num_persona'])

  df_merge = pd.merge(left = df_merge, right = df_persona, left_on = ['NUP'], right_on = ['num_persona'], how = 'left')
  df_merge = df_merge.drop(columns=['num_persona'])

  df_merge = pd.merge(left = df_merge, right = df_cotizaciones, left_on = ['NUP'], right_on = ['ivct_nu_nup'], how = 'left')
  df_merge = df_merge.drop(columns=['ivct_nu_nup'])

  df_merge = pd.merge(left = df_merge, right = df_cons, left_on = ['NUP'], right_on = ['PENUMPER'], how = 'left')
  df_merge = df_merge.drop(columns=['PENUMPER'])
       
    
  print(logging.info('Fin de manipulacion de datos'))


  #------------------------------------
  logging.info('Comienzo de carga de resultados en hive')
  
  #------------------ 
  df_merge.loc[:,'PECODPRO'] = df_merge.loc[:,'PECODPRO'].astype(str)
  df_merge.loc[:,'cod_estado_civil_cliente'] = df_merge.loc[:,'cod_estado_civil_cliente'].astype(str)
  df_merge.loc[:,'sexo_cliente'] = df_merge.loc[:,'sexo_cliente'].astype(str)
  df_merge.loc[:,'cod_tip_ocupacion_cliente'] = df_merge.loc[:,'cod_tip_ocupacion_cliente'].astype(str)
  df_merge.loc[:,'cod_actividad'] = df_merge.loc[:,'cod_actividad'].astype(str)
    
  table_name = cfg['db_name'] + '.' + cfg['tbl_name']  
  df = spark.createDataFrame(df_merge)
  df.repartition(5).write.partitionBy('ANIOMES').saveAsTable(name=table_name,format='parquet',mode='overwrite')

  logging.info('Se cargaron {} registros'.format(df_merge.shape[0]))
  logging.info('Fin de carga de resultados en hive')
  
  

  logging.info('Proceso terminado')
  


if __name__== "__main__":
  arguments = parse_arguments()
  main(arguments)
    

