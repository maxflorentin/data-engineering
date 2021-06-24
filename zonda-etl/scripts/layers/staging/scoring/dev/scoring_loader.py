import json
import os
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as psf

def get_config_data(spark):
    control_file_path = '/user/{}/.sparkStaging/{app_id}/{file_name}'.format(spark.sparkContext.sparkUser(),
        app_id=spark._jsc.sc().applicationId(), file_name=sys.argv[1])
    config_raw = spark.read.option("multiline", "true").text(control_file_path)
    config_raw.printSchema()
    config_raw.show()
    config_raw_row = config_raw.agg(
        psf.concat_ws("", psf.collect_list(config_raw["value"])).alias("config")).distinct().collect()
    config_raw_str = config_raw_row[0].__getitem__("config")
    return json.loads(config_raw_str)

def get_contexts():
    spark = SparkSession.builder \
        .enableHiveSupport() \
        .getOrCreate()
    global log
    log = spark._jvm.org.apache.log4j.LogManager.getLogger('scoring_loader')
    level = spark._jvm.org.apache.log4j.Level.INFO
    log.setLevel(level)
    return spark

def find_table(spark, db, table):
    query = 'show tables in {db}'.format(db=db)
    table_list = spark.sql(query)
    table_found = table_list.filter(table_list.tableName == table).collect()
    return len(table_found) > 0

def save_data(spark,df,schema,table,path, partition):
    if find_table(spark,schema,table):
        log.info("Writing partitioned data frame")
        df \
            .repartition(1) \
            .write \
            .partitionBy("partition_date") \
            .mode('Overwrite') \
            .option("compression",'gzip') \
            .parquet(path)
        if len(df.head(1)) != 0:
            spark.sql("ALTER TABLE {schema}.{table} ADD IF NOT EXISTS PARTITION (partition_date='{partition}')".format(schema=schema, table=table, partition=partition))

    else:
        log.info("Creating table and saving data frame")
        df \
            .repartition(1) \
            .write \
            .partitionBy("partition_date") \
            .option("compression", 'gzip') \
            .saveAsTable(name=schema + '.' + table,
                         format='parquet',
                         mode='append',
                         path=path)

def get_variables(spark, partition_date, level, scoring_type):
    level_query = '@' if level == 'Solicitud' or level == 'Participante' or level == 'Propuesta' or level == 'Operacion' else level
    if scoring_type == "solicitud":
        query = "select coalesce(rt.valor,'') as valor, " \
                "rt.tipo_tramite, " \
                "rt.cod_tramite, " \
                "rt.fec_proceso, " \
                "rt.partition_date, " \
                "case when split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[1] is null " \
                "    then split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[0] " \
                "    else concat(split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[0],'-',split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[1]) end as label, " \
                "split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[0] as level, " \
                "split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[1] as level_1, " \
                "substr(rt.cod_campo,locate('@',cod_campo)+1,length(rt.cod_campo)) as variable " \
                "from bi_corp_staging.scoring_omdm_respuesta_tramites rt " \
                "where cod_campo like '/Application/Solicitud/{level_query}%' and partition_date='{partition_date}' " \
                "order by tipo_tramite,cod_tramite,level,label,level_1,variable".format(level=level, partition_date=partition_date,level_query=level_query)
        df = spark.sql(query)
        df = df.withColumn("variable_valor",psf.concat(psf.lit('"'), psf.col("variable"), psf.lit('":"'),psf.col("valor"), psf.lit('"')))
        df = df.groupBy("tipo_tramite", "cod_tramite", "partition_date", "fec_proceso", "level","label", "level_1").agg(psf.collect_list(psf.col('variable_valor')).alias("json"))
        df = df.drop(psf.col("level"))
        df = df.drop(psf.col("level_1"))

    elif scoring_type == "participante":
        query = "select coalesce(valor,'') as valor," \
                 "tipo_tramite," \
                 "cod_tramite," \
                 "fec_proceso," \
                 "partition_date," \
                 "case when split(substr(cod_campo,locate('Participante',cod_campo),length(cod_campo)),'/')[0]='Participante' then '0' else substr(split(substr(cod_campo,locate('Participante',cod_campo),length(cod_campo)),'/')[0], length(split(substr(cod_campo,locate('Participante',cod_campo),length(cod_campo)),'/')[0])-1,1) end as participante," \
                 "case when '{level}' in ('DatosFinancieros','Experiencia') then concat(split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[0], coalesce(concat('-', case when split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[1] like '%{{%' then substr(split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[1],0,locate('{{',split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[1])-1) else split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[1] end),''))" \
                 "     when '{level}' in ('Fraude','Participante') then '{level}' " \
                 "     else split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[0] end as label, " \
                 "split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[0] as level, " \
                 "split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[1] as level_1, " \
                 "split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[2] as level_2, " \
                 "substr(cod_campo, locate('@',cod_campo)+1,length(cod_campo)) as variable " \
                 "from bi_corp_staging.scoring_omdm_respuesta_tramites " \
                 "where cod_campo rlike '^/Application/Solicitud/Participante[{{]?[0-9]?[}}]?/{level_query}' and partition_date='{partition_date}' " \
                 "order by tipo_tramite,cod_tramite,participante,level,label,level_1,level_2,variable".format(level=level, partition_date=partition_date,level_query=level_query)

        df = spark.sql(query)
        df = df.withColumn("variable_valor",psf.concat(psf.lit('"'), psf.col("variable"), psf.lit('":"'),psf.col("valor"), psf.lit('"')))
        df = df.groupBy("tipo_tramite", "cod_tramite", "participante", "partition_date","fec_proceso", "level", "label", "level_1", "level_2").agg(psf.collect_list(psf.col('variable_valor')).alias("json"))
        df = df.drop(psf.col("level"))
        df = df.drop(psf.col("level_1"))
        df = df.drop(psf.col("level_2"))

    elif scoring_type == "participantePropuestaPyme":
        query = "select coalesce(valor,'') as valor," \
                 "tipo_tramite," \
                 "cod_tramite," \
                 "fec_proceso," \
                 "partition_date," \
                 "case when split(substr(cod_campo,locate('Participante',cod_campo),length(cod_campo)),'/')[0]='Participante' then '0' else substr(split(substr(cod_campo,locate('Participante',cod_campo),length(cod_campo)),'/')[0], length(split(substr(cod_campo,locate('Participante',cod_campo),length(cod_campo)),'/')[0])-1,1) end as participante," \
                 "case when '{level}' in ('Agri','Nosis') then concat(split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[0], coalesce(concat('-', case when split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[1] like '%{{%' then substr(split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[1],0,locate('{{',split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[1])-1) else split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[1] end),''))" \
                 "     when '{level}' in ('Participante', 'Balance', 'SaldosActualizados') then '{level}' " \
                 "     else split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[0] end as label, " \
                 "split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[0] as level, " \
                 "split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[1] as level_1, " \
                 "split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[2] as level_2, " \
                 "substr(cod_campo, locate('@',cod_campo)+1,length(cod_campo)) as variable " \
                 "from bi_corp_staging.scoring_omdm_respuesta_tramites " \
                 "where tipo_tramite in ('F487','F485') and cod_campo rlike '^/Application/Propuesta/Participante[{{]?[0-9]?[}}]?/{level_query}' and partition_date='{partition_date}' " \
                 "order by tipo_tramite,cod_tramite,participante,level,label,level_1,level_2,variable".format(level=level, partition_date=partition_date,level_query=level_query)

        df = spark.sql(query)
        df = df.withColumn("variable_valor",psf.concat(psf.lit('"'), psf.col("variable"), psf.lit('":"'),psf.col("valor"), psf.lit('"')))
        df = df.groupBy("tipo_tramite", "cod_tramite", "participante", "partition_date","fec_proceso", "level", "label", "level_1", "level_2").agg(psf.collect_list(psf.col('variable_valor')).alias("json"))
        df = df.drop(psf.col("level"))
        df = df.drop(psf.col("level_1"))
        df = df.drop(psf.col("level_2"))

    elif scoring_type == "propuestaPyme":
        query = "select coalesce(valor,'') as valor," \
                 "tipo_tramite," \
                 "cod_tramite," \
                 "fec_proceso," \
                 "partition_date," \
                 "split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[0] as label, " \
                 "substr(cod_campo, locate('@',cod_campo)+1,length(cod_campo)) as variable " \
                 "from bi_corp_staging.scoring_omdm_respuesta_tramites " \
                 "where tipo_tramite in ('F487','F485') and cod_campo rlike '^/Application/Propuesta/{level_query}' and partition_date='{partition_date}' " \
                 "order by tipo_tramite,cod_tramite,label,variable".format(level=level, partition_date=partition_date,level_query=level_query)

        df = spark.sql(query)
        df = df.withColumn("variable_valor",psf.concat(psf.lit('"'), psf.col("variable"), psf.lit('":"'),psf.col("valor"), psf.lit('"')))
        df = df.groupBy("tipo_tramite", "cod_tramite", "partition_date","fec_proceso", "label").agg(psf.collect_list(psf.col('variable_valor')).alias("json"))

    elif scoring_type == "operacionPyme":
        query = "select coalesce(valor,'') as valor," \
                 "tipo_tramite," \
                 "cod_tramite," \
                 "fec_proceso," \
                 "partition_date," \
                 "split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[0] as label, " \
                 "substr(cod_campo, locate('@',cod_campo)+1,length(cod_campo)) as variable " \
                 "from bi_corp_staging.scoring_omdm_respuesta_tramites " \
                 "where tipo_tramite in ('F487','F485') and cod_campo rlike '^/Application/Operacion/{level_query}' and partition_date='{partition_date}' " \
                 "order by tipo_tramite,cod_tramite,label,variable".format(level=level, partition_date=partition_date,level_query=level_query)

        df = spark.sql(query)
        df = df.withColumn("variable_valor",psf.concat(psf.lit('"'), psf.col("variable"), psf.lit('":"'),psf.col("valor"), psf.lit('"')))
        df = df.groupBy("tipo_tramite", "cod_tramite", "partition_date","fec_proceso", "label").agg(psf.collect_list(psf.col('variable_valor')).alias("json"))

    elif scoring_type == "participanteOperacionPyme":
        query = "select coalesce(valor,'') as valor," \
                "tipo_tramite," \
                "cod_tramite," \
                "fec_proceso," \
                "partition_date," \
                "case when split(substr(cod_campo,locate('Participante',cod_campo),length(cod_campo)),'/')[0]='Participante' then '0' else substr(split(substr(cod_campo,locate('Participante',cod_campo),length(cod_campo)),'/')[0], length(split(substr(cod_campo,locate('Participante',cod_campo),length(cod_campo)),'/')[0])-1,1) end as participante," \
                "case when '{level}' in ('Nosis') then concat(split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[0], coalesce(concat('-', case when split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[1] like '%{{%' then substr(split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[1],0,locate('{{',split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[1])-1) else split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[1] end),''))" \
                "     when '{level}' in ('Participante') then '{level}' " \
                "     else split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[0] end as label, " \
                "split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[0] as level, " \
                "split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[1] as level_1, " \
                "split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[2] as level_2, " \
                "substr(cod_campo, locate('@',cod_campo)+1,length(cod_campo)) as variable " \
                "from bi_corp_staging.scoring_omdm_respuesta_tramites " \
                "where tipo_tramite in ('F487','F485') and cod_campo rlike '^/Application/Operacion/Participante[{{]?[0-9]?[}}]?/{level_query}' and partition_date='{partition_date}' " \
                "order by tipo_tramite,cod_tramite,participante,level,label,level_1,level_2,variable".format(level=level, partition_date=partition_date, level_query=level_query)

        df = spark.sql(query)
        df = df.withColumn("variable_valor",psf.concat(psf.lit('"'), psf.col("variable"), psf.lit('":"'), psf.col("valor"),psf.lit('"')))
        df = df.groupBy("tipo_tramite", "cod_tramite", "participante", "partition_date", "fec_proceso", "level","label", "level_1", "level_2").agg(psf.collect_list(psf.col('variable_valor')).alias("json"))
        df = df.drop(psf.col("level"))
        df = df.drop(psf.col("level_1"))
        df = df.drop(psf.col("level_2"))

    elif scoring_type == "propuestaEmpresas":
        query = "select coalesce(valor,'') as valor," \
                 "tipo_tramite," \
                 "cod_tramite," \
                 "fec_proceso," \
                 "partition_date," \
                 "split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[0] as label, " \
                 "substr(cod_campo, locate('@',cod_campo)+1,length(cod_campo)) as variable " \
                 "from bi_corp_staging.scoring_omdm_respuesta_tramites " \
                 "where tipo_tramite = 'CPP_OMDM' and cod_campo rlike '^/Application/Propuesta/{level_query}' and partition_date='{partition_date}' " \
                 "order by tipo_tramite,cod_tramite,label,variable".format(level=level, partition_date=partition_date,level_query=level_query)

        df = spark.sql(query)
        df = df.withColumn("variable_valor",psf.concat(psf.lit('"'), psf.col("variable"), psf.lit('":"'),psf.col("valor"), psf.lit('"')))
        df = df.groupBy("tipo_tramite", "cod_tramite", "partition_date","fec_proceso", "label").agg(psf.collect_list(psf.col('variable_valor')).alias("json"))

    elif scoring_type == "participantePropuestaEmpresas":
        query = "select coalesce(valor,'') as valor," \
                 "tipo_tramite," \
                 "cod_tramite," \
                 "fec_proceso," \
                 "partition_date," \
                 "case when split(substr(cod_campo,locate('Participante',cod_campo),length(cod_campo)),'/')[0]='Participante' then '0' else substr(split(substr(cod_campo,locate('Participante',cod_campo),length(cod_campo)),'/')[0], length(split(substr(cod_campo,locate('Participante',cod_campo),length(cod_campo)),'/')[0])-1,1) end as participante," \
                 "case when '{level}' in ('Agri','Nosis') then concat(split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[0], coalesce(concat('-', case when split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[1] like '%{{%' then substr(split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[1],0,locate('{{',split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[1])-1) else split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[1] end),''))" \
                 "     when '{level}' in ('Participante', 'Balance', 'SaldosActualizados') then '{level}' " \
                 "     else split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[0] end as label, " \
                 "split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[0] as level, " \
                 "split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[1] as level_1, " \
                 "split(substr(cod_campo,locate('{level}',cod_campo),locate('/@',cod_campo)-locate('{level}',cod_campo)),'/')[2] as level_2, " \
                 "substr(cod_campo, locate('@',cod_campo)+1,length(cod_campo)) as variable " \
                 "from bi_corp_staging.scoring_omdm_respuesta_tramites " \
                 "where tipo_tramite = 'CPP_OMDM' and cod_campo rlike '^/Application/Propuesta/Participante[{{]?[0-9]?[}}]?/{level_query}' and partition_date='{partition_date}' " \
                 "order by tipo_tramite,cod_tramite,participante,level,label,level_1,level_2,variable".format(level=level, partition_date=partition_date,level_query=level_query)

        df = spark.sql(query)
        df = df.withColumn("variable_valor",psf.concat(psf.lit('"'), psf.col("variable"), psf.lit('":"'),psf.col("valor"), psf.lit('"')))
        df = df.groupBy("tipo_tramite", "cod_tramite", "participante", "partition_date","fec_proceso", "level", "label", "level_1", "level_2").agg(psf.collect_list(psf.col('variable_valor')).alias("json"))
        df = df.drop(psf.col("level"))
        df = df.drop(psf.col("level_1"))
        df = df.drop(psf.col("level_2"))

    df = df.withColumn("json", psf.concat_ws(",", psf.col("json")))
    df = df.withColumn("json", psf.concat(psf.lit('{'), psf.col("json"), psf.lit('}')))
    return df

def scoring_loader():
    spark = get_contexts()
    control_file = get_config_data(spark)
    partition_date = os.path.expandvars(control_file.get("partition_date"))
    df_solicitudes = ''
    df_participantes = ''
    df_participantesPropuestaPyme = ''
    df_propuestasPyme = ''
    df_operacionesPyme = ''
    df_participantesOperacionPyme = ''
    df_participantesPropuestaEmpresas = ''
    df_propuestasEmpresas = ''

    for variable in control_file.get("solicitud").get("variables"):
        df = get_variables(spark, partition_date, variable,"solicitud")
        df_solicitudes = df if df_solicitudes == '' else df_solicitudes.union(df)

    save_data(spark,df_solicitudes, control_file.get("solicitud").get("schema"), control_file.get("solicitud").get("table"), control_file.get("solicitud").get("path"), partition_date)

    for variable in control_file.get("participante").get("variables"):
        df = get_variables(spark, partition_date, variable,"participante")
        df_participantes = df if df_participantes == '' else df_participantes.union(df)

    save_data(spark, df_participantes, control_file.get("solicitud").get("schema"), control_file.get("participante").get("table"),control_file.get("participante").get("path"), partition_date)

    for variable in control_file.get("participantePropuestaPyme").get("variables"):
        df = get_variables(spark, partition_date, variable,"participantePropuestaPyme")
        df_participantesPropuestaPyme = df if df_participantesPropuestaPyme == '' else df_participantesPropuestaPyme.union(df)

    save_data(spark,df_participantesPropuestaPyme, control_file.get("participantePropuestaPyme").get("schema"), control_file.get("participantePropuestaPyme").get("table"), control_file.get("participantePropuestaPyme").get("path"), partition_date)

    for variable in control_file.get("propuestaPyme").get("variables"):
        df = get_variables(spark, partition_date, variable,"propuestaPyme")
        df_propuestasPyme = df if df_propuestasPyme == '' else df_propuestasPyme.union(df)

    save_data(spark,df_propuestasPyme, control_file.get("propuestaPyme").get("schema"), control_file.get("propuestaPyme").get("table"), control_file.get("propuestaPyme").get("path"), partition_date)

    for variable in control_file.get("operacionPyme").get("variables"):
        df = get_variables(spark, partition_date, variable,"operacionPyme")
        df_operacionesPyme = df if df_operacionesPyme == '' else df_operacionesPyme.union(df)

    save_data(spark,df_operacionesPyme, control_file.get("operacionPyme").get("schema"), control_file.get("operacionPyme").get("table"), control_file.get("operacionPyme").get("path"), partition_date)

    for variable in control_file.get("participanteOperacionPyme").get("variables"):
        df = get_variables(spark, partition_date, variable, "participanteOperacionPyme")
        df_participantesOperacionPyme = df if df_participantesOperacionPyme == '' else df_participantesOperacionPyme.union(df)

    save_data(spark, df_participantesOperacionPyme, control_file.get("participanteOperacionPyme").get("schema"),control_file.get("participanteOperacionPyme").get("table"),control_file.get("participanteOperacionPyme").get("path"), partition_date)

    for variable in control_file.get("propuestaEmpresas").get("variables"):
        df = get_variables(spark, partition_date, variable,"propuestaEmpresas")
        df_propuestasEmpresas = df if df_propuestasEmpresas == '' else df_propuestasEmpresas.union(df)

    save_data(spark,df_propuestasEmpresas, control_file.get("propuestaEmpresas").get("schema"), control_file.get("propuestaEmpresas").get("table"), control_file.get("propuestaEmpresas").get("path"), partition_date)

    for variable in control_file.get("participantePropuestaEmpresas").get("variables"):
        df = get_variables(spark, partition_date, variable,"participantePropuestaEmpresas")
        df_participantesPropuestaEmpresas = df if df_participantesPropuestaEmpresas == '' else df_participantesPropuestaEmpresas.union(df)

    save_data(spark,df_participantesPropuestaEmpresas, control_file.get("participantePropuestaEmpresas").get("schema"), control_file.get("participantePropuestaEmpresas").get("table"), control_file.get("participantePropuestaEmpresas").get("path"), partition_date)

if __name__ == "__main__":
    scoring_loader()