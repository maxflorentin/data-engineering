CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio157_ms0_dt_plan_agrp_func (
    cod_pais number string , 
    cod_agrp_func string , 
    des_agrp_func string , 
    tip_concepto_ctb string , 
    tip_malla char string , 
    fec_alta string , 
    fec_baja string , 
    userid_umo string , 
    timest_umo string , 
    ind_insolvencia string , 
    ind_comision string , 
    cod_est_sdo string , 
    cod_sis_origen string , 
    cod_contenido_prev string , 
    ind_costes string , 
    cod_posicion_valor string
        )
    PARTITIONED BY (
      partition_date string)
    ROW FORMAT SERDE
      'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio157/dim/ms0_dt_plan_agrp_func'