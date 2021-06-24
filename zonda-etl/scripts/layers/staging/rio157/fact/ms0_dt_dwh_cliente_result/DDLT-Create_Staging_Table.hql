CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio157_ms0_dt_dwh_cliente_result (
    cod_pais  string,
    fec_data  string,
    idf_pers_ods  string,
    idf_cliente  string,
    num_persona  string,
    nom_nombre  string,
    nom_apellido_1  string,
    nom_apellido_2  string,
    cod_tip_persona  string,
    cod_vincula  string,
    val_sexo  string,
    cod_gestor  string,
    cod_segmento_gest  string,
    fec_alta_pers  string,
    fec_baja_pers  string,
    camada  string,
    cod_campus  string,
    cod_estamento  string,
    cod_universidad  string,
    idf_campus  string,
    idf_universidad  string,
    ind_tip_universidad  string,
    ind_convenio  string,
    cod_grp_global  string,
    userid_umo  string,
    timest_umo  string,
    cod_estado_riesgo  string,
    cod_subestado_riesgo  string,
    cal_rating  string,
    clasif_banco  string,
    marca_subest  string,
    mca_ree_cli  string,
    con_grado_cli  string,
    fec_reestruc  string,
    ind_empl_grupo  string
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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio157/fact/ms0_dt_dwh_cliente_result'