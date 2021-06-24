CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio258_cp_envios(
    crupier_id string ,
    cod_cliente string ,
    cod_sucursal string ,
    nro_solicitud_asol string ,
    nro_paquete string ,
    nro_cta_paquete string ,
    cod_operacion string ,
    fecha_registro string ,
    cod_canal_venta string ,
    ejecutivo_comercial string ,
    calle string ,
    altura string ,
    piso string ,
    depto string ,
    cod_postal string ,
    localidad string ,
    telefono string ,
    cod_paquete string ,
    cod_barra string ,
 --    ultima_modif string ,
    creado_por string ,
   ultima_modif_por string ,
    cod_producto string ,
    tipo_dom string ,
    exp_partial string,
    estado string,
    fecha_estado string,
    es_finalizado string,
    cod_subproducto string,
    suc_horario_atencion string,
    courier          string
 )
    PARTITIONED BY (
       ultima_modif string)
    ROW FORMAT SERDE
      'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio258/fact/rio258_cp_envios'