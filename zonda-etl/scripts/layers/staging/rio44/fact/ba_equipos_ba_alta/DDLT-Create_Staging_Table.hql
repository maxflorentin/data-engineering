CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio44_ba_equipos_ba_alta(
    id_equipos string,
    sigla string,
    nro_serie string,
    equipo string,
    modelo_descri string,
    marca_descri string,
    apertura string,
    reconoc_bill string,
    funcionalidad string,
    posicion_num string,
    posicion_num_cc string,
    id_zona string,
    descri_zonal string,
    area string,
    posicion_nom string,
    descri_pos_tipo string,
    calle string,
    numero string,
    postal string,
    postal_cpa string,
    tel string,
    localidad string,
    provincia string,
    f_modif string,
    operador_descrip string,
    red string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio44/fact/ba_equipos_ba_alta';