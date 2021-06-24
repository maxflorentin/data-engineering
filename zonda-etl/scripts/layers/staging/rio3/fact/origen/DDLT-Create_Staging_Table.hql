CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio3_origen(
    codigo string,
    descri string,
    activo string,
    agenda string,
    producto string,
    appsource string,
    cantcontactos string,
    prioridad string,
    cantoperaciones string,
    fecdesde string,
    fechasta string,
    canal string,
    fecdesde_crm string,
    id_camp_buc string,
    tipo_campania string,
    cant_op_agente string,
    nroshot string,
    modelo_contacto string,
    modalidad_outxorigen string,
    fec_pri_vigencia string,
    usuario_modif string,
    fecha_modif string,
    pais string,
    porcentaje_cont_contactar string,
    setgestion string,
    listallamado string,
    fec_ult_vigencia string,
    blanquear_origen_discador string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio3/fact/origen';