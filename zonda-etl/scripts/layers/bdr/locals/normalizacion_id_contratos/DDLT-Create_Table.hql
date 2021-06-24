CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.normalizacion_id_contratos(
id_cto_bdr string
,source string
,id_cto_source string
,cred_cod_entidad string
,cred_cod_centro string
,cred_num_cuenta string
,cred_cod_producto string
,cred_cod_subprodu_altair string
,mmff_cod_especie string
,mmff_cod_disponibilidad string
,mmff_cod_sis_origen string
,mmff_cod_cartera string
,mmff_cod_operacion string
,mmff_cod_pata string
,blc_sociedad string
,blc_cargabal string
,blc_sociedad_eliminacion string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/normalizacion_id_contratos';