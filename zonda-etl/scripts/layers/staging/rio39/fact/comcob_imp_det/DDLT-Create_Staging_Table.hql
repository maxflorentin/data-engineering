CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.comex_rio39_comcob_imp_det(
         comnum string,
         concepto string,
         dbto_cdto string,
         porcentaje string,
         cod_moneda string,
         cotizacion string,
         importe_origen string,
         importe_pesos string,
         concepto_desc string,
         modif_usu string,
         modif_fec string,
         verif_usu string,
         verif_fec string,
         fecha_proc string,
         fecha_valor string,
         estado string,
         contab string,
         comprob string,
         cod_agrupa string,
         cob_dev string,
         marca_iva string,
         carga_fecha string
)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio39/fact/comcob_imp_det';