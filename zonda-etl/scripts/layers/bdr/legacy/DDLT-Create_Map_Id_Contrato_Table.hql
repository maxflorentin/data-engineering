
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.id_cto_map(
 idf_cto_supra string,
 cod_entidad string,
 cod_centro string,
 num_cuenta string,
 cod_producto string,
 cod_subprodu_altair string,
 cod_divisa string,
 id_cto_bdr string
)
clustered by (idf_cto_supra) into 5 buckets
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/id_cto_map';