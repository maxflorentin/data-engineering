CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_rec_subconcepto (
cod_rec_entidad string ,
cod_rec_subconcepto int ,
ds_rec_subconcepto string ,
ds_rec_detalle_subconcepto string ,
cod_rec_estado_subconcepto string ,
partition_date string )
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' 
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/dim_rec_subconcepto';