-- dim_ren_area_de_negocio
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_ren_area_de_negocio (
    cod_ren_pais INT ,
	cod_ren_area_negocio STRING ,
	cod_ren_area_negocio_niv_1 STRING ,
	cod_ren_area_negocio_niv_2 STRING ,
	cod_ren_area_negocio_niv_3 STRING ,
	cod_ren_area_negocio_niv_4 STRING ,
	cod_ren_area_negocio_niv_5 STRING ,
	cod_ren_area_negocio_niv_6 STRING ,
	cod_ren_area_negocio_niv_7 STRING ,
	cod_ren_area_negocio_niv_8 STRING ,
	cod_ren_area_negocio_niv_9 STRING ,
	cod_ren_area_negocio_niv_10 STRING ,
	cod_ren_area_negocio_niv_11 STRING ,
	cod_ren_area_negocio_niv_12 STRING ,
	cod_ren_area_negocio_niv_13 STRING ,
	cod_ren_area_negocio_niv_14 STRING ,
	ds_ren_area_negocio_niv_1 STRING ,
	ds_ren_area_negocio_niv_2 STRING ,
	ds_ren_area_negocio_niv_3 STRING ,
	ds_ren_area_negocio_niv_4 STRING ,
	ds_ren_area_negocio_niv_5 STRING ,
	ds_ren_area_negocio_niv_6 STRING ,
	ds_ren_area_negocio_niv_7 STRING ,
	ds_ren_area_negocio_niv_8 STRING ,
	ds_ren_area_negocio_niv_9 STRING ,
	ds_ren_area_negocio_niv_10 STRING ,
	ds_ren_area_negocio_niv_11 STRING ,
	ds_ren_area_negocio_niv_12 STRING ,
	ds_ren_area_negocio_niv_13 STRING ,
	ds_ren_area_negocio_niv_14 STRING ,
partition_date STRING )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/santander/bi-corp/common/rentabilidad/dim/dim_ren_area_de_negocio';

/* 
SELECT 
AN.cod_pais as cod_ren_pais,
AN.cod_area_negocio as cod_ren_area_negocio,
AN.cod_area_negocio_niv_1 as cod_ren_area_negocio_niv_1,
AN.cod_area_negocio_niv_2 as cod_ren_area_negocio_niv_2,
AN.cod_area_negocio_niv_3 as cod_ren_area_negocio_niv_3,
AN.cod_area_negocio_niv_4 as cod_ren_area_negocio_niv_4,
AN.cod_area_negocio_niv_5 as cod_ren_area_negocio_niv_5,
AN.cod_area_negocio_niv_6 as cod_ren_area_negocio_niv_6,
AN.cod_area_negocio_niv_7 as cod_ren_area_negocio_niv_7,
AN.cod_area_negocio_niv_8 as cod_ren_area_negocio_niv_8,
AN.cod_area_negocio_niv_9 as cod_ren_area_negocio_niv_9,
AN.cod_area_negocio_niv_10 as cod_ren_area_negocio_niv_10,
AN.cod_area_negocio_niv_11 as cod_ren_area_negocio_niv_11,
AN.cod_area_negocio_niv_12 as cod_ren_area_negocio_niv_12,
AN.cod_area_negocio_niv_13 as cod_ren_area_negocio_niv_13,
AN.cod_area_negocio_niv_14 as cod_ren_area_negocio_niv_14,
AN.des_area_negocio_niv_1 as ds_ren_area_negocio_niv_1,
AN.des_area_negocio_niv_2 as ds_ren_area_negocio_niv_2,
AN.des_area_negocio_niv_3 as ds_ren_area_negocio_niv_3,
AN.des_area_negocio_niv_4 as ds_ren_area_negocio_niv_4,
AN.des_area_negocio_niv_5 as ds_ren_area_negocio_niv_5,
AN.des_area_negocio_niv_6 as ds_ren_area_negocio_niv_6,
AN.des_area_negocio_niv_7 as ds_ren_area_negocio_niv_7,
AN.des_area_negocio_niv_8 as ds_ren_area_negocio_niv_8,
AN.des_area_negocio_niv_9 as ds_ren_area_negocio_niv_9,
AN.des_area_negocio_niv_10 as ds_ren_area_negocio_niv_10,
AN.des_area_negocio_niv_11 as ds_ren_area_negocio_niv_11,
AN.des_area_negocio_niv_12 as ds_ren_area_negocio_niv_12,
AN.des_area_negocio_niv_13 as ds_ren_area_negocio_niv_13,
AN.des_area_negocio_niv_14 as ds_ren_area_negocio_niv_14,
AN.partition_date as partition_date

FROM bi_corp_staging.rio157_ms0_dm_dwh_area_negocio_ AN
WHERE AN.cod_jerq_adn01 = 'JAN01'
AND AN.partition_date IN 
                (SELECT max(aux.partition_date)
		           FROM bi_corp_staging.rio157_ms0_dm_dwh_area_negocio_ aux
	            	WHERE aux.cod_jerq_adn01 = 'JAN01' AND aux.partition_date >= date_sub(current_date(), 7) AND aux.partition_date <= to_date(current_date()) ) */
	

-- dim_ren_bca_corp
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_ren_bca_corp (
    cod_ren_pais INT ,
	cod_ren_bca_corp_niv_1 STRING ,
	cod_ren_bca_corp_niv_2 STRING ,
	cod_ren_bca_corp_niv_3 STRING ,
	cod_ren_bca_corp_niv_4 STRING ,
	cod_ren_bca_corp_niv_5 STRING ,
	cod_ren_bca_corp_niv_6 STRING ,
	cod_ren_bca_corp_niv_7 STRING ,
	cod_ren_bca_corp_niv_8 STRING ,
	cod_ren_bca_corp_niv_9 STRING ,
	cod_ren_bca_corp_niv_10 STRING ,
	cod_ren_bca_corp_niv_11 STRING ,
	cod_ren_bca_corp_niv_12 STRING ,
	cod_ren_bca_corp_niv_13 STRING ,
	cod_ren_bca_corp_niv_14 STRING ,
	ds_ren_bca_corp_niv_1 STRING ,
	ds_ren_bca_corp_niv_2 STRING ,
	ds_ren_bca_corp_niv_3 STRING ,
	ds_ren_bca_corp_niv_4 STRING ,
	ds_ren_bca_corp_niv_5 STRING ,
	ds_ren_bca_corp_niv_6 STRING ,
	ds_ren_bca_corp_niv_7 STRING ,
	ds_ren_bca_corp_niv_8 STRING ,
	ds_ren_bca_corp_niv_9 STRING ,
	ds_ren_bca_corp_niv_10 STRING ,
	ds_ren_bca_corp_niv_11 STRING ,
	ds_ren_bca_corp_niv_12 STRING ,
	ds_ren_bca_corp_niv_13 STRING ,
	ds_ren_bca_corp_niv_14 STRING ,
	partition_date STRING )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/santander/bi-corp/common/rentabilidad/dim/dim_ren_bca_corp';

/*
INSERT
	OVERWRITE TABLE bi_corp_common.dim_ren_bca_corp
SELECT
	BC.cod_pais  cod_ren_pais,
	BC.cod_bca_corp_niv_1  cod_ren_bca_corp_niv_1,
	BC.cod_bca_corp_niv_2  cod_ren_bca_corp_niv_2,
	BC.cod_bca_corp_niv_3  cod_ren_bca_corp_niv_3,
	BC.cod_bca_corp_niv_4  cod_ren_bca_corp_niv_4,
	BC.cod_bca_corp_niv_5  cod_ren_bca_corp_niv_5,
	BC.cod_bca_corp_niv_6  cod_ren_bca_corp_niv_6,
	BC.cod_bca_corp_niv_7  cod_ren_bca_corp_niv_7,
	BC.cod_bca_corp_niv_8  cod_ren_bca_corp_niv_8,
	BC.cod_bca_corp_niv_9  cod_ren_bca_corp_niv_9,
	BC.cod_bca_corp_niv_10  cod_ren_bca_corp_niv_10,
	BC.cod_bca_corp_niv_11  cod_ren_bca_corp_niv_11,
	BC.cod_bca_corp_niv_12  cod_ren_bca_corp_niv_12,
	BC.cod_bca_corp_niv_13  cod_ren_bca_corp_niv_13,
	BC.cod_bca_corp_niv_14  cod_ren_bca_corp_niv_14,
	BC.des_bca_corp_niv_1  ds_ren_bca_corp_niv_1,
	BC.des_bca_corp_niv_2  ds_ren_bca_corp_niv_2,
	BC.des_bca_corp_niv_3  ds_ren_bca_corp_niv_3,
	BC.des_bca_corp_niv_4  ds_ren_bca_corp_niv_4,
	BC.des_bca_corp_niv_5  ds_ren_bca_corp_niv_5,
	BC.des_bca_corp_niv_6  ds_ren_bca_corp_niv_6,
	BC.des_bca_corp_niv_7  ds_ren_bca_corp_niv_7,
	BC.des_bca_corp_niv_8  ds_ren_bca_corp_niv_8,
	BC.des_bca_corp_niv_9  ds_ren_bca_corp_niv_10,
	BC.des_bca_corp_niv_10  ds_ren_bca_corp_niv_11,
	BC.des_bca_corp_niv_11  ds_ren_bca_corp_niv_12,
	BC.des_bca_corp_niv_12  ds_ren_bca_corp_niv_13,
	BC.des_bca_corp_niv_13  ds_ren_bca_corp_niv_14,
	BC.des_bca_corp_niv_14  ds_ren_bca_corp_niv_15,
	BC.partition_date  partition_date
FROM
	bi_corp_staging.rio157_ms0_dm_dwh_bca_corp_ BC
WHERE
	BC.partition_date = '2020-07-10' */


-- dim_ren_centro
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_ren_centro (
    cod_ren_pais INT ,
    cod_ren_centro_cont STRING ,
	cod_ren_centro_niv_1 STRING ,
	cod_ren_centro_niv_2 STRING ,
	cod_ren_centro_niv_3 STRING ,
	cod_ren_centro_niv_4 STRING ,
	cod_ren_centro_niv_5 STRING ,
	cod_ren_centro_niv_6 STRING ,
	cod_ren_centro_niv_7 STRING ,
	cod_ren_centro_niv_8 STRING ,
	cod_ren_centro_niv_9 STRING ,
	cod_ren_centro_niv_10 STRING ,
	cod_ren_centro_niv_11 STRING ,
	cod_ren_centro_niv_12 STRING ,
	cod_ren_centro_niv_13 STRING ,
	cod_ren_centro_niv_14 STRING ,
	ds_ren_centro_niv_1 STRING ,
	ds_ren_centro_niv_2 STRING ,
	ds_ren_centro_niv_3 STRING ,
	ds_ren_centro_niv_4 STRING ,
	ds_ren_centro_niv_5 STRING ,
	ds_ren_centro_niv_6 STRING ,
	ds_ren_centro_niv_7 STRING ,
	ds_ren_centro_niv_8 STRING ,
	ds_ren_centro_niv_9 STRING ,
	ds_ren_centro_niv_10 STRING ,
	ds_ren_centro_niv_11 STRING ,
	ds_ren_centro_niv_12 STRING ,
	ds_ren_centro_niv_13 STRING ,
	ds_ren_centro_niv_14 STRING ,
	partition_date STRING )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/santander/bi-corp/common/rentabilidad/dim/dim_ren_centro';
/*
INSERT
	OVERWRITE TABLE bi_corp_common.dim_ren_centro
SELECT
	c.cod_pais cod_ren_pais,
	c.cod_centro_cont cod_ren_centro_cont,
	c.cod_centro_cont_niv_1 cod_ren_centro_cont_niv_1,
	c.cod_centro_cont_niv_2 cod_ren_centro_cont_niv_2,
	c.cod_centro_cont_niv_3 cod_ren_centro_cont_niv_3,
	c.cod_centro_cont_niv_4 cod_ren_centro_cont_niv_4,
	c.cod_centro_cont_niv_5 cod_ren_centro_cont_niv_5,
	c.cod_centro_cont_niv_6 cod_ren_centro_cont_niv_6,
	c.cod_centro_cont_niv_7 cod_ren_centro_cont_niv_7,
	c.cod_centro_cont_niv_8 cod_ren_centro_cont_niv_8,
	c.cod_centro_cont_niv_9 cod_ren_centro_cont_niv_9,
	c.cod_centro_cont_niv_10 cod_ren_centro_cont_niv_10,
	c.cod_centro_cont_niv_11 cod_ren_centro_cont_niv_11,
	c.cod_centro_cont_niv_12 cod_ren_centro_cont_niv_12,
	c.cod_centro_cont_niv_13 cod_ren_centro_cont_niv_13,
	c.cod_centro_cont_niv_14 cod_ren_centro_cont_niv_14,
	c.des_centro_cont_niv_1 ds_ren_centro_cont_niv_1,
	c.des_centro_cont_niv_2 ds_ren_centro_cont_niv_2,
	c.des_centro_cont_niv_3 ds_ren_centro_cont_niv_3,
	c.des_centro_cont_niv_4 ds_ren_centro_cont_niv_4,
	c.des_centro_cont_niv_5 ds_ren_centro_cont_niv_5,
	c.des_centro_cont_niv_6 ds_ren_centro_cont_niv_6,
	c.des_centro_cont_niv_7 ds_ren_centro_cont_niv_7,
	c.des_centro_cont_niv_8 ds_ren_centro_cont_niv_8,
	c.des_centro_cont_niv_9 ds_ren_centro_cont_niv_9,
	c.des_centro_cont_niv_10 ds_ren_centro_cont_niv_10,
	c.des_centro_cont_niv_11 ds_ren_centro_cont_niv_11,
	c.des_centro_cont_niv_12 ds_ren_centro_cont_niv_12,
	c.des_centro_cont_niv_13 ds_ren_centro_cont_niv_13,
	c.des_centro_cont_niv_14 ds_ren_centro_cont_niv_14,
	c.partition_date partition_date
FROM
	bi_corp_staging.rio157_ms0_dm_dwh_centro_ c
WHERE
	c.partition_date = '2020-07-18'
*/


-- dim_ren_cobrabilidad
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_ren_cobrabilidad (
    cod_ren_pais INT ,
    cod_ren_cobrabilidad STRING ,
	cod_ren_cobrabilidad_niv_1 STRING ,
	cod_ren_cobrabilidad_niv_2 STRING ,
	cod_ren_cobrabilidad_niv_3 STRING ,
	cod_ren_cobrabilidad_niv_4 STRING ,
	cod_ren_cobrabilidad_niv_5 STRING ,
	cod_ren_cobrabilidad_niv_6 STRING ,
	cod_ren_cobrabilidad_niv_7 STRING ,
	cod_ren_cobrabilidad_niv_8 STRING ,
	cod_ren_cobrabilidad_niv_9 STRING ,
	cod_ren_cobrabilidad_niv_10 STRING ,
	cod_ren_cobrabilidad_niv_11 STRING ,
	cod_ren_cobrabilidad_niv_12 STRING ,
	cod_ren_cobrabilidad_niv_13 STRING ,
	cod_ren_cobrabilidad_niv_14 STRING ,
	ds_ren_cobrabilidad_niv_1 STRING ,
	ds_ren_cobrabilidad_niv_2 STRING ,
	ds_ren_cobrabilidad_niv_3 STRING ,
	ds_ren_cobrabilidad_niv_4 STRING ,
	ds_ren_cobrabilidad_niv_5 STRING ,
	ds_ren_cobrabilidad_niv_6 STRING ,
	ds_ren_cobrabilidad_niv_7 STRING ,
	ds_ren_cobrabilidad_niv_8 STRING ,
	ds_ren_cobrabilidad_niv_9 STRING ,
	ds_ren_cobrabilidad_niv_10 STRING ,
	ds_ren_cobrabilidad_niv_11 STRING ,
	ds_ren_cobrabilidad_niv_12 STRING ,
	ds_ren_cobrabilidad_niv_13 STRING ,
	ds_ren_cobrabilidad_niv_14 STRING ,
	partition_date STRING )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/santander/bi-corp/common/rentabilidad/dim/dim_ren_cobrabilidad';

/*
set hive.execution.engine=spark;
set spark.yarn.queue=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_ren_cobrabilidad
SELECT
	C.cod_pais cod_ren_pais,
	C.cod_cobrabilidad cod_ren_cobrabilidad,
	C.cod_cobrabilidad_niv_1 cod_ren_cobrabilidad_niv_1,
	C.cod_cobrabilidad_niv_2 cod_ren_cobrabilidad_niv_2,
	C.cod_cobrabilidad_niv_3 cod_ren_cobrabilidad_niv_3,
	C.cod_cobrabilidad_niv_4 cod_ren_cobrabilidad_niv_4,
	C.cod_cobrabilidad_niv_5 cod_ren_cobrabilidad_niv_5,
	C.cod_cobrabilidad_niv_6 cod_ren_cobrabilidad_niv_6,
	C.cod_cobrabilidad_niv_7 cod_ren_cobrabilidad_niv_7,
	C.cod_cobrabilidad_niv_8 cod_ren_cobrabilidad_niv_8,
	C.cod_cobrabilidad_niv_9 cod_ren_cobrabilidad_niv_9,
	C.cod_cobrabilidad_niv_10 cod_ren_cobrabilidad_niv_10,
	C.cod_cobrabilidad_niv_11 cod_ren_cobrabilidad_niv_11,
	C.cod_cobrabilidad_niv_12 cod_ren_cobrabilidad_niv_12,
	C.cod_cobrabilidad_niv_13 cod_ren_cobrabilidad_niv_13,
	C.cod_cobrabilidad_niv_14 cod_ren_cobrabilidad_niv_14,
	C.des_cobrabilidad_niv_1 ds_ren_cobrabilidad_niv_1,
	C.des_cobrabilidad_niv_2 ds_ren_cobrabilidad_niv_2,
	C.des_cobrabilidad_niv_3 ds_ren_cobrabilidad_niv_3,
	C.des_cobrabilidad_niv_4 ds_ren_cobrabilidad_niv_4,
	C.des_cobrabilidad_niv_5 ds_ren_cobrabilidad_niv_5,
	C.des_cobrabilidad_niv_6 ds_ren_cobrabilidad_niv_6,
	C.des_cobrabilidad_niv_7 ds_ren_cobrabilidad_niv_7,
	C.des_cobrabilidad_niv_8 ds_ren_cobrabilidad_niv_8,
	C.des_cobrabilidad_niv_9 ds_ren_cobrabilidad_niv_9,
	C.des_cobrabilidad_niv_10 ds_ren_cobrabilidad_niv_10,
	C.des_cobrabilidad_niv_11 ds_ren_cobrabilidad_niv_11,
	C.des_cobrabilidad_niv_12 ds_ren_cobrabilidad_niv_12,
	C.des_cobrabilidad_niv_13 ds_ren_cobrabilidad_niv_13,
	C.des_cobrabilidad_niv_14 ds_ren_cobrabilidad_niv_14,
	C.partition_date partition_date
FROM
	bi_corp_staging.rio157_ms0_dm_dwh_cobrabilidad_ C
WHERE
	C.partition_date = '2020-07-18'
*/


-- dim_ren_cta_resultados
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_ren_cta_resultados (
    cod_ren_pais INT ,
    cod_ren_cta_resultados STRING ,
	cod_ren_cta_resultados_niv_1 STRING ,
	cod_ren_cta_resultados_niv_2 STRING ,
	cod_ren_cta_resultados_niv_3 STRING ,
	cod_ren_cta_resultados_niv_4 STRING ,
	cod_ren_cta_resultados_niv_5 STRING ,
	cod_ren_cta_resultados_niv_6 STRING ,
	cod_ren_cta_resultados_niv_7 STRING ,
	cod_ren_cta_resultados_niv_8 STRING ,
	cod_ren_cta_resultados_niv_9 STRING ,
	cod_ren_cta_resultados_niv_10 STRING ,
	cod_ren_cta_resultados_niv_11 STRING ,
	cod_ren_cta_resultados_niv_12 STRING ,
	cod_ren_cta_resultados_niv_13 STRING ,
	cod_ren_cta_resultados_niv_14 STRING ,
	ds_ren_cta_resultados_niv_1 STRING ,
	ds_ren_cta_resultados_niv_2 STRING ,
	ds_ren_cta_resultados_niv_3 STRING ,
	ds_ren_cta_resultados_niv_4 STRING ,
	ds_ren_cta_resultados_niv_5 STRING ,
	ds_ren_cta_resultados_niv_6 STRING ,
	ds_ren_cta_resultados_niv_7 STRING ,
	ds_ren_cta_resultados_niv_8 STRING ,
	ds_ren_cta_resultados_niv_9 STRING ,
	ds_ren_cta_resultados_niv_10 STRING ,
	ds_ren_cta_resultados_niv_11 STRING ,
	ds_ren_cta_resultados_niv_12 STRING ,
	ds_ren_cta_resultados_niv_13 STRING ,
	ds_ren_cta_resultados_niv_14 STRING ,
	partition_date STRING )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/santander/bi-corp/common/rentabilidad/dim/dim_ren_cta_resultados';

/*
INSERT
	OVERWRITE TABLE bi_corp_common.dim_ren_cta_resultados
SELECT
	cr.cod_pais cod_ren_cod_pais,
	cr.cod_cta_resultados cod_ren_cta_resultados,
	cr.cod_cta_resultados_niv_1 cod_ren_cta_resultados_niv_1,
	cr.cod_cta_resultados_niv_2 cod_ren_cta_resultados_niv_2,
	cr.cod_cta_resultados_niv_3 cod_ren_cta_resultados_niv_3,
	cr.cod_cta_resultados_niv_4 cod_ren_cta_resultados_niv_4,
	cr.cod_cta_resultados_niv_5 cod_ren_cta_resultados_niv_5,
	cr.cod_cta_resultados_niv_6 cod_ren_cta_resultados_niv_6,
	cr.cod_cta_resultados_niv_7 cod_ren_cta_resultados_niv_7,
	cr.cod_cta_resultados_niv_8 cod_ren_cta_resultados_niv_8,
	cr.cod_cta_resultados_niv_9 cod_ren_cta_resultados_niv_9,
	cr.cod_cta_resultados_niv_10 cod_ren_cta_resultados_niv_10,
	cr.cod_cta_resultados_niv_11 cod_ren_cta_resultados_niv_11,
	cr.cod_cta_resultados_niv_12 cod_ren_cta_resultados_niv_12,
	cr.cod_cta_resultados_niv_13 cod_ren_cta_resultados_niv_13,
	cr.cod_cta_resultados_niv_14 cod_ren_cta_resultados_niv_14,
	cr.des_cta_resultados_niv_1 ds_ren_cta_resultados_niv_1,
	cr.des_cta_resultados_niv_2 ds_ren_cta_resultados_niv_2,
	cr.des_cta_resultados_niv_3 ds_ren_cta_resultados_niv_3,
	cr.des_cta_resultados_niv_4 ds_ren_cta_resultados_niv_4,
	cr.des_cta_resultados_niv_5 ds_ren_cta_resultados_niv_5,
	cr.des_cta_resultados_niv_6 ds_ren_cta_resultados_niv_6,
	cr.des_cta_resultados_niv_7 ds_ren_cta_resultados_niv_7,
	cr.des_cta_resultados_niv_8 ds_ren_cta_resultados_niv_8,
	cr.des_cta_resultados_niv_9 ds_ren_cta_resultados_niv_9,
	cr.des_cta_resultados_niv_10 ds_ren_cta_resultados_niv_10,
	cr.des_cta_resultados_niv_11 ds_ren_cta_resultados_niv_11,
	cr.des_cta_resultados_niv_12 ds_ren_cta_resultados_niv_12,
	cr.des_cta_resultados_niv_13 ds_ren_cta_resultados_niv_13,
	cr.des_cta_resultados_niv_14 ds_ren_cta_resultados_niv_14,
	cr.partition_date partition_date
FROM
	bi_corp_staging.rio157_ms0_dm_dwh_cta_resultados_ cr
WHERE
	UPPER(cr.COD_JERQ_CR01) = 'JCNBV'
	AND cr.partition_date = '2020-07-18'
*/


-- dim_ren_grupos_globales
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_ren_grupos_globales (
    cod_ren_pais INT ,
    cod_ren_grupo_cliente STRING ,
	cod_ren_grupo_cliente_niv_1 STRING ,
	cod_ren_grupo_cliente_niv_2 STRING ,
	cod_ren_grupo_cliente_niv_3 STRING ,
	cod_ren_grupo_cliente_niv_4 STRING ,
	cod_ren_grupo_cliente_niv_5 STRING ,
	cod_ren_grupo_cliente_niv_6 STRING ,
	cod_ren_grupo_cliente_niv_7 STRING ,
	cod_ren_grupo_cliente_niv_8 STRING ,
	cod_ren_grupo_cliente_niv_9 STRING ,
	cod_ren_grupo_cliente_niv_10 STRING ,
	cod_ren_grupo_cliente_niv_11 STRING ,
	cod_ren_grupo_cliente_niv_12 STRING ,
	cod_ren_grupo_cliente_niv_13 STRING ,
	cod_ren_grupo_cliente_niv_14 STRING ,
	ds_ren_grupo_cliente_niv_1 STRING ,
	ds_ren_grupo_cliente_niv_2 STRING ,
	ds_ren_grupo_cliente_niv_3 STRING ,
	ds_ren_grupo_cliente_niv_4 STRING ,
	ds_ren_grupo_cliente_niv_5 STRING ,
	ds_ren_grupo_cliente_niv_6 STRING ,
	ds_ren_grupo_cliente_niv_7 STRING ,
	ds_ren_grupo_cliente_niv_8 STRING ,
	ds_ren_grupo_cliente_niv_9 STRING ,
	ds_ren_grupo_cliente_niv_10 STRING ,
	ds_ren_grupo_cliente_niv_11 STRING ,
	ds_ren_grupo_cliente_niv_12 STRING ,
	ds_ren_grupo_cliente_niv_13 STRING ,
	ds_ren_grupo_cliente_niv_14 STRING ,
	partition_date STRING )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/santander/bi-corp/common/rentabilidad/dim/dim_ren_grupos_globales';

/*
INSERT
	OVERWRITE TABLE bi_corp_common.dim_ren_grupos_globales
SELECT
	GG.cod_pais cod_ren_pais,
	GG.cod_grupo_cliente cod_ren_grupo_cliente,
	GG.cod_grupo_cliente_niv_1 cod_ren_grupo_cliente_niv_1,
	GG.cod_grupo_cliente_niv_2 cod_ren_grupo_cliente_niv_2,
	GG.cod_grupo_cliente_niv_3 cod_ren_grupo_cliente_niv_3,
	GG.cod_grupo_cliente_niv_4 cod_ren_grupo_cliente_niv_4,
	GG.cod_grupo_cliente_niv_5 cod_ren_grupo_cliente_niv_5,
	GG.cod_grupo_cliente_niv_6 cod_ren_grupo_cliente_niv_6,
	GG.cod_grupo_cliente_niv_7 cod_ren_grupo_cliente_niv_7,
	GG.cod_grupo_cliente_niv_8 cod_ren_grupo_cliente_niv_8,
	GG.cod_grupo_cliente_niv_9 cod_ren_grupo_cliente_niv_9,
	GG.cod_grupo_cliente_niv_10 cod_ren_grupo_cliente_niv_10,
	GG.cod_grupo_cliente_niv_11 cod_ren_grupo_cliente_niv_11,
	GG.cod_grupo_cliente_niv_12 cod_ren_grupo_cliente_niv_12,
	GG.cod_grupo_cliente_niv_13 cod_ren_grupo_cliente_niv_13,
	GG.cod_grupo_cliente_niv_14 cod_ren_grupo_cliente_niv_14,
	GG.des_grupo_cliente_niv_1 ds_ren_grupo_cliente_niv_1,
	GG.des_grupo_cliente_niv_2 ds_ren_grupo_cliente_niv_2,
	GG.des_grupo_cliente_niv_3 ds_ren_grupo_cliente_niv_3,
	GG.des_grupo_cliente_niv_4 ds_ren_grupo_cliente_niv_4,
	GG.des_grupo_cliente_niv_5 ds_ren_grupo_cliente_niv_5,
	GG.des_grupo_cliente_niv_6 ds_ren_grupo_cliente_niv_6,
	GG.des_grupo_cliente_niv_7 ds_ren_grupo_cliente_niv_7,
	GG.des_grupo_cliente_niv_8 ds_ren_grupo_cliente_niv_8,
	GG.des_grupo_cliente_niv_9 ds_ren_grupo_cliente_niv_9,
	GG.des_grupo_cliente_niv_10 ds_ren_grupo_cliente_niv_10,
	GG.des_grupo_cliente_niv_11 ds_ren_grupo_cliente_niv_11,
	GG.des_grupo_cliente_niv_12 ds_ren_grupo_cliente_niv_12,
	GG.des_grupo_cliente_niv_13 ds_ren_grupo_cliente_niv_13,
	GG.des_grupo_cliente_niv_14 ds_ren_grupo_cliente_niv_14,
	GG.partition_date partition_date
FROM
	bi_corp_staging.rio157_ms0_dm_dwh_grupos_globales_ GG
WHERE
	GG.partition_date = '2020-07-18'
*/


-- dim_ren_oficinas
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_ren_oficinas (
    cod_ren_pais INT ,
    cod_ren_ofi_comercial STRING ,
	cod_ren_ofi_comercial_niv_1 STRING ,
	cod_ren_ofi_comercial_niv_2 STRING ,
	cod_ren_ofi_comercial_niv_3 STRING ,
	cod_ren_ofi_comercial_niv_4 STRING ,
	cod_ren_ofi_comercial_niv_5 STRING ,
	cod_ren_ofi_comercial_niv_6 STRING ,
	cod_ren_ofi_comercial_niv_7 STRING ,
	cod_ren_ofi_comercial_niv_8 STRING ,
	cod_ren_ofi_comercial_niv_9 STRING ,
	cod_ren_ofi_comercial_niv_10 STRING ,
	cod_ren_ofi_comercial_niv_11 STRING ,
	cod_ren_ofi_comercial_niv_12 STRING ,
	cod_ren_ofi_comercial_niv_13 STRING ,
	cod_ren_ofi_comercial_niv_14 STRING ,
	ds_ren_ofi_comercial_niv_1 STRING ,
	ds_ren_ofi_comercial_niv_2 STRING ,
	ds_ren_ofi_comercial_niv_3 STRING ,
	ds_ren_ofi_comercial_niv_4 STRING ,
	ds_ren_ofi_comercial_niv_5 STRING ,
	ds_ren_ofi_comercial_niv_6 STRING ,
	ds_ren_ofi_comercial_niv_7 STRING ,
	ds_ren_ofi_comercial_niv_8 STRING ,
	ds_ren_ofi_comercial_niv_9 STRING ,
	ds_ren_ofi_comercial_niv_10 STRING ,
	ds_ren_ofi_comercial_niv_11 STRING ,
	ds_ren_ofi_comercial_niv_12 STRING ,
	ds_ren_ofi_comercial_niv_13 STRING ,
	ds_ren_ofi_comercial_niv_14 STRING ,
	partition_date STRING )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/santander/bi-corp/common/rentabilidad/dim/dim_ren_oficinas';

/*
INSERT
	OVERWRITE TABLE bi_corp_common.dim_ren_oficinas
SELECT
	offi.cod_pais cod_ren_pais,
	offi.cod_ofi_comercial cod_ren_ofi_comercial,
	offi.cod_ofi_comercial_niv_1 cod_ren_ofi_comercial_niv_1,
	offi.cod_ofi_comercial_niv_2 cod_ren_ofi_comercial_niv_2,
	offi.cod_ofi_comercial_niv_3 cod_ren_ofi_comercial_niv_3,
	offi.cod_ofi_comercial_niv_4 cod_ren_ofi_comercial_niv_4,
	offi.cod_ofi_comercial_niv_5 cod_ren_ofi_comercial_niv_5,
	offi.cod_ofi_comercial_niv_6 cod_ren_ofi_comercial_niv_6,
	offi.cod_ofi_comercial_niv_7 cod_ren_ofi_comercial_niv_7,
	offi.cod_ofi_comercial_niv_8 cod_ren_ofi_comercial_niv_8,
	offi.cod_ofi_comercial_niv_9 cod_ren_ofi_comercial_niv_9,
	offi.cod_ofi_comercial_niv_10 cod_ren_ofi_comercial_niv_10,
	offi.cod_ofi_comercial_niv_11 cod_ren_ofi_comercial_niv_11,
	offi.cod_ofi_comercial_niv_12 cod_ren_ofi_comercial_niv_12,
	offi.cod_ofi_comercial_niv_13 cod_ren_ofi_comercial_niv_13,
	offi.cod_ofi_comercial_niv_14 cod_ren_ofi_comercial_niv_14,
	offi.des_ofi_comercial_niv_1 ds_ren_ofi_comercial_niv_1,
	offi.des_ofi_comercial_niv_2 ds_ren_ofi_comercial_niv_2,
	offi.des_ofi_comercial_niv_3 ds_ren_ofi_comercial_niv_3,
	offi.des_ofi_comercial_niv_4 ds_ren_ofi_comercial_niv_4,
	offi.des_ofi_comercial_niv_5 ds_ofi_comercial_niv_5,
	offi.des_ofi_comercial_niv_6 ds_ren_ofi_comercial_niv_6,
	offi.des_ofi_comercial_niv_7 ds_ren_ofi_comercial_niv_7,
	offi.des_ofi_comercial_niv_8 ds_ren_ofi_comercial_niv_8,
	offi.des_ofi_comercial_niv_9 ds_ren_ofi_comercial_niv_9,
	offi.des_ofi_comercial_niv_10 ds_ren_ofi_comercial_niv_10,
	offi.des_ofi_comercial_niv_11 ds_ren_ofi_comercial_niv_11,
	offi.des_ofi_comercial_niv_12 ds_ren_ofi_comercial_niv_12,
	offi.des_ofi_comercial_niv_13 ds_ren_ofi_comercial_niv_13,
	offi.des_ofi_comercial_niv_14 ds_ren_ofi_comercial_niv_14,
	offi.partition_date partition_date
FROM
	bi_corp_staging.rio157_ms0_dm_dwh_oficina offi
WHERE
	UPPER(offi.cod_jerq_oc01) = 'JOC01'
	AND offi.partition_date = '2020-07-10'
*/


-- dim_ren_productos
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_ren_productos (
    cod_ren_pais INT ,
    cod_ren_producto STRING ,
	cod_ren_producto_niv_1 STRING ,
	cod_ren_producto_niv_2 STRING ,
	cod_ren_producto_niv_3 STRING ,
	cod_ren_producto_niv_4 STRING ,
	cod_ren_producto_niv_5 STRING ,
	cod_ren_producto_niv_6 STRING ,
	cod_ren_producto_niv_7 STRING ,
	cod_ren_producto_niv_8 STRING ,
	cod_ren_producto_niv_9 STRING ,
	cod_ren_producto_niv_10 STRING ,
	cod_ren_producto_niv_11 STRING ,
	cod_ren_producto_niv_12 STRING ,
	cod_ren_producto_niv_13 STRING ,
	cod_ren_producto_niv_14 STRING ,
	ds_ren_producto_niv_1 STRING ,
	ds_ren_producto_niv_2 STRING ,
	ds_ren_producto_niv_3 STRING ,
	ds_ren_producto_niv_4 STRING ,
	ds_ren_producto_niv_5 STRING ,
	ds_ren_producto_niv_6 STRING ,
	ds_ren_producto_niv_7 STRING ,
	ds_ren_producto_niv_8 STRING ,
	ds_ren_producto_niv_9 STRING ,
	ds_ren_producto_niv_10 STRING ,
	ds_ren_producto_niv_11 STRING ,
	ds_ren_producto_niv_12 STRING ,
	ds_ren_producto_niv_13 STRING ,
	ds_ren_producto_niv_14 STRING ,
	partition_date STRING )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/santander/bi-corp/common/rentabilidad/dim/dim_ren_productos';


/*
set hive.execution.engine=spark;
set spark.yarn.queue=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_ren_productos
SELECT
	PROD.cod_pais cod_ren_pais,
	PROD.cod_producto cod_ren_producto,
	PROD.cod_producto_niv_1 cod_ren_producto_niv_1,
	PROD.cod_producto_niv_2 cod_ren_producto_niv_2,
	PROD.cod_producto_niv_3 cod_ren_producto_niv_3,
	PROD.cod_producto_niv_4 cod_ren_producto_niv_4,
	PROD.cod_producto_niv_5 cod_ren_producto_niv_5,
	PROD.cod_producto_niv_6 cod_ren_producto_niv_6,
	PROD.cod_producto_niv_7 cod_ren_producto_niv_7,
	PROD.cod_producto_niv_8 cod_ren_producto_niv_8,
	PROD.cod_producto_niv_9 cod_ren_producto_niv_9,
	PROD.cod_producto_niv_10 cod_ren_producto_niv_10,
	PROD.cod_producto_niv_11 cod_ren_producto_niv_11,
	PROD.cod_producto_niv_12 cod_ren_producto_niv_12,
	PROD.cod_producto_niv_13 cod_ren_producto_niv_13,
	PROD.cod_producto_niv_14 cod_ren_producto_niv_14,
	PROD.des_producto_niv_1 ds_ren_producto_niv_1,
	PROD.des_producto_niv_2 ds_ren_producto_niv_2,
	PROD.des_producto_niv_3 ds_ren_producto_niv_3,
	PROD.des_producto_niv_4 ds_ren_producto_niv_4,
	PROD.des_producto_niv_5 ds_ren_producto_niv_5,
	PROD.des_producto_niv_6 ds_ren_producto_niv_6,
	PROD.des_producto_niv_7 ds_ren_producto_niv_7,
	PROD.des_producto_niv_8 ds_ren_producto_niv_8,
	PROD.des_producto_niv_9 ds_ren_producto_niv_9,
	PROD.des_producto_niv_10 ds_ren_producto_niv_10,
	PROD.des_producto_niv_11 ds_ren_producto_niv_11,
	PROD.des_producto_niv_12 ds_ren_producto_niv_12,
	PROD.des_producto_niv_13 ds_ren_producto_niv_13,
	PROD.des_producto_niv_14 ds_ren_producto_niv_14,
	PROD.partition_date partition_date
FROM
	bi_corp_staging.rio157_ms0_dm_dwh_productos_ PROD
WHERE
	(UPPER(PROD.cod_jerq_pr01) = 'JBLDN'
	OR UPPER(PROD.cod_jerq_pr01) = 'JBC01')
	AND PROD.partition_date = '2020-07-18'
*/


-- dim_ren_tipo_ajuste
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_ren_tipo_ajuste (
    cod_ren_pais INT ,
    cod_ren_tipo_ajuste STRING ,
	cod_ren_tipo_ajuste_niv_1 STRING ,
	cod_ren_tipo_ajuste_niv_2 STRING ,
	cod_ren_tipo_ajuste_niv_3 STRING ,
	cod_ren_tipo_ajuste_niv_4 STRING ,
	cod_ren_tipo_ajuste_niv_5 STRING ,
	cod_ren_tipo_ajuste_niv_6 STRING ,
	cod_ren_tipo_ajuste_niv_7 STRING ,
	cod_ren_tipo_ajuste_niv_8 STRING ,
	cod_ren_tipo_ajuste_niv_9 STRING ,
	cod_ren_tipo_ajuste_niv_10 STRING ,
	cod_ren_tipo_ajuste_niv_11 STRING ,
	cod_ren_tipo_ajuste_niv_12 STRING ,
	cod_ren_tipo_ajuste_niv_13 STRING ,
	cod_ren_tipo_ajuste_niv_14 STRING ,
	ds_ren_tipo_ajuste_niv_1 STRING ,
	ds_ren_tipo_ajuste_niv_2 STRING ,
	ds_ren_tipo_ajuste_niv_3 STRING ,
	ds_ren_tipo_ajuste_niv_4 STRING ,
	ds_ren_tipo_ajuste_niv_5 STRING ,
	ds_ren_tipo_ajuste_niv_6 STRING ,
	ds_ren_tipo_ajuste_niv_7 STRING ,
	ds_ren_tipo_ajuste_niv_8 STRING ,
	ds_ren_tipo_ajuste_niv_9 STRING ,
	ds_ren_tipo_ajuste_niv_10 STRING ,
	ds_ren_tipo_ajuste_niv_11 STRING ,
	ds_ren_tipo_ajuste_niv_12 STRING ,
	ds_ren_tipo_ajuste_niv_13 STRING ,
	ds_ren_tipo_ajuste_niv_14 STRING ,
	partition_date STRING )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/santander/bi-corp/common/rentabilidad/dim/dim_ren_tipo_ajuste';

/*
INSERT
	OVERWRITE TABLE bi_corp_common.dim_ren_tipo_ajuste
SELECT
	TA.cod_pais cod_ren_pais,
	TA.cod_tip_ajuste cod_ren_tipo_ajuste,
	TA.cod_tip_ajuste_niv_1 cod_ren_tipo_ajuste_niv_1,
	TA.cod_tip_ajuste_niv_2 cod_ren_tipo_ajuste_niv_2,
	TA.cod_tip_ajuste_niv_3 cod_ren_tipo_ajuste_niv_3,
	TA.cod_tip_ajuste_niv_4 cod_ren_tipo_ajuste_niv_4,
	TA.cod_tip_ajuste_niv_5 cod_ren_tipo_ajuste_niv_5,
	TA.cod_tip_ajuste_niv_6 cod_ren_tipo_ajuste_niv_6,
	TA.cod_tip_ajuste_niv_7 cod_ren_tipo_ajuste_niv_7,
	TA.cod_tip_ajuste_niv_8 cod_ren_tipo_ajuste_niv_8,
	TA.cod_tip_ajuste_niv_9 cod_ren_tipo_ajuste_niv_9,
	TA.cod_tip_ajuste_niv_10 cod_ren_tipo_ajuste_niv_10,
	TA.cod_tip_ajuste_niv_11 cod_ren_tipo_ajuste_niv_11,
	TA.cod_tip_ajuste_niv_12 cod_ren_tipo_ajuste_niv_12,
	TA.cod_tip_ajuste_niv_13 cod_ren_tipo_ajuste_niv_13,
	TA.cod_tip_ajuste_niv_14 cod_ren_tipo_ajuste_niv_14,
	TA.des_tip_ajuste_niv_1 ds_ren_tipo_ajuste_niv_1,
	TA.des_tip_ajuste_niv_2 ds_ren_tipo_ajuste_niv_2,
	TA.des_tip_ajuste_niv_3 ds_ren_tipo_ajuste_niv_3,
	TA.des_tip_ajuste_niv_4 ds_ren_tipo_ajuste_niv_4,
	TA.des_tip_ajuste_niv_5 ds_ren_tipo_ajuste_niv_5,
	TA.des_tip_ajuste_niv_6 ds_ren_tipo_ajuste_niv_6,
	TA.des_tip_ajuste_niv_7 ds_ren_tipo_ajuste_niv_7,
	TA.des_tip_ajuste_niv_8 ds_ren_tipo_ajuste_niv_8,
	TA.des_tip_ajuste_niv_9 ds_ren_tipo_ajuste_niv_9,
	TA.des_tip_ajuste_niv_10 ds_ren_tipo_ajuste_niv_10,
	TA.des_tip_ajuste_niv_11 ds_ren_tipo_ajuste_niv_11,
	TA.des_tip_ajuste_niv_12 ds_ren_tipo_ajuste_niv_12,
	TA.des_tip_ajuste_niv_13 ds_ren_tipo_ajuste_niv_13,
	TA.des_tip_ajuste_niv_14 ds_ren_tipo_ajuste_niv_14,
	TA.partition_date partition_date
FROM
	bi_corp_sTAging.rio157_ms0_dm_dwh_tipo_ajuste_ TA
WHERE
	TA.partition_date = '2020-07-18'
*/