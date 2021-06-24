
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.rel_cyp_pagos(
cod_cyp_ide_pgoorigen               string,
cod_cyp_form_pgo                    int,
cod_cyp_moneda                      int,
ds_cyp_nro_instr                    string,
cod_cyp_ide_pgodestino              string,
ds_cyp_tpo_relacion                 string,
cod_cyp_nro_emprorigen              bigint,
cod_cyp_nro_digorigen               int,
cod_cyp_nro_prodorigen              int,
ds_cyp_nro_intanorigen              int,
cod_cyp_nro_emprdestino             bigint,
cod_cyp_nro_digdestino              int,
ds_cyp_nro_intandestino             int,
dt_cyp_pgo_origen                   string,
dt_cyp_pgo_destino                  string,
ds_cyp_cuit_clte                    bigint,
cod_cyp_est_relacion                string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/cyp/fact/rel_cyp_pagos'