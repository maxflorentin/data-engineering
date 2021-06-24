CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cyp_documentospagados(
cod_cyp_ide_pgo       			   string,
ds_cyp_tpo_cprb                    string,
ds_cyp_nro_cprb                    string,
ds_cyp_nro_cuo                     string,
cod_cyp_nro_empr                   bigint,
cod_cyp_nro_digempr                int,
cod_cyp_nro_prod                   int,
ds_cyp_nro_instan                  int,
cod_cyp_nro_clte                   string,
dt_cyp_fec_vto                     string,
fc_cyp_sldo_ant	                   decimal(15,2),
dt_cyp_fec_pgoant                  string,
ds_cyp_tsa_punitorios              decimal(5,2),
fc_cyp_imp_difvto                  decimal(15,2),
fc_cyp_imp_puni                    decimal(15,2),
fc_cyp_imp_dto                     decimal(15,2),
fc_cyp_imp_pgo                     decimal(15,2),
ds_cyp_obs1                        string,
ds_cyp_obs2                        string,
ds_cyp_obs3                        string,
cod_cyp_mone                       int
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/cyp/fact/stk_cyp_documentospagados'