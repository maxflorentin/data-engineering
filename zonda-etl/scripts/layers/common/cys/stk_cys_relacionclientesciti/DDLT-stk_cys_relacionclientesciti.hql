CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cys_relacionclientesciti (
  cod_per_nup BIGINT ,
  cod_cys_identorigen STRING ,
  cod_cys_entidadorigen STRING )
PARTITIONED BY (partition_date string)
STORED AS PARQUET
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/cys/stk_cys_relacionclientesciti' ;