    CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_adm_ind_estadosasol (
    cod_adm_estado int,
    ds_adm_descripcionestado string,
    ds_adm_marcaestadoterminal string
    )
    PARTITIONED BY (
      partition_date string)
    STORED AS PARQUET
    LOCATION
      '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/dim_adm_ind_estadosasol'
    ;