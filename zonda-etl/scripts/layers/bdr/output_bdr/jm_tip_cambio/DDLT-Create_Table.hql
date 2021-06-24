CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_tip_cambio(
n0738_feoperac string,
n0738_coddiv string,
n0738_coddiv2 string,
n0738_ortipcam string,
n0738_tpcam string,
n0738_fecultmo string
)
PARTITIONED BY (partition_date string)
STORED AS PARQUET
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/output_bdr/jm_tip_cambio';