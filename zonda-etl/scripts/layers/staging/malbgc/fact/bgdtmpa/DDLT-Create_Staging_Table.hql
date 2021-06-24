CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malbgc_bgdtmpa(
mpa_entidad 		STRING,
mpa_centro_alta 	STRING,
mpa_paquete 		STRING,
mpa_producto_paq 	STRING,
mpa_subprodu_paq 	STRING,
mpa_entidad_inv 	STRING,
mpa_centro_inv 		STRING,
mpa_cuenta_inv 		STRING,
mpa_entidad_contab 	STRING,
mpa_centro_contab 	STRING,
mpa_cod_plan 		STRING,
mpa_indesta 		STRING,
mpa_cod_sop_ext 	STRING,
mpa_pco_ecu_fhpr 	STRING,
mpa_fecha_alta 		STRING,
mpa_fecha_cancel 	STRING,
mpa_fecha_upgrade 	STRING,
mpa_fecha_downgr 	STRING,
mpa_ind_inhab_sbrg 	STRING,
mpa_cod_envio_mov 	STRING,
mpa_calpar_envio_mov STRING,
mpa_ordpar_envio_mov INT,
mpa_entidad_umo 	STRING,
mpa_centro_umo 		STRING,
mpa_userid_umo 		STRING,
mpa_netname_umo 	STRING,
mpa_timest_umo 		STRING
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malbgc/fact/bgdtmpa/consolidated';