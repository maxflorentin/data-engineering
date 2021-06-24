CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.afma_wasdo12(
     wasdo12_tipo_cuenta int,
     wasdo12_nro_cuenta bigint,
     wasdo12_f_pago_aa1 int,
     wasdo12_f_pago_aa2 int,
     wasdo12_f_pago_mm int,
     wasdo12_f_pago_dd int,
     wasdo12_nro_seq int,
     wasdo12_importe decimal(17,2),
     wasdo12_empresa int,
     wasdo12_suc_recaud int,
     wasdo12_origen_mov string,
     wasdo12_moneda int,
     wasdo12_nro_comprobante string,
     wasdo12_estado string,
     wasdo12_estado_fecha int,
     wasdo12_cartera int,
     wasdo12_ident_canal string,
     wasdo12_ident_fecha int,
     wasdo12_ident_nro int,
     wasdo12_nro_rel_simul string,
     wasdo12_ind_pago_efectivo string,
     wasdo12_ind_boleto_online string,
     wasdo12_ind_pago_boleto string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/afma/fact/wasdo12/consolidated';
