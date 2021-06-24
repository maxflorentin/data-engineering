CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malbgc_bgdtacu(
acu_entidad				STRING,
acu_centro_alta			STRING,
acu_cuenta				STRING,
acu_divisa				STRING,
acu_secuencia			INT,
acu_tipo_acuerdo		STRING,
acu_estado				STRING,
acu_limite				DECIMAL(15,2),
acu_limite_renov		DECIMAL(15,2),
acu_period_liq			STRING,
acu_tipo_liq			STRING,
acu_clase_liq			STRING,
acu_dia_liq				STRING,
acu_prioridad			INT,
acu_tarifa				STRING,
acu_clase_taf			STRING,
acu_tipo_interes		DECIMAL(8,5),
acu_coefici				STRING,
acu_puntos				DECIMAL(8,5),
acu_tipoint_inc_sbrg	DECIMAL(8,5),
acu_tasa_max_conv		STRING,
acu_importe_cambio		DECIMAL(15,2),
acu_limite_ori			DECIMAL(15,2),
acu_inte_devdeud		DECIMAL(15,2),
acu_refer_liq			INT,
acu_ind_renauto			STRING,
acu_ind_intv_sal_dia	STRING,
acu_ind_pet_liq			STRING,
acu_tipo_vto			STRING,
acu_num_vto				INT,
acu_dias_uso			INT,
acu_dias_desp_pro		INT,
acu_fecha_ultren		STRING,
acu_fecha_inicio		STRING,
acu_fecha_vcto			STRING,
acu_fecha_ultliq		STRING,
acu_fecha_upro_liq		STRING,
acu_fecha_proliq		STRING,
acu_fecha_proc_liq 		STRING,
acu_entidad_umo 		STRING,
acu_centro_umo 			STRING,
acu_userid_umo 			STRING,
acu_netname_umo 		STRING,
acu_timest_umo 			STRING
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malbgc/fact/bgdtacu/consolidated';