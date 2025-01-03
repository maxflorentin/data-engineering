CREATE TABLE IF NOT EXISTS bi_corp_common.stk_per_personas(
cod_per_nup	STRING,
per_nombre STRING,
per_apellido STRING,
per_tipo_doc STRING,
per_num_doc STRING,
cod_sucursal_administradora	STRING,
flag_empleado	STRING,
cod_clanae	STRING,
cod_per_segmento_duro	STRING,
cod_per_banca	STRING,
flag_banca_privada	INT,
flag_cuenta_interna	INT,
flag_woman	INT,
flag_iu	INT,
flag_nova	INT,
flag_citi	INT,
flag_universitario	INT,
cod_universitario STRING,
ds_universitario STRING,
flag_international_desk	INT,
flag_uge	INT,
flag_vip	INT,
flag_plan_sueldo	INT,
flag_titular	INT,
flag_cotitular	INT,
flag_adicional	INT,
flag_firmante	INT,
flag_supervivencia	INT,
flag_jubilado	INT,
flag_cliente_anses	INT,
flag_colectivo	INT,
flag_digital_30	INT,
cod_per_grupo_economico	STRING,
ds_per_grupo_economico STRING,
cod_top_level_grupo_economico	STRING,
ds_top_level_grupo_economico	STRING,
ds_tipo_empresa	STRING,
cod_per_ramo_empresa	STRING,
cod_per_vinculacion	STRING,
ds_per_vinculacion	STRING,
ds_mapa_seguimiento	STRING,
cod_kgr	STRING,
dt_ultima_acreditacion	STRING,
ds_per_segmento	STRING,
ds_per_subsegmento	STRING,
cod_per_cuadrante	STRING,
dt_fecha_antiguedad	STRING,
ds_test_inversor	STRING,
dt_test_inversor_ult_fecha	STRING,
flag_superclub	INT,
flag_aa	INT,
flag_sorpresa	INT,
flag_clave_automatico	INT,
flag_cuenta_blanca	INT,
cod_ofical_asignado	STRING,
ds_ofical_asignado	STRING,
ds_modelo_atencion	STRING,
flag_agro	INT,
cod_division	STRING,
fc_maximo_dias_mora	DECIMAL(15,2),
fc_productos_mora	DECIMAL(15,2),
fc_score_comportamiento	DECIMAL(15,2),
fc_score_veraz	DECIMAL(15,2),
fc_raiting	DECIMAL(15,2),
flag_raiting_plus	INT,
ds_principalidad_veraz_behaviour	STRING,
ds_motivo_baja	STRING,
cod_per_sexo	STRING,
cod_per_tipo_persona	STRING,
dt_per_fecha_nacimiento	DATE,
cod_per_estado_civil	STRING,
cod_per_pais_nacimiento	STRING,
cod_per_nacionalidad	STRING,
cod_per_residencia	STRING,
cod_actividad_afip	STRING,
ds_tipo_sociedad	STRING,
cod_per_condicion_cliente	STRING,
cod_sit_bcra_bsr	STRING,
cod_sit_bcra_sist_financiero	STRING,
fc_deuda_sist_financiero	DECIMAL(15,2),
flag_mipyme	INT,
cod_mipyme STRING,
ds_mipyme STRING,
fc_per_edad	INT,
fc_cuentas	DECIMAL(15,2),
fc_tarjetas	DECIMAL(15,2),
fc_prestamos	DECIMAL(15,2),
fc_seguros	DECIMAL(15,2),
fc_plazo_fijo	DECIMAL(15,2),
fc_fci	DECIMAL(15,2),
fc_letras	DECIMAL(15,2),
fc_acciones	DECIMAL(15,2),
fc_bonos	DECIMAL(15,2),
fc_caja_seguridad	DECIMAL(15,2),
fc_limite_pre_acordado	DECIMAL(15,2),
cod_ocupacion	STRING,
ds_ocupacion	STRING,
flag_fallecido	INT,
cod_canal_venta	STRING,
cod_actividad_bcra	STRING,
ds_actividad_bcra	STRING,
cod_campana	STRING,
cod_sujeto	STRING,
dt_fecha_inicio_actividades	DATE,
cod_forma_juridica	STRING,
fc_score_cliente1	DECIMAL(15,2),
fc_score_cliente2	DECIMAL(15,2),
fc_score_alineado_cliente1	DECIMAL(15,2),
fc_score_alineado_cliente2	DECIMAL(15,2),
fc_score_bruto_cliente1	DECIMAL(15,2),
fc_score_bruto_cliente2	DECIMAL(15,2),
cod_renta	STRING,
cod_segmento_triad	STRING,
cod_suc_paquete	STRING,
cod_zona	STRING,
fc_limite_disponible_compra_tc	DECIMAL(15,2),
fc_monto_total_impago_cliente	DECIMAL(15,2),
fc_ingreso_bruto_calificacion	DECIMAL(15,2),
dt_fecha_ultima_calificacion	STRING,
fc_mon_ingreso	DECIMAL(15,2)
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/personas/fact/stk_per_personas'