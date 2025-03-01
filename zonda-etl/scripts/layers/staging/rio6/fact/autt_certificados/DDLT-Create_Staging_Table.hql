CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_autt_certificados(
    auce_cace_casu_cd_sucursal      string,
    auce_cace_carp_cd_ramo          string,
    auce_cace_capo_nu_poliza        string,
    auce_cace_nu_certificado        string,
    auce_fe_efectiva                string,
    auce_auve_nu_serial_carroceria  string,
    auce_in_ramo_rc                 string,
    auce_in_ramo_oc                 string,
    auce_in_ramo_gv                 string,
    auce_in_ramo_ac                 string,
    auce_cd_persona_uso             string,
    auce_auus_cd_uso                string,
    auce_augr_cd_grupo              string,
    auce_autg_cd_tipo_carga         string,
    auce_cd_grado_licencia          string,
    auce_caci_cd_ciudad             string,
    auce_caci_caes_cd_estado        string,
    auce_an_experiencia             string,
    auce_nu_tiempo_uso              string,
    auce_in_uso_urbano              string,
    auce_ca_pasajeros               string,
    auce_ca_conductor               string,
    auce_ca_ayudantes               string,
    auce_tp_riesgo_pasajeros        string,
    auce_tp_riesgo_conductor        string,
    auce_tp_riesgo_ayudantes        string,
    auce_nu_plan                    string,
    auce_de_unidad                  string,
    auce_nu_inspeccion              string,
    auce_casu_cd_sucursal_inspec    string,
    auce_capj_cd_sucursal_ptmo      string,
    auce_nu_prestamo                string,
    auce_fe_desde_ptmo              string,
    auce_fe_hasta_ptmo              string,
    auce_nu_kilometros              string,
    auce_ca_cuotas                  string,
    auce_fe_vcto_cuota_1            string,
    auce_cuota_1_cobrada            string,
    auce_balc_cd_alternativa_cob    string,
    auce_ca_siniestros              string,
    auce_ca_meses_sin_siniestros    string,
    auce_adtb_po_bonif              string,
    auce_nu_pol_ant_bonif           string,
    auce_atbo_tp_bonif              string,
    auce_ateb_tp_escala             string,
    auce_cazp_cd_postal             string,
    auce_direccion                  string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/fact/autt_certificados';