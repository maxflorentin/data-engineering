CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_log_rio2_clob (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_log_rio2_clob';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_omdm (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_omdm';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_omdm_motivos (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_omdm_motivos';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_omdm_regla_decision_razon (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_omdm_regla_decision_razon';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_omdm_scoring (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_omdm_scoring';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_omdm_variables_entrada (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_omdm_variables_entrada';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_participante (
    cod_sucursal int,
    nro_solicitud int,
    nro_participante int,
    cod_prod_no_enc string,
    mar_caj_segu string,
    mar_titulo string,
    mar_prod_pas string,
    cod_prod_paq string,
    can_plz_fij_pes int,
    tot_plz_fij_pes double,
    can_plz_fij_dol int,
    tot_plz_fij_dol double,
    cod_bca string,
    cod_divs string,
    cod_tlead int,
    cod_ejec_cta int,
    mar_emp_inh string,
    cod_bol_inh int,
    nro_orden int,
    cod_secc_inh int,
    nro_banco int,
    fec_alt_inh string,
    fec_vto_inh string,
    can_inh int,
    cod_caus_inh string,
    des_caus_inh string,
    nom_nombre string,
    nom_apellido string,
    tpo_doc string,
    nro_doc int,
    fec_nacimiento string,
    mar_sexo string,
    cod_nacionalidad int,
    cod_estado_civil string,
    can_pers_a_cargo int,
    cod_nivel_estudio string,
    cod_rol_en_soli string,
    mar_aprb_rech_scor string,
    can_consul int,
    can_antec_menor int,
    can_antec_mayr int,
    can_antec_regu int,
    fec_alta_prod_cb string,
    can_prod_actv_cb int,
    can_prod_actv_bc int,
    cod_exper_previa_sis string,
    cod_informe_veraz string,
    cod_ref_bancaria string,
    mar_verf_tel string,
    mar_verf_dom string,
    mar_verf_actv_ingr string,
    tpo_renta string,
    mon_suel int,
    mon_sac int,
    mon_comision int,
    mon_gratif int,
    mon_rentas int,
    mon_otr_ingr int,
    des_otr_ingr string,
    mon_alquiler int,
    mon_expensas int,
    mar_cotitular string,
    cod_profesion string,
    mar_cliente string,
    cod_suc_char string,
    nro_part_cony int,
    fec_anio_mes_calif string,
    can_consul_2m int,
    cod_ult_consulta string,
    cod_pri_consulta string,
    mar_verf_docu string,
    mar_alerta string,
    can_tarjetas int,
    can_antec_mayr_lev int,
    can_antec_mayr_gra int,
    cod_suc_veraz_reut int,
    nro_solic_veraz_reut int,
    ide_nup string,
    cod_val_docu string,
    tpo_ins string,
    nro_ins bigint,
    mar_fraude string,
    mar_experiencia string,
    nro_score_veraz int,
    cod_grupo_veraz string,
    cod_poblac_veraz string,
    indicador_riesgo string,
    nup_empresa_asociada int,
    mar_max_ir string,
    mon_ing_validos bigint,
    lim_tope_renta bigint,
    mar_pyme string,
    fec_mar_juanito string,
    email string,
    celular string,
    documentacion string,
    hijos int,
    importe_colegio int,
    nro_lote_veraz_reut string,
    mar_citi string,
    mar_rel_amex string,
    actividad_afip string,
    antig_act_afip string,
    tipo_cliente string,
    cod_tipo_renta string,
    cant_adel_eftvo_pesos int,
    mon_adel_eftvo_pesos double,
    cant_adel_eftvo_dolares int,
    mon_adel_eftvo_dolares double
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_participante';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_solicitud (
    cod_sucursal int,
    nro_solicitud int,
    cod_canal string,
    cod_prom_scor_acte string,
    mar_acte_eftv string,
    cod_prom_scor_accd string,
    cod_prom_scor_paq string,
    tpo_paq string,
    cod_dest_fond_aptm string,
    val_bien_aptm int,
    mon_soli_aptm int,
    can_cuo_aptm int,
    imp_cuo_aptm int,
    cod_prom_scor_aptm string,
    cod_dest_fond_aptp string,
    val_bien_aptp int,
    mon_soli_aptp int,
    can_cuo_aptp int,
    imp_cuo_aptp int,
    cod_prom_scor_aptp string,
    cod_dest_fond_afco string,
    val_bien_afco int,
    mon_soli_afco int,
    can_cuo_afco int,
    imp_cuo_afco int,
    cod_prom_scor_afco string,
    cod_prom_scor_avis string,
    cod_prom_scor_amas string,
    nro_score int,
    mon_afec_tar double,
    mar_aprob_rech string,
    mon_afec_acuerdo double,
    mon_afec_ptmo double,
    des_obs_scor string,
    can_participante int,
    mon_afectac_scor double,
    cod_estado string,
    mar_plan_sueldo string,
    nro_referencia int,
    cod_motivo_1 string,
    cod_motivo_2 string,
    cod_motivo_3 string,
    cod_motivo_4 string,
    porc_afec_calculado int,
    fec_ingreso_sco string,
    fec_eleva_superv string,
    fec_respuesta_sco string,
    cod_estado_sco string,
    cod_analista string,
    cod_supervisor string,
    mar_preembozado string,
    cod_cola_asignada string,
    cod_user_asignado string,
    cod_moneda_aptm string,
    cod_moneda_aptp string,
    cod_moneda_afco string,
    tpo_tasa_aptm string,
    tpo_tasa_aptp string,
    tpo_tasa_afco string,
    cod_sis_amrt_aptm string,
    cod_sis_amrt_aptp string,
    cod_sis_amrt_afco string,
    hra_ingreso_sco int,
    cod_promotor string,
    des_obser string,
    cod_prom_scor_amex string,
    cod_mot_zg string,
    cod_nivel_resol string,
    tpo_asignacion string,
    nro_ord_asignacion int,
    mar_aso1_auto string,
    cod_emp_pas string,
    cod_analista_veraz string,
    cod_prom_scor string,
    cod_supervisor_origina string,
    cod_analista_original string,
    cod_sector string,
    mar_bp string,
    fec_alta_paq string,
    mar_limites_optimizados string,
    lim_ppp_vigente int,
    lim_acc_vigente int
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_solicitud';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_solicitud_propuesta (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_solicitud_propuesta';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_tdatos_adicionales (
    cod_sucursal int,
    nro_solicitud int,
    tpo_tarjeta string,
    nro_tarjeta bigint,
    fec_vto string,
    lim_actual int,
    lim_prop int,
    cod_mone int,
    fec_ini_ampl string,
    pzo_dias int,
    cod_gtia int,
    cod_motivo int,
    mar_ca string,
    mar_cc string,
    mar_empl_brio string,
    cod_est_cta int,
    cod_est_tarj int,
    fec_baja string,
    lim_finan int,
    mar_pgo_min string,
    mar_est_inh string,
    tpo_uso string,
    nom_aseguradora string,
    obs_anio int,
    cod_canal string,
    tpo_bien string,
    porc_ltv double,
    obs_cobertura string,
    cod_marca int,
    cod_modelo int,
    anio_bien int,
    val_rel_cuota_ingreso int,
    mon_ingreso_total_grupo int,
    cod_dest_fond_afco string,
    periodicidad string,
    lim_prop_res int,
    pzo_dias_res int,
    fec_ini_ampl_res string,
    obs_prendario string,
    cod_anal_obs_prend string,
    cod_frontend string,
    nro_solicitud_fe int,
    des_marca string,
    des_modelo string,
    mon_acred_hab bigint,
    dias_acred_hab int,
    valor_tasa double,
    fecha_info_comp string,
    pzo_prestamo double,
    mar_reestructura string,
    mar_veraz_renovado string,
    tasa_a_12_meses double,
    tasa_a_24_meses double,
    tasa_a_36_meses double,
    tasa_a_48_meses double,
    tasa_a_60_meses double,
    modi_ingresos string,
    alicuota_iva_prend double,
    seguro_automotor_prend double,
    seguro_vida_prend double,
    max_cod_prod_ofrecer string,
    max_cod_subprod_ofrecer string,
    habilita_mejora string,
    politicas string,
    matricula_mes int,
    matricula_anio int,
    tipo_medicina string,
    importe_medicina int,
    importe_expensas int,
    producto_a_mejorar string,
    tipo_producto_a_mejorar string,
    universidad string,
    carrera string,
    segmento string,
    fec_ingreso_uni string,
    cant_mat_falt int,
    tipo_universidad string,
    lim_cheq_vigente string,
    saldo_act_preacordado string,
    consumido_cheques string,
    consumido_acuerdo string,
    mejora_oferta string,
    tasa_a_12_meses_uva double,
    tasa_a_24_meses_uva double,
    tasa_a_36_meses_uva double,
    tasa_a_48_meses_uva double,
    tasa_a_60_meses_uva double,
    val_bien_estimado bigint,
    tpo_solicitud string,
    linea_cesion_cheque double,
    ult_ing_suc double,
    mar_renta_presunta string,
    lim_no_util_ppp double,
    mon_util_acu_sobreg double,
    mon_cheq_pend_pago double,
    saldo_deuda_ppp double,
    cotizacion double,
    fec_egreso_uni string,
    mar_cupon string,
    plazo_maximo_cupon int,
    plazo_minimo_cupon int,
    monto_maximo_cupon int,
    pct_max_financiacion_cupon int
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_tdatos_adicionales';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_testado (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_testado';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_testado_cvcsri_srs (
    cod_sucursal int,
    nro_solicitud int,
    cod_sector string,
    cod_estado string,
    mot_resol1 string,
    mot_resol2 string,
    mot_resol3 string,
    mot_resol4 string,
    nro_sec int,
    fec_estado string,
    mot_resol5 string,
    analista string,
    fec_ingreso string,
    fec_inicio_resol string,
    fec_fin_resol string,
    fec_res_prendario string
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_testado_cvcsri_srs';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_tmotivo_sw_srs (
    cod_grupo_decision string,
    cod_motivo string,
    nro_prioridad int,
    cod_motivo_asol string,
    nom_aplicativo string,
    des_motivo string
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_tmotivo_sw_srs';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_tramites_ptovta_tarjeta (
    cod_sucursal int,
    nro_solicitud bigint,
    nro_tramite_fe bigint,
    marca_telefono string,
    cod_zona string,
    marca_datos_adic string,
    marca_tarjeta_visa string,
    tipo_tarjeta_visa string,
    lim_tarjeta_visa int,
    marca_tarjeta_amex string,
    tipo_tarjeta_amex string,
    lim_tarjeta_amex int,
    ver_documento int,
    mar_pide_datos_ingresos string,
    val_ingresos string,
    rango_rnet string,
    ingresos_verificados int,
    fec_rnet timestamp(6),
    tpo_preeval varchar2(50),
    cod_tramite_reu string,
    marca_tarjeta_master string,
    tipo_tarjeta_master string,
    lim_tarjeta_master int
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_tramites_ptovta_tarjeta';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_v_f485_sge (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_v_f485_sge';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_v_lineas_otorgadas_sge (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_v_lineas_otorgadas_sge';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_v_personas_sge (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_v_personas_sge';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_v_propuesta_sge (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_v_propuesta_sge';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_v_stnd_datos_adicionales_prop (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_v_stnd_datos_adicionales_prop';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_testado (
    cod_sucursal int,
    nro_solicitud int,
    cod_canal string,
    fec_ingreso_rio2 string,
    fec_envio_sw1 string,
    fec_envio_sw2 string,
    cod_estado_sw string,
    fec_desicion_sw string,
    mar_pedido_veraz string,
    mar_reutiliza_veraz string,
    fec_envio_veraz string,
    fec_recep_veraz string,
    fec_ingreso_srs string,
    fec_asig_ana_srs string,
    fec_ini_resol_srs string,
    fec_fin_resol_srs string,
    fec_retro_srs string,
    fec_reasig_ana_srs string,
    cod_estado_srs string,
    fec_resol_suc string,
    cod_resol_suc string,
    fec_resol_altas string,
    cod_resol_altas string,
    fec_retro string,
    cod_estado_retro char(3),
    cod_estado_actual char(3),
    fec_estado_actual string,
    cod_estado_asol string,
    fec_estado_asol string,
    nom_sector_altas string
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_testado';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_tlegajos (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_tlegajos';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_tsol_estado (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_tsol_estado';


CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_accionariado_prop (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_accionariado_prop';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_balances_per (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_balances_per';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_blc_pas (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_blc_pas';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_blc_act (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_blc_act';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_blc_dadic (
    id_blc int,
    penumper int,
    fec_blc string,
    consolidado string,
    id_proyeccion int,
    duracion int,
    orden int,
    peusualt string,
    peusumod string,
    pefecalt string,
    pefecmod string,
    fec_ini_blc string,
    tpo_blc string,
    imp_deprec_cprod double,
    imp_deprec_gsadm double,
    imp_deprec_gscom double,
    imp_deprec_otrgsop double,
    imp_deprec_otregresos double,
    imp_deprec_perdext double,
    imp_desresrtbsuso_cprod double,
    imp_desresrtbsuso_gsadm double,
    imp_desresrtbsuso_gscom double,
    imp_desresrtbsuso_otrgsop double,
    imp_desresrtbsuso_otringresos double,
    imp_desresrtbsuso_ganext double,
    imp_amortint_cprod double,
    imp_amortint_gsadm double,
    imp_amortint_gscom double,
    imp_amortint_otrgsop double,
    imp_amortint_otregresos double,
    imp_amortint_perdext double,
    imp_amortinv_otregresos double,
    imp_prev_cprod double,
    imp_prev_gsadm double,
    imp_prev_gscom double,
    imp_prev_otrgsop double,
    imp_prev_otregresos double,
    imp_prev_perdext double,
    imp_hondir double,
    imp_gratifpersonal double,
    imp_gtosprod double,
    imp_dismamort double,
    imp_divcob_socvinculadas double,
    imp_aportes_capital double,
    imp_constrrt double,
    imp_desafrrt double,
    imp_ajustes_resantnaf double,
    imp_ajuste_ejerant double,
    imp_act_monex double,
    imp_pas_monex double,
    nro_ultima_prop int,
    imp_ajejerant_ajinfla double,
    imp_altas_bienes_uso double,
    imp_bajas_bienes_uso double,
    imp_altas_inv_perm double,
    imp_bajas_inv_perm double,
    imp_altas_int_val_llave double,
    imp_altas_int_gtos_reorg double,
    imp_bajas_int double,
    imp_var_prev_pasivo double
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_blc_dadic';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_blc_eres (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_blc_eres';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_cat_grupos (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_cat_grupos';


CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_deudas_bancarias_prop (
    fecha_deuda_id int,
    cod_banco int,
    acc_cto_plz double,
    acc_lgo_plz double,
    aecc_cto_plz double,
    aecc_lgo_plz double,
    dto_doc_cto_plz double,
    dto_doc_lgo_plz double,
    leasing_cto_plz double,
    leasing_lgo_plz double,
    gtia_real_cto_plz double,
    gtia_real_lgo_plz double,
    otr_gtias_cto_plz double,
    otr_gtias_lgo_plz double,
    cart_cred_cto_plz double,
    cart_cred_lgo_plz double,
    avales_cto_plz double,
    avales_lgo_plz double,
    acc_cto_plz_dol double,
    acc_lgo_plz_dol double,
    aecc_cto_plz_dol double,
    aecc_lgo_plz_dol double,
    dto_doc_cto_plz_dol double,
    dto_doc_lgo_plz_dol double,
    leasing_cto_plz_dol double,
    leasing_lgo_plz_dol double,
    gtia_real_cto_plz_dol double,
    gtia_real_lgo_plz_dol double,
    otr_gtias_cto_plz_dol double,
    otr_gtias_lgo_plz_dol double,
    avales_cto_plz_dol double,
    avales_lgo_plz_dol double,
    cod_col_variable_1 string,
    cod_col_variable_2 string,
    variable_1_cto_plz double,
    variable_1_lgo_plz double,
    variable_1_cto_plz_dol double,
    variable_1_lgo_plz_dol double,
    variable_2_cto_plz double,
    variable_2_lgo_plz double,
    variable_2_cto_plz_dol double,
    variable_2_lgo_plz_dol double,
    nro_prop int
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_deudas_bancarias_prop';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_estados_f487 (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_estados_f487';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_estados_propuesta (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_estados_propuesta';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_f485 (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_f485';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_f487 (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_f487';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_fechas_deudas_bancarias_prop (
    fecha_deuda_id int,
    fec_deuda string,
    penumper int,
    nro_prop int,
    cant_ban_bcra int,
    deu_total_bcra int,
    mar_consulta_bcra string,
    obs_can_deu_bcra string
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_fechas_deudas_bancarias_prop';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_garantias_genericas (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_garantias_genericas';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_jerarquia_cargos (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_jerarquia_cargos';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_lineas_otorgadas (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_lineas_otorgadas';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_personas (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_personas';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_prop_global (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_prop_global';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_propuesta (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_propuesta';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_ree_area_preguntas (
    codigo int,
    cod_rating_area int
    descripcion_pregunta string
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_ree_area_preguntas';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_ree_areas (
    codigo int,
    descripcion string
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_ree_areas';

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio2_ree_motivos_desacuerdo (
    CODIGO string,
    DESCRIPCION string
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_ree_motivos_desacuerdo';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_ree_pregunta_respuestas (
    codigo int,
    cod_area_pregunta int,
    descripcion_respuesta string,
    codigo_ree int
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_ree_pregunta_respuestas';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_ree_tipos_rating (
    codigo int,
    descripcion string
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_ree_tipos_rating';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_rpc (
    id_rpc int,
    nro_prop int,
    imp_importe_calif double,
    imp_resp_pat_comp double,
    justificacion string,
    fec_calificacion string,
    cod_usu_alta string,
    fec_estados_contables string,
    duracion int,
    rel_porc string,
    penumper int,
    mar_imprimir_acta string
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_rpc';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_stnd_balances (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_stnd_balances';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_stnd_cod_estado (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_stnd_cod_estado';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_stnd_datos_adicionales_f487 (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_stnd_datos_adicionales_f487';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_stnd_datos_adicionales_prop (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_stnd_datos_adicionales_prop';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_stnd_log_f487 (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_stnd_log_f487';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_stnd_log_propuesta (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_stnd_log_propuesta';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_tpo_operacion (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_tpo_operacion';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_usuarios (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_usuarios';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_valoracion (
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_valoracion';

CREATE TABLE IF NOT EXISTS bi_corp_staging.rio2_valoracion_per (
    penumper int,
    antig_acc_mot string,
    antig_acc_result string,
    ubicacion_mot string,
    ubicacion_result string,
    sist_fin_mot  string,
    sist_fin_result string,
    cump_afip_mot string,
    cump_afip_result string,
    pn_emp_mot string,
    pn_emp_resul string,
    fac_bru_mot string,
    fac_bru_result string,
    beneficios_mot string,
    beneficios_result string,
    deuda_com_mot string,
    deuda_com_result string,
    cant_bcos_mot string,
    cant_bcos_result string,
    deuda_banc_mot string,
    deuda_banc_result string,
    garantias_mot string,
    garantias_result string,
    vcuali_pro_dem_mer_pun double,
    vcuali_acc_ger_pun double,
    vcuali_acc_cred_pun double,
    vcuant_ben_rentab_pun double,
    vcuant_gen_recursos_pun double,
    vcuant_solvencia_pun double,
    vcuant_promedio double,
    vcuant_valoracion_final double,
    vcuant_rnetfin_vta_res double,
    vcuant_rnetfin_vta_pun int,
    vcuant_dbtot_vta_res double,
    vcuant_dbtot_vta_pun int,
    vcuant_dbtot_rent_res double,
    vcuant_dbtot_rent_pun int,
    vcuant_pastot_pnet_res double,
    vcuant_pastot_pnet_pun int,
    vcuant_dbtot_pnet_res double,
    vcuant_dbtot_pnet_pun int,
    obs_pro_dem_mer string,
    obs_acc_ger string,
    obs_acc_cred string,
    obs_ben_rentab string,
    obs_gen_recursos string,
    obs_solvencia string,
    obs_proyecciones string,
    obs_exp_ant string,
    obs_sintesis string,
    peusualt string,
    pefecalt string,
    peusumod string,
    pefecmod string,
    obs_pro_dem_mer_2 string,
    fecha_valoracion string,
    vcuant_ben_rentab_pun_a int,
    vcuant_gen_recursos_pun_a double,
    vcuant_solvencia_pun_a double,
    obs_pro_dem_mer_3 string,
    obs_pro_dem_mer_4 string,
    obs_acc_ger_2 string,
    obs_acc_cred_2 string,
    obs_ben_rentab_2 string,
    obs_gen_recursos_2 string,
    obs_solvencia_2 string,
    obs_proyecciones_2 string,
    obs_exp_ant_2 string,
    obs_sintesis_2 string,
    fecha_valoracion_ant string,
    valoracion_final_ant double,
    fecha_valoracion_garra string,
    valoracion_final_garra double,
    usuario_val_garra string,
    rel_fac_tierra_mot string,
    rel_fac_tierra_result string,
    vcuali_pro_dem_mer_res string,
    vcuali_acc_ger_res string,
    vcuali_acc_cred_res string,
    vcuant_ben_rentab_res double,
    vcuant_gen_recursos_res double,
    vcuant_solvencia_res double,
    nro_ultima_prop int,
    mar_valoracion string,
    mar_rating string,
    vcuant_val_estadisticadouble,
    apoyo_casa_matriz int(5,2),
    ree_motivo_no_aplic string,
    cod_tipo_rating int,
    ree_rating_matriz double,
    ree_prob_apoyo int,
    ree_rating_estadistico double,
    ree_rating_est_experto double,
    ree_rating_apoyo_matriz double,
    rc_rating_matriz double,
    rc_prob_apoyo int,
    vcuali_admin_pun double,
    vcuali_finan_promo_pun double,
    vcuali_prof_gestor_pun double,
    obs_admin string,
    obs_admin_2 string,
    obs_finan_promo string,
    obs_finan_promo_2 string,
    obs_prof_gestor string,
    obs_prof_gestor_2 string,
    ree_id_balance_1 int,
    ree_id_balance_2 int,
    mra double,
    val_ant_rentabilidad double,
    val_ant_recursos double,
    val_ant_solvencia double,
    val_ant_prodmerc double,
    val_ant_accionistas double,
    val_ant_credito double,
    resumen_act string,
    bullet_points string,
    rating_regulador string,
    comentarios string,
    riesgos string,
    mitigantes string,
    ree_motivos_desacuerdo int,
    ree_alerta_rating string,
    m_rating_plus string,
    rating_plus double,
    rating_final double,
    t_rating_final char
)
PARTITIONED BY (partition_string string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/rio2_valoracion_per';







