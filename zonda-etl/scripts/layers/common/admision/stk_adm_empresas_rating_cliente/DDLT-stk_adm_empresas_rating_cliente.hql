CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_rating_cliente (
cod_adm_penumper string,
ds_adm_antig_acc_mot string,
ds_adm_antig_acc_result string,
ds_adm_ubicacion_mot string,
ds_adm_ubicacion_result string,
ds_adm_sist_fin_mot string,
ds_adm_sist_fin_result string,
ds_adm_cump_afip_mot string,
ds_adm_cump_afip_result string,
ds_adm_pn_emp_mot string,
ds_adm_pn_emp_resul string,
ds_adm_fac_bru_mot string,
ds_adm_fac_bru_result string,
ds_adm_beneficios_mot string,
ds_adm_beneficios_result string,
ds_adm_deuda_com_mot string,
ds_adm_deuda_com_result string,
ds_adm_cant_bcos_mot string,
ds_adm_cant_bcos_result string,
ds_adm_deuda_banc_mot string,
ds_adm_deuda_banc_result string,
ds_adm_garantias_mot string,
ds_adm_garantias_result string,
dec_adm_vcuali_pro_dem_mer_pun decimal(4,2),
dec_adm_vcuali_acc_ger_pun decimal(4,2),
dec_adm_vcuali_acc_cred_pun decimal(4,2),
dec_adm_vcuant_ben_rentab_pun decimal(4,2),
dec_adm_vcuant_gen_recursos_pun decimal(3,2),
dec_adm_vcuant_solvencia_pun decimal(3,2),
dec_adm_vcuant_promedio decimal(3,2),
dec_adm_vcuant_valoracion_final decimal(3,2),
dec_adm_vcuant_rnetfin_vta_res decimal(16,2),
int_adm_vcuant_rnetfin_vta_pun int,
dec_adm_vcuant_dbtot_vta_res decimal(16,2),
int_adm_vcuant_dbtot_vta_pun int,
dec_adm_vcuant_dbtot_rent_res decimal(16,2),
int_adm_vcuant_dbtot_rent_pun int,
dec_adm_vcuant_pastot_pnet_res decimal(16,2),
int_adm_vcuant_pastot_pnet_pun int,
dec_adm_vcuant_dbtot_pnet_res decimal(16,2),
int_adm_vcuant_dbtot_pnet_pun int,
ds_adm_obs_pro_dem_mer string,
ds_adm_obs_acc_ger string,
ds_adm_obs_acc_cred string,
ds_adm_obs_ben_rentab string,
ds_adm_obs_gen_recursos string,
ds_adm_obs_solvencia string,
ds_adm_obs_proyecciones string,
ds_adm_obs_exp_ant string,
ds_adm_obs_sintesis string,
cod_adm_peusualt string,
dt_adm_pefecalt string,
cod_adm_peusumod string,
dt_adm_pefecmod string,
ds_adm_obs_pro_dem_mer_2 string,
id_adm int,
dt_adm_fecha_valoracion string,
int_adm_vcuant_ben_rentab_pun_a int,
dec_adm_vcuant_gen_recursos_pun_a decimal(3,2),
dec_adm_vcuant_solvencia_pun_a decimal(3,2),
ds_adm_obs_pro_dem_mer_3 string,
ds_adm_obs_pro_dem_mer_4 string,
ds_adm_obs_acc_ger_2 string,
ds_adm_obs_acc_cred_2 string,
ds_adm_obs_ben_rentab_2 string,
ds_adm_obs_gen_recursos_2 string,
ds_adm_obs_solvencia_2 string,
ds_adm_obs_proyecciones_2 string,
ds_adm_obs_exp_ant_2 string,
ds_adm_obs_sintesis_2 string,
dt_adm_fecha_valoracion_ant string,
dec_adm_valoracion_final_ant decimal(3,2),
dt_adm_fecha_valoracion_garra string,
dec_adm_valoracion_final_garra decimal(3,2),
cod_adm_usuario_val_garra string,
ds_adm_rel_fac_tierra_mot string,
ds_adm_rel_fac_tierra_result string,
ds_adm_vcuali_pro_dem_mer_res string,
ds_adm_vcuali_acc_ger_res string,
ds_adm_vcuali_acc_cred_res string,
dec_adm_vcuant_ben_rentab_res decimal(16,2),
dec_adm_vcuant_gen_recursos_res decimal(16,2),
dec_adm_vcuant_solvencia_res decimal(16,2),
int_adm_nro_ultima_prop int,
ds_adm_mar_valoracion string,
ds_adm_mar_rating string,
dec_adm_vcuant_val_estadistica decimal(3,2),
dec_adm_apoyo_casa_matriz decimal(5,2),
ds_adm_ree_motivo_no_aplic string,
cod_adm_tipo_rating int,
dec_adm_ree_rating_matriz decimal(16,2),
int_adm_ree_prob_apoyo int,
dec_adm_ree_rating_estadistico decimal(16,2),
dec_adm_ree_rating_est_experto decimal(16,2),
dec_adm_ree_rating_apoyo_matriz decimal(16,2),
dec_adm_rc_rating_matriz decimal(16,2),
int_adm_rc_prob_apoyo int,
dec_adm_vcuali_admin_pun decimal(4,2),
dec_adm_vcuali_finan_promo_pun decimal(4,2),
dec_adm_vcuali_prof_gestor_pun decimal(4,2),
ds_adm_obs_admin string,
ds_adm_obs_admin_2 string,
ds_adm_obs_finan_promo string,
ds_adm_obs_finan_promo_2 string,
ds_adm_obs_prof_gestor string,
ds_adm_obs_prof_gestor_2 string,
int_adm_ree_id_balance_1 int,
int_adm_ree_id_balance_2 int,
dec_adm_mra decimal(16,2),
dec_adm_val_ant_rentabilidad decimal(4,2),
dec_adm_val_ant_recursos decimal(4,2),
dec_adm_val_ant_solvencia decimal(4,2),
dec_adm_val_ant_prodmerc decimal(4,2),
dec_adm_val_ant_accionistas decimal(4,2),
dec_adm_val_ant_credito decimal(4,2),
ds_adm_resumen_act string,
ds_adm_bullet_points string,
dec_adm_rating_regulador decimal(4,2),
ds_adm_comentarios string,
ds_adm_riesgos string,
ds_adm_mitigantes string,
int_adm_ree_motivos_desacuerdo int,
ds_adm_ree_alerta_rating string,
ds_adm_m_rating_plus string,
dec_adm_rating_plus  decimal(16,2),
dec_adm_rating_final decimal(16,2),
ds_adm_t_rating_final string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_empresas_rating_cliente';