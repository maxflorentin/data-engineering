set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_rating_cliente
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')

SELECT
    v.penumper as cod_adm_penumper,
    v.antig_acc_mot as ds_adm_antig_acc_mot,
    v.antig_acc_result as ds_adm_antig_acc_result,
    v.ubicacion_mot as ds_adm_ubicacion_mot,
    v.ubicacion_result as ds_adm_ubicacion_result,
    v.sist_fin_mot as ds_adm_sist_fin_mot,
    v.sist_fin_result as ds_adm_sist_fin_result,
    v.cump_afip_mot as ds_adm_cump_afip_mot,
    v.cump_afip_result as ds_adm_cump_afip_result,
    v.pn_emp_mot as ds_adm_pn_emp_mot,
    v.pn_emp_resul as ds_adm_pn_emp_resul,
    v.fac_bru_mot as ds_adm_fac_bru_mot,
    v.fac_bru_result as ds_adm_fac_bru_result,
    v.beneficios_mot as ds_adm_beneficios_mot,
    v.beneficios_result as ds_adm_beneficios_result,
    v.deuda_com_mot as ds_adm_deuda_com_mot,
    v.deuda_com_result as ds_adm_deuda_com_result,
    v.cant_bcos_mot as ds_adm_cant_bcos_mot,
    v.cant_bcos_result as ds_adm_cant_bcos_result,
    v.deuda_banc_mot as ds_adm_deuda_banc_mot,
    v.deuda_banc_result as ds_adm_deuda_banc_result,
    v.garantias_mot as ds_adm_garantias_mot,
    v.garantias_result as ds_adm_garantias_result,
    cast(v.vcuali_pro_dem_mer_pun as decimal(4,2)) as dec_adm_vcuali_pro_dem_mer_pun,
    cast(v.vcuali_acc_ger_pun as decimal(4,2)) as dec_adm_vcuali_acc_ger_pun,
    cast(v.vcuali_acc_cred_pun as decimal(4,2)) as dec_adm_vcuali_acc_cred_pun,
    cast(v.vcuant_ben_rentab_pun as decimal(4,2)) as dec_adm_vcuant_ben_rentab_pun,
    cast(v.vcuant_gen_recursos_pun as decimal(3,2)) as dec_adm_vcuant_gen_recursos_pun,
    cast(v.vcuant_solvencia_pun as decimal(3,2)) as dec_adm_vcuant_solvencia_pun,
    cast(v.vcuant_promedio as decimal(3,2)) as dec_adm_vcuant_promedio,
    cast(v.vcuant_valoracion_final as decimal(3,2)) as dec_adm_vcuant_valoracion_final,
    cast(v.vcuant_rnetfin_vta_res as decimal(16,2)) as dec_adm_vcuant_rnetfin_vta_res,
    cast(v.vcuant_rnetfin_vta_pun as int) as int_adm_vcuant_rnetfin_vta_pun,
    cast(v.vcuant_dbtot_vta_res as decimal(16,2)) as dec_adm_vcuant_dbtot_vta_res,
    cast(v.vcuant_dbtot_vta_pun as int) as int_adm_vcuant_dbtot_vta_pun,
    cast(v.vcuant_dbtot_rent_pun as int) as int_adm_vcuant_dbtot_rent_pun,
    cast(v.vcuant_pastot_pnet_res as decimal(16,2)) as dec_adm_vcuant_pastot_pnet_res,
    cast(v.vcuant_pastot_pnet_pun as int) as int_adm_vcuant_pastot_pnet_pun,
    cast(v.vcuant_dbtot_pnet_res as decimal(16,2)) as dec_adm_vcuant_dbtot_pnet_res,
    cast(v.vcuant_dbtot_pnet_pun as int) as int_adm_vcuant_dbtot_pnet_pun,
    v.obs_pro_dem_mer as ds_adm_obs_pro_dem_mer,
    v.obs_acc_ger as ds_adm_obs_acc_ger,
    v.obs_acc_cred as ds_adm_obs_acc_cred,
    cast(v.vcuant_dbtot_rent_res as decimal(16,2)) as dec_adm_vcuant_dbtot_rent_res,
    v.obs_ben_rentab as ds_adm_obs_ben_rentab,
    v.obs_gen_recursos as ds_adm_obs_gen_recursos,
    v.obs_solvencia as ds_adm_obs_solvencia,
    v.obs_proyecciones as ds_adm_obs_proyecciones,
    v.obs_exp_ant as ds_adm_obs_exp_ant,
    v.obs_sintesis as ds_adm_obs_sintesis,
    v.peusualt as cod_adm_peusualt,
    from_unixtime(unix_timestamp(TRIM(v.pefecalt), 'yyyyMMdd'),"yyyy-MM-dd") as dt_adm_pefecalt,
    v.peusumod as cod_adm_peusumod,
    from_unixtime(unix_timestamp(TRIM(v.pefecmod), 'yyyyMMdd'),"yyyy-MM-dd") as dt_adm_pefecmod,
    v.obs_pro_dem_mer_2 as ds_adm_obs_pro_dem_mer_2,
    cast(v.id as int) as id_adm,
    from_unixtime(unix_timestamp(TRIM(v.fecha_valoracion), 'yyyyMMdd'),"yyyy-MM-dd") as dt_adm_fecha_valoracion,
    cast(v.vcuant_ben_rentab_pun_a as int) as int_adm_vcuant_ben_rentab_pun_a,
    cast(v.vcuant_gen_recursos_pun_a as decimal(3,2)) as dec_adm_vcuant_gen_recursos_pun_a,
    cast(v.vcuant_solvencia_pun_a as decimal(3,2)) as dec_adm_vcuant_solvencia_pun_a,
    v.obs_pro_dem_mer_3 as ds_adm_obs_pro_dem_mer_3,
    v.obs_pro_dem_mer_4 as ds_adm_obs_pro_dem_mer_4,
    v.obs_acc_ger_2 as ds_adm_obs_acc_ger_2,
    v.obs_acc_cred_2 as ds_adm_obs_acc_cred_2,
    v.obs_ben_rentab_2 as ds_adm_obs_ben_rentab_2,
    v.obs_gen_recursos_2 as ds_adm_obs_gen_recursos_2,
    v.obs_solvencia_2 as ds_adm_obs_solvencia_2,
    v.obs_proyecciones_2 as ds_adm_obs_proyecciones_2,
    v.obs_exp_ant_2 as ds_adm_obs_exp_ant_2,
    v.obs_sintesis_2 as ds_adm_obs_sintesis_2,
    from_unixtime(unix_timestamp(TRIM(v.fecha_valoracion_ant), 'yyyyMMdd'),"yyyy-MM-dd") as dt_adm_fecha_valoracion_ant,
    cast(v.valoracion_final_ant as decimal(3,2)) as dec_adm_valoracion_final_ant,
    from_unixtime(unix_timestamp(TRIM(v.fecha_valoracion_garra), 'yyyyMMdd'),"yyyy-MM-dd") as dt_adm_fecha_valoracion_garra,
     cast(v.valoracion_final_garra as decimal(3,2)) as dec_adm_valoracion_final_garra,
    v.usuario_val_garra as cod_adm_usuario_val_garra,
    v.rel_fac_tierra_mot as ds_adm_rel_fac_tierra_mot,
    v.rel_fac_tierra_result as ds_adm_rel_fac_tierra_result,
    v.vcuali_pro_dem_mer_res as ds_adm_vcuali_pro_dem_mer_res,
    v.vcuali_acc_ger_res as ds_adm_vcuali_acc_ger_res,
    v.vcuali_acc_cred_res as ds_adm_vcuali_acc_cred_res,
    cast(v.vcuant_ben_rentab_res as decimal(16,2)) as dec_adm_vcuant_ben_rentab_res,
    cast(v.vcuant_gen_recursos_res as decimal(16,2)) as dec_adm_vcuant_gen_recursos_res,
    cast(v.vcuant_solvencia_res as decimal(16,2)) as dec_adm_vcuant_solvencia_res,
    cast(v.nro_ultima_prop as int) as int_adm_nro_ultima_prop,
    v.mar_valoracion as ds_adm_mar_valoracion,
    v.mar_rating as ds_adm_mar_rating,
    cast(v.vcuant_val_estadistica as decimal(3,2)) as dec_adm_vcuant_val_estadistica,
    cast(v.apoyo_casa_matriz as decimal(5,2)) as dec_adm_apoyo_casa_matriz,
    v.ree_motivo_no_aplic as ds_adm_ree_motivo_no_aplic,
    cast(v.cod_tipo_rating as int) as cod_adm_tipo_rating,
    cast(v.ree_rating_matriz as decimal(16,2)) as dec_adm_ree_rating_matriz,
    cast(v.ree_prob_apoyo as int) as int_adm_ree_prob_apoyo,
    cast(v.ree_rating_estadistico as decimal(16,2)) as dec_adm_ree_rating_estadistico,
    cast(v.ree_rating_est_experto as decimal(16,2)) as dec_adm_ree_rating_est_experto,
    cast(v.ree_rating_apoyo_matriz as decimal(16,2)) as dec_adm_ree_rating_apoyo_matriz,
    cast(v.rc_rating_matriz as decimal(16,2)) as dec_adm_rc_rating_matriz,
    cast(v.rc_prob_apoyo as int) as int_adm_rc_prob_apoyo,
    cast(v.vcuali_admin_pun as decimal(4,2)) as dec_adm_vcuali_admin_pun,
    cast(v.vcuali_finan_promo_pun as decimal(4,2)) as dec_adm_vcuali_finan_promo_pun,
    cast(v.vcuali_prof_gestor_pun as decimal(4,2)) as dec_adm_vcuali_prof_gestor_pun,
    v.obs_admin as ds_adm_obs_admin,
    v.obs_admin_2 as ds_adm_obs_admin_2,
    v.obs_finan_promo as ds_adm_obs_finan_promo,
    v.obs_finan_promo_2 as ds_adm_obs_finan_promo_2,
    v.obs_prof_gestor as ds_adm_obs_prof_gestor,
    v.obs_prof_gestor_2 as ds_adm_obs_prof_gestor_2,
    cast(v.ree_id_balance_1 as int) as int_adm_ree_id_balance_1,
    cast(v.ree_id_balance_2 as int) as int_adm_ree_id_balance_2,
    cast(v.mra as decimal(16,2)) as dec_adm_mra,
    cast(v.val_ant_rentabilidad as decimal(4,2)) as dec_adm_val_ant_rentabilidad,
    cast(v.val_ant_recursos as decimal(4,2)) as dec_adm_val_ant_recursos,
    cast(v.val_ant_solvencia as decimal(4,2)) as dec_adm_val_ant_solvencia,
    cast(v.val_ant_prodmerc as decimal(4,2)) as dec_adm_val_ant_prodmerc,
    cast(v.val_ant_accionistas as decimal(4,2)) as dec_adm_val_ant_accionistas,
    cast(v.val_ant_credito as decimal(4,2)) as dec_adm_val_ant_credito,
    v.resumen_act as ds_adm_resumen_act,
    v.bullet_points as ds_adm_bullet_points,
    cast(v.rating_regulador as decimal(4,2)) as dec_adm_rating_regulador,
    v.comentarios as ds_adm_comentarios,
    v.riesgos as ds_adm_riesgos,
    v.mitigantes as ds_adm_mitigantes,
    cast(v.ree_motivos_desacuerdo as int) as int_adm_ree_motivos_desacuerdo,
    v.ree_alerta_rating as ds_adm_ree_alerta_rating,
    v.m_rating_plus as ds_adm_m_rating_plus,
    cast(v.rating_plus as decimal(16,2)) as dec_adm_rating_plus,
    cast(v.rating_final as decimal(16,2)) as dec_adm_rating_final,
    v.t_rating_final as ds_adm_t_rating_final
FROM bi_corp_staging.sge_valoracion_per v
WHERE partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';