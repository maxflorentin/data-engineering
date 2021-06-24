set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_gnl_datos_complem
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
SELECT
    nro_tramite_zx ds_adm_nrotramite_zx,
    nup cod_per_nup,
    cliente_nuevo ds_adm_cliente_nuevo,
    banca ds_adm_banca,
    segm_origen ds_adm_segm_origen,
    tipo_per ds_adm_tipo_per,
    tipo_emp ds_adm_tipo_emp,
    tipo_linea ds_adm_tipo_linea,
    cast(nro_prop as int) int_adm_nro_prop,
    fec_vigencia dt_adm_fecvigencia,
    mca_vigencia ds_adm_mca_vigencia,
    obs_vigencia ds_adm_obs_vigencia,
    mca_calificacion ds_adm_mca_calificacion,
    cast(mon_lin_otorgada as decimal(16,2)) fc_adm_mon_lin_otorgada,
    cast(mon_lin_otorgada_no_cesion as decimal(16,2))fc_adm_mon_lin_otorgada_no_cesion,
    real_calificacion ds_adm_real_calificacion,
    obs_calificacion ds_adm_obs_calificacion,
    mca_disponible ds_adm_mca_disponible,
    real_disponible ds_adm_real_disponible,
    cast(mon_disponible as decimal(16,2)) fc_adm_mon_disponible,
    obs_disponible ds_adm_obs_disponible,
    mca_garantia ds_adm_mca_garantia,
    real_garantia ds_adm_real_garantia,
    obs_garantia ds_adm_obs_garantia,
    mca_cli_vin ds_adm_mca_cli_vin,
    real_cli_vin ds_adm_real_cli_vin,
    obs_cli_vin ds_adm_obs_cli_vin,
    mca_afip ds_adm_mca_afip,
    real_afip ds_adm_real_afip,
    obs_afip ds_adm_obs_afip,
    mca_bcra ds_adm_mca_bcra,
    real_bcra ds_adm_real_bcra,
    obs_bcra ds_adm_obs_bcra,
    form_487 ds_adm_secu_f487 ,
    mca_opera ds_adm_mca_opera,
    obs_opera ds_adm_obs_opera,
    mca_especial_granel ds_adm_mca_especial_granel,
    obs_especial_granel ds_adm_obs_especial_granel,
    mca_especial_f487 ds_adm_mca_especial_f487,
    obs_especial_f487 ds_adm_obs_especial_f487,
    motiv_nopera ds_adm_motiv_nopera,
    cod_pevalind cod_adm_pevalind,
    cod_peindica cod_adm_peindica,
    cast(tasa_min as decimal(16,2)) dec_adm_tasa_min,
    cast(cta_cte as bigint) int_adm_cta_cte,
    prod_cta_cte ds_adm_prod_cta_cte,
    subp_cta_cte ds_adm_subp_cta_cte,
    fec_alta dt_adm_fecalta,
    usu_alta ds_adm_usu_alta,
    fec_modif dt_adm_fecmodif,
    usu_modif ds_adm_usu_modif,
    rechazo_antecedentes_int ds_adm_rechazo_antecedentes_int

FROM bi_corp_staging.sge_gnl_datos_complem
WHERE partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';