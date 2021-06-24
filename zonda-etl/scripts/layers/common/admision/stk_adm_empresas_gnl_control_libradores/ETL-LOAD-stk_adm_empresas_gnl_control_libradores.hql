set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_gnl_control_libradores
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
SELECT
    cast(nro_tramite_zx as int) as cod_adm_nro_tramite_zx,
    cast(nup as int) as cod_per_nup,
    cast(id_librador as int) as id_adm_librador,
    cast(nro_doc as bigint) as cod_adm_nro_doc,
    cast(plazo as int) as int_adm_plazo,
    cast(suc_cta_credito as int) as cod_adm_suc_cta_credito,
    tipo_cta_credito as ds_adm_tipo_cta_credito,
    cast(cuenta_credito as bigint) as cod_nro_cuenta_credito,
    cast(monto_librador as decimal(16,2)) as fc_adm_monto_librador,
    mca_atomizacion as flag_adm_mca_atomizacion,
    real_atomizacion as flag_adm_real_atomizacion,
    nosis_realizado as flag_adm_nosis_realizado,
    nosis_bcra as flag_adm_nosis_bcra,
    nosis_inhabilitados as flag_adm_nosis_inhabilitados,
    nosis_cheques_rec as flag_adm_nosis_cheques_rec,
    nosis_quiebra as flag_adm_nosis_quiebra,
    nosis_concurso as flag_adm_nosis_concurso,
    observaciones as ds_adm_observaciones,
    fec_alta as ts_adm_fec_alta,
    usu_alta as ds_adm_usu_alta,
    fec_modif as ts_adm_fec_modif,
    usu_modif as ds_adm_usu_modif,
    mca_vinculado as ds_adm_mca_vinculado,
    obs_vinculado as ds_adm_obs_vinculado,
    rechazo_antecedentes_int as ds_adm_rechazo_antecedentes_int,
    tipo_cliente as cod_adm_tipo_cliente
FROM bi_corp_staging.sge_gnl_control_libradores
WHERE partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';