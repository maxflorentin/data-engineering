set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.dim_rec_respuesta_resol_predef
SELECT
    trim(cod_entidad) cod_entidad,
    trim(cod_rta_resol_predef) cod_rta_resol_predef,
    trim(desc_rta_resol_predef) desc_rta_resol_predef,
    case when LENGTH(trim(desc_detall_rta_resol))=0 then null else trim(desc_detall_rta_resol) end desc_detall_rta_resol,
    trim(indi_tpo_rta)indi_tpo_rta ,
    trim(est_rta_resol_predef) est_rta_resol_predef,
    trim(user_alt_rta_resol_predef) user_alt_rta_resol_predef,
    trim(fec_alt_rta_resol_predef) fec_alt_rta_resol_predef,
    trim(user_modf_rta_resol_predef) user_modf_rta_resol_predef,
    trim(fec_modf_rta_resol_predef) fec_modf_rta_resol_predef,
    case when LENGTH(trim(cod_tipo_resolucion))=0 then null else trim(cod_tipo_resolucion) end cod_tipo_resolucion,
    trim(indi_opcion_acm) indi_opcion_acm,
    trim(sector_owner) sector_owner,
    partition_date
FROM
    bi_corp_staging.rio56_rta_resol_predef
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_SGC-Daily_scheduled') }}'
