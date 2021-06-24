SET mapred.job.queue.name=root.dataeng;
INSERT OVERWRITE TABLE bi_corp_common.dim_rec_tipo_favorabilidad
SELECT
         trim(cod_entidad) cod_entidad
        ,trim(cod_tipo_favorabilidad) cod_tipo_favorabilidad
        ,trim(desc_tipo_favorabilidad) desc_tipo_favorabilidad
        ,trim(est_tipo_favorabilidad) est_tipo_favorabilidad
        ,trim(user_alt_tpo_favorabilidad) user_alt_tpo_favorabilidad
        ,trim(fec_alt_tpo_favorabilidad) fec_alt_tpo_favorabilidad
        ,trim(user_modf_tpo_favorabilidad) user_modf_tpo_favorabilidad
        ,trim(fec_modf_tpo_favorabilidad) fec_modf_tpo_favorabilidad
        ,trim(sector_owner) sector_owner
        ,partition_date
FROM
    bi_corp_staging.rio56_tipo_favorabilidad
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_SGC-Daily_scheduled') }}'