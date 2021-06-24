SET mapred.job.queue.name=root.dataeng;
INSERT OVERWRITE TABLE bi_corp_common.dim_rec_circ_resolutor
select
    trim(cod_entidad) as cod_entidad
    ,trim(ide_circuito) ide_circuito
    ,trim(cod_sector) cod_sector
    ,trim(nro_ord_circ) nro_ord_circ
    ,trim(cod_prior_circ) cod_prior_circ
    ,case when trim(ind_obl_resp)='S' then 1 else 0 end ind_obl_resp
    ,trim(tmp_circ_resol) tmp_circ_resol
    ,trim(est_circ_resol) est_circ_resol
    ,trim(user_alt_circ_resol) user_alt_circ_resol
    ,trim(fec_alt_circ_resol) fec_alt_circ_resol
    ,trim(user_modf_circ_resol) user_modf_circ_resol
    ,trim(fec_modf_circ_resol) fec_modf_circ_resol
	,partition_date
from bi_corp_staging.rio56_circ_resolutor
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_SGC-Daily_scheduled') }}'