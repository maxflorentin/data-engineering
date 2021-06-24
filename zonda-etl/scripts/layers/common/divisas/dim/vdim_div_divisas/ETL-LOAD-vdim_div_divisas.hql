set mapred.job.queue.name=root.dataeng;

DROP VIEW IF EXISTS bi_corp_common.vdim_div_divisas;
CREATE VIEW IF NOT EXISTS bi_corp_common.vdim_div_divisas AS
select
    a.tcdt081_cddiviss cod_div_divisa,
    a.tcdt081_inddivbi cod_div_bi,
    a.tcdt081_indcot cod_div_cot,
    a.tcdt081_cambbajo/100 fc_div_cambio_bajo,
    a.tcdt081_cambalto/100 fc_div_cambio_alto,
    a.tcdt081_segmento cod_div_segmento,
    a.tcdt081_usultmod ds_div_usuario_modif,
    a.tcdt081_fhcambio as partition_date
from
    bi_corp_staging.maltc_tcdt081 a
where
    a.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_maltc_tcdt081', dag_id='LOAD_CMN_Divisas-Daily') }}';