set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.dim_inv_codigos_fondos
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones') }}')
select
    cast(f.manager_id as int) as cod_inv_id_sociedad_gerente,
    manager_fund_code as cod_inv_fondo_gerente,
    f.manager_fund_class as cod_inv_fondo_clase,
    f.fund_code as cod_inv_fondo_depositaria,
    f.name as ds_inv_fondo,
    f.currency as cod_inv_moneda,
    f.isin as cod_inv_isin,
    f.fund_cnv_code as cod_inv_cnv,
    f.fund_custody_code as cod_inv_custodia,
    f.`format` as cod_inv_formato_fondo,
    cast(f.lack_days as int) as int_inv_carencia,
    cast(f.fund_type_id as int) as cod_inv_tipo_fondo,
    f.fact_sheet_url as ds_inv_rendimiento,
    f.regulation_url as ds_inv_reglamento,
    cast(f.status_id as int) as cod_inv_estado,
    cast(f.is_enabled as int) as flag_inv_habilitado
from bi_corp_staging.fm_maestro_fondos f
where partition_date=IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones') }}'<'2021-03-15','2021-03-15','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones') }}');