set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

--inserto dimension a foto completa en cada particion desde ATRC

INSERT OVERWRITE TABLE bi_corp_common.dim_trf_conceptos
PARTITION (partition_date)
select distinct
    trim(atrc.codigo)                         cod_trf_concepto,
    upper(trim(atrc.descripcion))             ds_trf_concepto,
    case when trim(atrc.fecha_ult_modif) in ('9999-12-31', '0001-01-01','0000-00-00')  then null else trim(atrc.fecha_ult_modif) end dt_trf_fecha_ult_modif,
    param.ds_trf_concepto_normalizado         ds_trf_concepto_unificado,
    'ATRC'                                    cod_trf_origen,
    '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}' partition_date
from bi_corp_staging.atrc_conceptos atrc
left join bi_corp_common.dim_trf_param_conceptos param
on (trim(atrc.codigo) = param.cod_trf_concepto
    and param.cod_trf_origen = 'ATRC' )
where atrc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}'
union all
select distinct
    trim(abae.concepto)                       cod_trf_concepto,
    upper(trim(abae.descripcion))             ds_trf_concepto,
    '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}' dt_trf_fecha_ult_modif,
     param.ds_trf_concepto_normalizado        ds_trf_concepto_unificado,
    'ABAE'                                    cod_trf_origen,
    '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}' partition_date
from bi_corp_staging.abae_conceptos abae
left join bi_corp_common.dim_trf_param_conceptos param
on (trim(abae.concepto) = param.cod_trf_concepto
    and param.cod_trf_origen = 'ABAE' )
where abae.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}';

