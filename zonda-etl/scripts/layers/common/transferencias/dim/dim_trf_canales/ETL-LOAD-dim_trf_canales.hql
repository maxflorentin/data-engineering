set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

--inserto dimension a foto completa en cada particion desde ATRC + ABAE + tcdtgen para cuentas.

INSERT OVERWRITE TABLE bi_corp_common.dim_trf_canales
PARTITION (partition_date)
select distinct
    trim(atrc.codigo)                  cod_trf_canal,
    case when upper(trim(atrc.descripcion)) = '' then null
          else
            upper(trim(atrc.descripcion))
    end ds_trf_canal,
    case when trim(atrc.fecha_ult_modif) in ('9999-12-31', '0001-01-01','0000-00-00')  then null else trim(atrc.fecha_ult_modif) end dt_trf_fecha_ult_modif,
    param.ds_trf_canal_normalizado     ds_trf_canal_unificado,
    'ATRC'                             cod_trf_origen,
    '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}' partition_date
from bi_corp_staging.atrc_canales atrc
left join bi_corp_common.dim_trf_param_canales param
on (trim(atrc.codigo) = param.cod_trf_canal
    and param.cod_trf_origen = 'ATRC' )
where atrc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}'

union all

select distinct
	trim (abae.canal)				   cod_trf_canal,
    case when upper(trim(abae.descripcion)) = '' then null
          else
            upper(trim(abae.descripcion))
    end ds_trf_canal,
	abae.partition_date 		       dt_trf_fecha_ult_modif,
    param.ds_trf_canal_normalizado     ds_trf_canal_unificado,
    'ABAE'                             cod_trf_origen,
    '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}' partition_date
from bi_corp_staging.abae_canales abae
left join bi_corp_common.dim_trf_param_canales param
on (trim(abae.canal) = param.cod_trf_canal
	and param.cod_trf_origen = 'ABAE')
where abae.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}'

union all

select distinct
	substr(tcd.gen_clave,1,2)         cod_trf_canal,
    trim(substr(tcd.gen_Datos,1,40))  ds_trf_canal,
	tcd.partition_date 		          dt_trf_fecha_ult_modif,
	ds_trf_canal_normalizado          ds_trf_canal_unificado,
	'MALBGC'       		              cod_trf_origen,
	tcd.partition_date                partition_date
from bi_corp_staging.tcdtgen tcd
left join bi_corp_common.dim_trf_param_canales param
on (substr(tcd.gen_clave,1,2) = param.cod_trf_canal
    and param.cod_trf_origen = 'MALBGC' )
where tcd.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}'
and tcd.gentabla = '0222'
and substr(tcd.gen_clave,1,2) not in ('97','98','99')

union all

select
    '97'                           cod_trf_canal,
    'ONLINE BANKING AGENDAMIENTO'  ds_trf_canal,
     '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}' dt_trf_fecha_ult_modif,
	 'OBP'                         ds_trf_canal_unificado,
	'MALBGC'       		           cod_trf_origen,
	'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}'  partition_date

union all

select
    '98'                           cod_trf_canal,
    'OBE-API'                      ds_trf_canal,
     '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}' dt_trf_fecha_ult_modif,
	 'OBE'                         ds_trf_canal_unificado,
	'MALBGC'       		           cod_trf_origen,
	'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}'  partition_date

union all

select
    '99'                           cod_trf_canal,
    'OBP'  ds_trf_canal,
     '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}' dt_trf_fecha_ult_modif,
	 'OBP'                         ds_trf_canal_unificado,
	'MALBGC'       		           cod_trf_origen,
'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}'  partition_date;