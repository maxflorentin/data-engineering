set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS max_reest_dia_anterior;
CREATE TEMPORARY TABLE max_reest_dia_anterior AS
SELECT max(partition_date) AS partition_date
FROM bi_corp_common.stk_pre_reestructuraciones
WHERE partition_date > date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}',7)
and partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}';

DROP TABLE IF EXISTS ree_last;
CREATE TEMPORARY TABLE ree_last AS
select s.*
FROM bi_corp_common.stk_pre_reestructuraciones s
inner join max_reest_dia_anterior m on m.partition_date=s.partition_date
WHERE s.partition_date > date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}',7)
AND s.partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}';

DROP TABLE IF EXISTS ree_new;
CREATE TEMPORARY TABLE ree_new AS
select *
FROM bi_corp_common.stk_pre_reestructuraciones
WHERE partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}';

DROP TABLE IF EXISTS max_bajas_dia_anterior;
CREATE TEMPORARY TABLE max_bajas_dia_anterior AS
SELECT max(partition_date) AS partition_date
FROM bi_corp_common.bt_pre_bajasreestructuraciones
WHERE partition_date > date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}',7)
and partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}';


DROP TABLE IF EXISTS ree_bajas;
CREATE TEMPORARY TABLE ree_bajas AS
select s.*
FROM bi_corp_common.bt_pre_bajasreestructuraciones s
inner join max_bajas_dia_anterior m on m.partition_date=s.partition_date
WHERE s.partition_date > date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}',7)
AND s.partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}';


INSERT OVERWRITE TABLE bi_corp_common.bt_pre_bajasreestructuraciones
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}')
SELECT *
FROM (

select
l.cod_suc_sucursal,
l.cod_nro_cuenta,
l.cod_per_nup,
l.cod_prod_producto,
l.cod_prod_subproducto,
l.cod_div_divisa,
l.dt_pre_fechaaltaproducto,
l.partition_date as dt_pre_fechabaja,
l.ds_pre_tiporeestructuracion,
l.flag_pre_mora,
l.flag_pre_vigilanciaespecial,
l.ds_pre_subestandar,
l.ds_pre_estadogestion,
l.cod_pre_segmento,
l.ds_pre_segmento,
l.ds_pre_renta,
l.ds_pre_categoriaproducto,
l.flag_pre_normalizado,
l.ds_pre_bucket,
l.cod_pre_tipoclasificacion,
l.ds_pre_tipoclasificacion,
l.ds_pre_tipomovimiento,
l.ds_pre_origendereestructuracion,
l.fc_pre_reestructuracionsucesiva,
l.fc_pre_diasatraso,
l.fc_pre_importeriesgo,
l.fc_pre_importedispuesto,
l.fc_pre_porcentajeprestamohipotecario,
l.fc_pre_porcentajeprestamoprendario,
l.fc_pre_porcentajeprestamopersonales,
l.fc_pre_porcentajetarjetacredito,
l.fc_pre_porcentajecuentacorriente,
l.fc_pre_porcentajeotroproducto,
l.fc_pre_porcentajedeudapendientepago,
l.fc_pre_porcentajedeudapagada
from ree_last l
left join ree_new n on l.cod_suc_sucursal=n.cod_suc_sucursal and l.cod_nro_cuenta=n.cod_nro_cuenta and l.cod_per_nup=n.cod_per_nup and l.cod_prod_producto=n.cod_prod_producto and l.cod_prod_subproducto=n.cod_prod_subproducto and l.cod_div_divisa=n.cod_div_divisa
where n.cod_suc_sucursal is null and n.cod_nro_cuenta is null and n.cod_per_nup is null and n.cod_prod_producto is null and n.cod_prod_subproducto is null and n.cod_div_divisa is null

union all

select
b.cod_suc_sucursal,
b.cod_nro_cuenta,
b.cod_per_nup,
b.cod_prod_producto,
b.cod_prod_subproducto,
b.cod_div_divisa,
b.dt_pre_fechaaltaproducto,
b.dt_pre_fechabaja,
b.ds_pre_tiporeestructuracion,
b.flag_pre_mora,
b.flag_pre_vigilanciaespecial,
b.ds_pre_subestandar,
b.ds_pre_estadogestion,
b.cod_pre_segmento,
b.ds_pre_segmento,
b.ds_pre_renta,
b.ds_pre_categoriaproducto,
b.flag_pre_normalizado,
b.ds_pre_bucket,
b.cod_pre_tipoclasificacion,
b.ds_pre_tipoclasificacion,
b.ds_pre_tipomovimiento,
b.ds_pre_origendereestructuracion,
b.fc_pre_reestructuracionsucesiva,
b.fc_pre_diasatraso,
b.fc_pre_importeriesgo,
b.fc_pre_importedispuesto,
b.fc_pre_porcentajeprestamohipotecario,
b.fc_pre_porcentajeprestamoprendario,
b.fc_pre_porcentajeprestamopersonales,
b.fc_pre_porcentajetarjetacredito,
b.fc_pre_porcentajecuentacorriente,
b.fc_pre_porcentajeotroproducto,
b.fc_pre_porcentajedeudapendientepago,
b.fc_pre_porcentajedeudapagada
from ree_bajas b

) t;