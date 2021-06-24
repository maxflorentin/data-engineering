set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS pedt008;
CREATE TEMPORARY TABLE pedt008 AS
select p.pecodofi, p.penumcon, p.pecodpro, p.pecodsub, p.penumper
from (
      select pecodofi, penumcon, pecodpro, pecodsub, penumper, ROW_NUMBER() OVER (PARTITION BY pecodofi, penumcon, pecodpro, pecodsub ORDER BY coalesce(pefecbrb,'9999-12-31') DESC, peordpar ASC) AS min_firmante
      from bi_corp_staging.malpe_pedt008
      where (('{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt008', dag_id='LOAD_CMN_PRESTAMOS_normalizaciones-History') }}' ='None' and partition_date ='2019-01-31')
      or ('{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt008', dag_id='LOAD_CMN_PRESTAMOS_normalizaciones-History') }}' <> 'None' and partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt008', dag_id='LOAD_CMN_PRESTAMOS_normalizaciones-History') }}'))
      AND pecalpar = 'TI'
      AND pecodpro IN ('35', '36', '37', '38', '39', '71', '74')
     ) p
where p.min_firmante = 1;

INSERT OVERWRITE TABLE bi_corp_common.stk_pre_normalizaciones
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS_normalizaciones-History') }}')
select distinct nor.entidad as cod_pre_entidad,
cast(nor.centro as int) as cod_suc_sucursal,
cast(nor.contrato as bigint) as cod_nro_cuenta,
cast(pedt.penumper as int) as cod_per_nup,
cast(nor.num_sec as int) as cod_pre_nrosecuencia,
cast(num_ree as int) as cod_pre_nroree,
nor.producto as cod_prod_producto,
nor.subproducto as cod_prod_subproducto,
nor.divisa as cod_div_divisa,
nor.marca_seg_act as cod_pre_marcaclasificacionactual,
case when nor.fec_apertura in ('0001-01-01','9999-12-31') then null else nor.fec_apertura end as dt_pre_fechaapertura,
case when nor.fec_cambio_seg in ('0001-01-01','9999-12-31') then null else nor.fec_cambio_seg end as dt_pre_fechacambioclasificacion,
case when nor.fec_cura in ('0001-01-01','9999-12-31') then null else nor.fec_cura end as dt_pre_fechacura,
case when nor.fec_normalizacion in ('0001-01-01','9999-12-31') then null else nor.fec_normalizacion end as dt_pre_fechanormalizacion
from bi_corp_staging.normalizacion nor
left join pedt008 pedt on nor.centro =pedt.pecodofi and nor.contrato = pedt.penumcon and nor.producto = pedt.pecodpro and nor.subproducto = pedt.pecodsub
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS_normalizaciones-History') }}';