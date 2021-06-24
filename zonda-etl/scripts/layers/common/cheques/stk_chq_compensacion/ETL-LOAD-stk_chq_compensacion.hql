set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS pedt008_use;
CREATE TEMPORARY TABLE pedt008_use AS
select p.pecdgent, p.pecodofi, p.penumcon, p.pecodpro, p.peordpar, p.penumper
from bi_corp_staging.malpe_pedt008 p
inner join
(select pecdgent, pecodofi, penumcon, pecodpro, max(PEORDPAR) peordpar
from
(select pecdgent, pecodofi, penumcon, pecodpro, min(cast(case when REGEXP_REPLACE(PEORDPAR, "^0+", '')='' then '0' else REGEXP_REPLACE(PEORDPAR, "^0+", '') end as int)) PEORDPAR
from bi_corp_staging.malpe_pedt008
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}'
and (PEFECBRB ='9999-12-31' and trim(PEMOTBAJ)='')
group by pecdgent, PECODOFI,penumcon, pecodpro

union all

select pecdgent, pecodofi, penumcon, pecodpro, min(cast(case when REGEXP_REPLACE(PEORDPAR, "^0+", '')='' then '0' else REGEXP_REPLACE(PEORDPAR, "^0+", '') end as int)) PEORDPAR
from bi_corp_staging.malpe_pedt008
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}'
group by pecdgent, PECODOFI,penumcon, pecodpro) t1
group by pecdgent, pecodofi, penumcon, pecodpro) t2 on t2.pecdgent=p.pecdgent and t2.pecodofi=p.pecodofi and t2.penumcon=p.penumcon and t2.pecodpro=p.pecodpro and t2.peordpar=cast(REGEXP_REPLACE(p.PEORDPAR, "^0+", '')as int)
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}';


insert overwrite table bi_corp_common.stk_chq_compensacion
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}')
select
lpad(bh.banco_cmc7, 4, '0') as cod_chq_entidad ,
cast(bh.nro_cheque_cmc7 as bigint) as cod_chq_nrocheque ,
cast(bh.nro_cuenta_cmc7 as bigint) as cod_chq_cuentacheque ,
cast(lpad(bh.sucursal_cmc7, 4, '0') as bigint) as cod_chq_sucursalgirada,
cast(bh.cod_postal as int) as cod_chq_codigopostal,
trim(bh.bco_recep) as ds_chq_bancoemisor,
cast(bh.suc_acred as bigint) as cod_suc_sucacred,
cast(bh.cta_acred as bigint) as cod_chq_ctaacred,
cast(trim(bh.suc_girada) as bigint) as cod_suc_sucgirada,
cast(trim(bh.cta_girada) as bigint) as cod_chq_ctagirada,
bh.moneda as cod_chq_divisa,
bh.estado as ds_chq_estado,
concat(substring(bh.fecha_compensacion,1,4),'-',substring(bh.fecha_compensacion,5,2),'-',substring(bh.fecha_compensacion,7,2)) as dt_chq_fechacompensacion,
cast(p1.penumper as bigint) as cod_per_nupgirada,
cast(p2.penumper as bigint) as cod_per_nupacred,
null as cod_chq_segmento,
null as cod_chq_agrupacionsegmento,
null as cod_chq_subsegmento,
null as cod_chq_clasesegmento,
coalesce(cast(trim(concat(substr(REGEXP_REPLACE(bh.importe, "^0+", ''),1,length(REGEXP_REPLACE(bh.importe, "^0+", ''))-2),'.',substr(REGEXP_REPLACE(bh.importe, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_monto,
case when canal_ingreso in (40, 72, 73, 74) and tipo_cheque in ('C','M') then 'CD'
 when canal_ingreso in (31, 50, 60, 61, 62, 63) and tipo_cheque in ('C','M') then 'PD'
 when tipo_cheque in ('C','M') then 'RE'
 when tipo_cheque = 'B' then 'CD'
 when canal_ingreso in (60, 61, 62, 63) then 'CC'
 when canal_ingreso = '31' then 'CE'
 when canal_ingreso = '30' then 'NE'
 when canal_ingreso = '50' then 'FI' end as cod_chq_productocontab,
bh.tipo_cheque as cod_chq_tipocanje,
null as cod_chq_producto,
null as cod_chq_subproducto,
cast(suc_origen as bigint) as cod_suc_sucursaltrx,
canal_ingreso as cod_chq_canaltrx,
case when tipo_cheque='C' then 'CANJE INTERNO'
 when tipo_cheque= 'M' then 'CAMARA REMITIDA'
 when tipo_cheque= 'B' then 'CAMARA RECIBIDA'
 when tipo_cheque= 'P' then 'PAGADOR'
 when tipo_cheque= 'S' then 'SOBRANTE' end as ds_chq_tipocheque,
case when bh.fec_pres in ('00000000','') then null else concat(substring(bh.fec_pres,1,4),'-',substring(bh.fec_pres,5,2),'-',substring(bh.fec_pres,7,2)) end as dt_chq_fechapresentacion,
case when bh.fec_deposito in ('00000000','') then null else concat(substring(bh.fec_deposito,1,4),'-',substring(bh.fec_deposito,5,2),'-',substring(bh.fec_deposito,7,2)) end as dt_chq_fechadeposito,
concat(bh.banco_cmc7, bh.sucursal_cmc7, bh.nro_cheque_cmc7, bh.nro_cuenta_cmc7, bh.cod_postal) as ds_chq_cmc7
from bi_corp_staging.acle_base_historica_compensacion bh
left join pedt008_use p1 on p1.pecdgent = '0072' and bh.suc_girada = p1.PECODOFI and substring(bh.cta_girada, 4) = substring(p1.penumcon, 4) and substring(bh.cta_girada, 2, 2) = p1.pecodpro
left join pedt008_use p2 on p2.pecdgent = '0072' and bh.suc_acred = p2.PECODOFI and substring(bh.cta_acred, 4) = substring(p2.penumcon, 4) and substring(bh.cta_acred, 2, 2) = p2.pecodpro
where bh.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}'
and bh.estado <> 'R';