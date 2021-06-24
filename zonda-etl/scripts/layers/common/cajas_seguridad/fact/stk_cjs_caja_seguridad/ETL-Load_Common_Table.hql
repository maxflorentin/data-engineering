set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

--------------Calculo la maxima particion PEDT001
DROP TABLE IF EXISTS tmp_maxpart001;
CREATE TEMPORARY TABLE tmp_maxpart001 AS
select max(partition_date) AS partition_date
  from bi_corp_Staging.malpe_pedt001
 WHERE partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CajaSeg') }}',7)
  and partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CajaSeg') }}';

--------------Calculo la maxima particion PEDT008
DROP TABLE IF EXISTS tmp_maxpart008;
CREATE TEMPORARY TABLE tmp_maxpart008 AS
select max(partition_date) AS partition_date
  from bi_corp_Staging.malpe_pedt008
 where partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CajaSeg') }}',7)
  and partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CajaSeg') }}'
   and pecodpro = '80' and pecalpar = 'TI' and peestrel = 'A' and pefecbrb = '9999-12-31'
   and pecdgent = '0072';

DROP TABLE IF EXISTS tmp_titular_contrato;
CREATE TEMPORARY TABLE tmp_titular_contrato AS
SELECT pdt8.contrato,
       pdt8.sucursal,
       pdt8.nup,
       pdt1.tipo_persona
  FROM
(select contrato,
        sucursal,
        nup
  from (
        select p.penumcon contrato,p.pecodofi sucursal ,p.penumper nup,ROW_NUMBER () OVER ( partition by P.penumcon, P.pecodofi, P.pecodpro, P.pecodsub ORDER BY P.peordpar ) as orden
        from bi_corp_Staging.malpe_pedt008 p
        INNER JOIN tmp_maxpart008 mx8 on p.partition_date=mx8.partition_date
        where pecodpro = '80' and pecalpar = 'TI' and peestrel = 'A' and pefecbrb = '9999-12-31'
          and pecdgent = '0072'
       ) x
 where orden = 1) pdt8
INNER JOIN (select p1.penumper nup,p1.petipper tipo_persona
             from bi_corp_staging.malpe_pedt001 p1
            inner join tmp_maxpart001 mx1 on p1.partition_date=mx1.partition_date) pdt1
ON pdt8.nup = pdt1.nup
;

DROP TABLE IF EXISTS tmp_tarifas;
CREATE TEMPORARY TABLE tmp_tarifas AS
select comision,
       CASE
              when comision = '01' then 'CSCC - Comision por cambio de combinaciones'
              when comision = '02' then 'CSGC - Comision por gastos en cerrajerias'
              when comision = '03' then 'CSGV - Comision por gastos varios'
              when comision = '04' then 'CSLO - Comision por locacion'
       end as ds_comision,
       zona,
       grupo_caja,
       tipo_persona,
       fec_vig_desde,
       fec_vig_hasta,
       cast(imp_comsion as int)/100 as imp_comsion
from bi_corp_staging.acse_tarifas
where partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CajaSeg') }}'
  and comision = '04' and fec_vig_hasta = '99991231'
  and zona = '01' -- la utilizada actualmente luego se debera tomar de la interface WACSE060 (zonas de sucursales)
order by grupo_caja,tipo_persona;

DROP TABLE IF EXISTS tmp_modulos_fisicos;
CREATE TEMPORARY TABLE tmp_modulos_fisicos AS
select sucursal_caja,
       sector_caja,
       numero_caja,
       tipo_caja,
       estado,
       ind_ocupacion,
       fecha_alta,
       fecha_baja
from bi_corp_staging.acse_modulos_fisicos
where partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CajaSeg') }}'
  and estado = 'A';

insert overwrite table bi_corp_common.stk_cjs_caja_seguridad
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CajaSeg') }}')
select
    CAST(mf.sucursal_caja AS BIGINT) cod_cjs_suc_caja,
    CAST(mf.sector_caja AS BIGINT) cod_cjs_sector_caja,
    CAST(mf.numero_caja AS BIGINT) cod_cjs_numero_caja,
    CAST(mf.tipo_caja AS BIGINT) cod_cjs_tipo_caja,
    TRIM(mf.ind_ocupacion) cod_cjs_ind_ocupacion,
    case when CONCAT(SUBSTRING(mf.fecha_alta,1,4),'-',SUBSTRING(mf.fecha_alta,5,2),'-',SUBSTRING(mf.fecha_alta,7,2))
       in ('9999-12-31', '0001-01-01','0000-00-00') then NULL
     else CONCAT(SUBSTRING(mf.fecha_alta,1,4),'-',SUBSTRING(mf.fecha_alta,5,2),'-',SUBSTRING(mf.fecha_alta,7,2)) end as dt_cjs_fecha_alta ,
    CAST(c.sucursal AS BIGINT) cod_cjs_suc_contrato,
    CAST(c.contrato AS BIGINT) cod_cjs_contrato,
    case when CONCAT(SUBSTRING(c.fec_locacion,1,4),'-',SUBSTRING(c.fec_locacion,5,2),'-',SUBSTRING(c.fec_locacion,7,2))
       in ('9999-12-31', '0001-01-01','0000-00-00') then NULL
     else CONCAT(SUBSTRING(c.fec_locacion,1,4),'-',SUBSTRING(c.fec_locacion,5,2),'-',SUBSTRING(c.fec_locacion,7,2)) end as dt_cjs_fec_locacion ,
    case when CONCAT(SUBSTRING(c.fec_vencimiento,1,4),'-',SUBSTRING(c.fec_vencimiento,5,2),'-',SUBSTRING(c.fec_vencimiento,7,2))
       in ('9999-12-31', '0001-01-01','0000-00-00') then NULL
     else CONCAT(SUBSTRING(c.fec_vencimiento,1,4),'-',SUBSTRING(c.fec_vencimiento,5,2),'-',SUBSTRING(c.fec_vencimiento,7,2)) end as dt_cjs_fec_vencimiento ,
    TRIM(c.forma_pago) cod_cjs_forma_pago,
    CASE
     when c.forma_pago = 'D' then 'DEBITO'
     when c.forma_pago = 'E' then 'EFECTIVO'
     when c.forma_pago = 'A' then 'ANULACION DE COMISION'
    end as ds_cjs_forma_pago,
    CAST(c.entidad_debito AS BIGINT) cod_cjs_entidad_debito,
    CAST(c.sucursal_debito AS BIGINT) cod_cjs_sucursal_debito,
    CAST(c.cuenta_debito AS BIGINT) cod_cjs_cuenta_debito,
    TRIM(c.divisa_debito) cod_cjs_divisa_debito,
    CAST(c.frecuencia AS BIGINT) cod_cjs_frecuencia,
    CAST(c.cod_bonificacion AS BIGINT) cod_cjs_bonificacion,
    TRIM(c.id_deuda_pendiente) cod_cjs_deuda_pendiente,
    case when CONCAT(SUBSTRING(c.fec_alta,1,4),'-',SUBSTRING(c.fec_alta,5,2),'-',SUBSTRING(c.fec_alta,7,2))
       in ('9999-12-31', '0001-01-01','0000-00-00') then NULL
     else CONCAT(SUBSTRING(c.fec_alta,1,4),'-',SUBSTRING(c.fec_alta,5,2),'-',SUBSTRING(c.fec_alta,7,2)) end as dt_cjs_fec_alta_contrato ,
    TRIM(c.ind_brio) cod_cjs_ind_brio ,
	tc.cod_cjs_grupo_caja,
    tc.ds_cjs_grupo_caja,
    tc.cod_cjs_medida_caja,
    CAST(ptc.nup AS BIGINT) cod_per_nup,
    TRIM(ptc.tipo_persona) cod_per_tipo_persona,
    CAST(t.comision AS BIGINT) cod_cjs_comision,
    TRIM(t.ds_comision) ds_cjs_comision,
    CAST(t.zona AS BIGINT) cod_cjs_zona,
    case when CONCAT(SUBSTRING(t.fec_vig_desde,1,4),'-',SUBSTRING(t.fec_vig_desde,5,2),'-',SUBSTRING(t.fec_vig_desde,7,2))
       in ('9999-12-31', '0001-01-01','0000-00-00') then NULL
     else CONCAT(SUBSTRING(t.fec_vig_desde,1,4),'-',SUBSTRING(t.fec_vig_desde,5,2),'-',SUBSTRING(t.fec_vig_desde,7,2)) end as dt_cjs_fec_vig_desde ,
    case when CONCAT(SUBSTRING(t.fec_vig_hasta,1,4),'-',SUBSTRING(t.fec_vig_hasta,5,2),'-',SUBSTRING(t.fec_vig_hasta,7,2))
       in ('9999-12-31', '0001-01-01','0000-00-00') then NULL
     else CONCAT(SUBSTRING(t.fec_vig_hasta,1,4),'-',SUBSTRING(t.fec_vig_hasta,5,2),'-',SUBSTRING(t.fec_vig_hasta,7,2)) end as dt_cjs_fec_vig_hasta ,
    TRIM(c.campania) cod_cjs_campania,
    cast(t.imp_comsion as decimal(13,2)) fc_cjs_imp_comision
from tmp_modulos_fisicos mf
inner join bi_corp_common.dim_cjs_tipos_caja tc on CAST(mf.tipo_caja AS BIGINT) = tc.cod_cjs_tipo_caja and tc.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CajaSeg') }}'
left outer join bi_corp_staging.acse_contratos c on mf.sucursal_caja = c.sucursal_caja and mf.sector_caja = c.sector_caja and mf.numero_caja = c.numero_caja and c.estado = 'A' and c.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CajaSeg') }}'
left outer join tmp_titular_contrato ptc on c.contrato = ptc.contrato and mf.sucursal_caja = ptc.sucursal
left outer join tmp_tarifas t on tc.cod_cjs_grupo_caja = CAST(t.grupo_caja AS BIGINT) and ptc.tipo_persona = t.tipo_persona;
