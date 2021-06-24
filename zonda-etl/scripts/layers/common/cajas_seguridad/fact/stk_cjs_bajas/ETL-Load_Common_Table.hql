set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.stk_cjs_bajas
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CajaSeg') }}')
select CAST(x.sucursal_caja AS BIGINT) cod_suc_caja,
       CAST(x.sector_caja AS BIGINT) cod_cjs_sector_caja,
       CAST(x.numero_caja AS BIGINT) cod_cjs_numero_caja,
       CAST(x.tipo_caja AS BIGINT) cod_cjs_tipo_caja,
       CAST(x.nup AS BIGINT) cod_per_nup,
       case when CONCAT(SUBSTRING(x.fecha_alta,1,4),'-',SUBSTRING(x.fecha_alta,5,2),'-',SUBSTRING(x.fecha_alta,7,2))
             in ('9999-12-31', '0001-01-01','0000-00-00') then NULL
            else CONCAT(SUBSTRING(x.fecha_alta,1,4),'-',SUBSTRING(x.fecha_alta,5,2),'-',SUBSTRING(x.fecha_alta,7,2)) end as dt_cjs_fecha_alta ,
       case when CONCAT(SUBSTRING(x.fecha_baja,1,4),'-',SUBSTRING(x.fecha_baja,5,2),'-',SUBSTRING(x.fecha_baja,7,2))
             in ('9999-12-31', '0001-01-01','0000-00-00') then NULL
            else CONCAT(SUBSTRING(x.fecha_baja,1,4),'-',SUBSTRING(x.fecha_baja,5,2),'-',SUBSTRING(x.fecha_baja,7,2)) end as dt_cjs_fecha_baja ,
       CAST(x.sucursal AS BIGINT) cod_cjs_suc_contrato,
       CAST(x.contrato AS BIGINT) cod_cjs_contrato,
       case when CONCAT(SUBSTRING(x.fec_locacion,1,4),'-',SUBSTRING(x.fec_locacion,5,2),'-',SUBSTRING(x.fec_locacion,7,2))
              in ('9999-12-31', '0001-01-01','0000-00-00') then NULL
            else CONCAT(SUBSTRING(x.fec_locacion,1,4),'-',SUBSTRING(x.fec_locacion,5,2),'-',SUBSTRING(x.fec_locacion,7,2)) end as dt_cjs_fec_locacion,
       CASE WHEN TRIM(x.contrato) = '' THEN 'MEDIO_FISICO'
            ELSE 'CONTRATO' end cod_marca_baja,
       CAST(x.entidad_debito AS BIGINT) cod_cjs_entidad_debito,
       CAST(x.sucursal_debito AS BIGINT) cod_cjs_sucursal_debito,
       CAST(x.cuenta_debito AS BIGINT) cod_cjs_cuenta_debito,
       case when TRIM(x.divisa_debito) = '' then NULL
            else TRIM(x.divisa_debito) end as cod_cjs_divisa_debito,
       CAST(x.frecuencia AS BIGINT) cod_cjs_frecuencia,
       CAST(x.cod_bonificacion AS BIGINT) cod_cjs_bonificacion,
       TRIM(x.id_deuda_pendiente) cod_cjs_deuda_pendiente,
       TRIM(x.ind_brio) cod_cjs_ind_brio,
       case when CONCAT(SUBSTRING(x.fec_vencimiento,1,4),'-',SUBSTRING(x.fec_vencimiento,5,2),'-',SUBSTRING(x.fec_vencimiento,7,2))
              in ('9999-12-31', '0001-01-01','0000-00-00') then NULL
            else CONCAT(SUBSTRING(x.fec_vencimiento,1,4),'-',SUBSTRING(x.fec_vencimiento,5,2),'-',SUBSTRING(x.fec_vencimiento,7,2)) end as dt_cjs_fec_vencimiento,
       TRIM(x.forma_pago) cod_cjs_forma_pago,
       CASE
         when x.forma_pago = 'D' then 'DEBITO'
         when x.forma_pago = 'E' then 'EFECTIVO'
         when x.forma_pago = 'A' then 'ANULACION DE COMISION'
       end as ds_cjs_forma_pago,
       case when TRIM(x.usuario_alta) = '' then NULL
            else TRIM(x.usuario_alta) end as cod_cjs_usu_alta,
       case when TRIM(x.usuario_ult_act) = '' then NULL
            else TRIM(x.usuario_ult_act) end as cod_cjs_usu_ult_act,
       case when CONCAT(SUBSTRING(x.fecha_ult_act,1,4),'-',SUBSTRING(x.fecha_ult_act,5,2),'-',SUBSTRING(x.fecha_ult_act,7,2))
             in ('9999-12-31', '0001-01-01','0000-00-00','0000-00-01') then NULL
            else CONCAT(SUBSTRING(x.fecha_ult_act,1,4),'-',SUBSTRING(x.fecha_ult_act,5,2),'-',SUBSTRING(x.fecha_ult_act,7,2)) end as dt_cjs_fecha_ult_act ,
       case when TRIM(X.ult_periodo_liq) in ('999912', '000101','000000','','0') then NULL
            else TRIM(x.ult_periodo_liq) end as cod_cjs_ult_periodo_liq ,
       TRIM(x.campania) cod_cjs_campania,
       case when CONCAT(SUBSTRING(x.fec_vto_camp,1,4),'-',SUBSTRING(x.fec_vto_camp,5,2),'-',SUBSTRING(x.fec_vto_camp,7,2))
             in ('9999-12-31', '0001-01-01','0000-00-00','0000-00-01') then NULL
            else CONCAT(SUBSTRING(x.fec_vto_camp,1,4),'-',SUBSTRING(x.fec_vto_camp,5,2),'-',SUBSTRING(x.fec_vto_camp,7,2)) end as dt_cjs_fec_vto_camp ,
       case when TRIM(x.fec_proxima_liq) in ('','0') then NULL
            when TRIM(x.fec_proxima_liq)  in ('9999-12-31', '0001-01-01','0000-00-00','0000-00-01') then NULL
            else TRIM(x.fec_proxima_liq) end as dt_cjs_fec_proxima_liq,
       case when CONCAT(SUBSTRING(x.ult_aniomes_desde_liq,1,4),'-',SUBSTRING(x.ult_aniomes_desde_liq,5,2),'-01')
             in ('9999-12-31', '0001-01-01','0000-00-00','0000-00-01') then NULL
            else CONCAT(SUBSTRING(x.ult_aniomes_desde_liq,1,4),'-',SUBSTRING(x.ult_aniomes_desde_liq,5,2),'-01') end as dt_cjs_ult_aniomes_desde_liq ,
       case when CONCAT(SUBSTRING(x.ult_aniomes_hasta_liq,1,4),'-',SUBSTRING(x.ult_aniomes_hasta_liq,5,2),'-01')
             in ('9999-12-31', '0001-01-01','0000-00-00','0000-00-01') then NULL
            else CONCAT(SUBSTRING(x.ult_aniomes_hasta_liq,1,4),'-',SUBSTRING(x.ult_aniomes_hasta_liq,5,2),'-01') end as dt_cjs_ult_aniomes_hasta_liq
from (
select null sucursal,
       mf.contrato,
       mf.sucursal_caja,
       mf.sector_caja,
       mf.numero_caja,
       mf.tipo_caja,
       null nup,
       mf.estado,
       null forma_operar,
       null fec_locacion,
       null fec_vencimiento,
       mf.ind_ocupacion,
       mf.fecha_alta,
       mf.fecha_baja,
       null forma_pago,
	   null entidad_debito,
       null sucursal_debito,
       null cuenta_debito,
       null divisa_debito,
       null frecuencia,
       null cod_bonificacion,
       null id_deuda_pendiente,
       null ind_brio,
       null usuario_alta,
       mf.usuario_ult_act,
	   null fecha_ult_act,
       null ult_periodo_liq,
       null campania,
       null fec_vto_camp,
       null fec_proxima_liq,
       null ult_aniomes_desde_liq,
       null ult_aniomes_hasta_liq
  from bi_corp_staging.acse_modulos_fisicos mf
 where mf.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CajaSeg') }}'
   and mf.estado = 'B'
UNION ALL
select c.sucursal,
       c.contrato,
       c.sucursal_caja,
       c.sector_caja,
       c.numero_caja,
       c.tipo_caja,
       y.nup,
       c.estado,
       c.forma_operar,
       c.fec_locacion,
       c.fec_vencimiento,
       null ind_ocupacion,
       c.fec_alta fecha_alta,
       c.fec_baja fecha_baja,
       c.forma_pago,
       c.entidad_debito,
       c.sucursal_debito,
       c.cuenta_debito,
       c.divisa_debito,
       c.frecuencia,
       c.cod_bonificacion,
       c.id_deuda_pendiente,
       c.ind_brio,
       c.usuario_alta,
       c.usuario_ult_act,
       c.fecha_ult_act,
       c.ult_periodo_liq,
       c.campania,
       c.fec_vto_camp,
       c.fec_proxima_liq,
       c.ult_aniomes_desde_liq,
       c.ult_aniomes_hasta_liq
  from bi_corp_staging.acse_contratos c
  left join (select p1.contrato,
                    p1.sucursal,
                    p1.nup,
                    p1.fec_baja
                from
                    (select  p.penumcon contrato,p.pecodofi sucursal ,p.penumper nup,p.pefecbrb fec_baja,ROW_NUMBER () OVER ( partition by P.penumcon, P.pecodofi, P.pecodpro, P.pecodsub ORDER BY P.peordpar ) as orden
                       from bi_corp_Staging.malpe_pedt008 p
                      where p.pecodpro = '80' and p.pecalpar = 'TI'
                        and p.pecodsub = '0000'
                        and p.pecdgent = '0072'
                        and p.partition_date = date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CajaSeg') }}',1)
                    ) p1
                    where p1.orden = 1) y
   on y.contrato = c.contrato and y.sucursal = c.sucursal
 where c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CajaSeg') }}'
   and c.estado = 'B') x;