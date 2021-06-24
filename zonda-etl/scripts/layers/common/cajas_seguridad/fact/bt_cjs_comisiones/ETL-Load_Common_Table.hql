set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.bt_cjs_comisiones
partition(partition_date)
  select  CAST(c.sucursal AS BIGINT) cod_cjs_suc_contrato,
          CAST(c.contrato AS BIGINT) cod_cjs_contrato,
          CAST(c.comision AS BIGINT) cod_cjs_comision,
          CASE
              when c.comision = '01' then 'CSCC - Comision por cambio de combinaciones'
              when c.comision = '02' then 'CSGC - Comision por gastos en cerrajerias'
              when c.comision = '03' then 'CSGV - Comision por gastos varios'
              when c.comision = '04' then 'CSLO - Comision por locacion'
          end ds_cjs_comision,
          c.periodo_liq cod_cjs_periodo_liq,
          case when CONCAT(SUBSTRING(c.fecha_liq,1,4),'-',SUBSTRING(c.fecha_liq,5,2),'-',SUBSTRING(c.fecha_liq,7,2))
              in ('9999-12-31', '0001-01-01','0000-00-00') then NULL
          else CONCAT(SUBSTRING(c.fecha_liq,1,4),'-',SUBSTRING(c.fecha_liq,5,2),'-',SUBSTRING(c.fecha_liq,7,2)) end as dt_cjs_fecha_liq ,
          TRIM(c.cod_concepto) cod_cjs_concepto,
          cast((cast(c.imp_concepto as int)/100) as decimal(13,2)) as fc_cjs_imp_concepto,
          cast((cast(c.imp_impuesto as int)/100) as decimal(13,2)) as fc_cjs_imp_impuesto,
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
          TRIM(case when CONCAT(SUBSTRING(c.fecha_pago,1,4),'-',SUBSTRING(c.fecha_pago,5,2),'-',SUBSTRING(c.fecha_pago,7,2))
            in ('9999-12-31', '0001-01-01','0000-00-00') then NULL
        	else CONCAT(SUBSTRING(c.fecha_pago,1,4),'-',SUBSTRING(c.fecha_pago,5,2),'-',SUBSTRING(c.fecha_pago,7,2)) end) as dt_cjs_fecha_pago ,
          TRIM(c.codigo_canal) cod_cjs_canal,
          CASE
              when c.codigo_canal = '00' then 'SELLSTATION'
              when c.codigo_canal = '18' then 'BANCA PRIVADA'
              when c.codigo_canal = '40' then 'CRM'
              when c.codigo_canal = 'BT' then 'BATCH'
          end as ds_cjs_canal,
          CAST(c.numper AS BIGINT) cod_per_nup,
          TRIM(c.usuario_oper) cod_cjs_usuario_oper ,
          c.partition_date
  from bi_corp_staging.acse_comisiones_cobradas c
  where c.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CajaSeg') }}'
  union all
  select  CAST(p.sucursal AS BIGINT) cod_cjs_suc_contrato,
          CAST(p.contrato AS BIGINT) cod_cjs_contrato,
          CAST(p.comision AS BIGINT) cod_cjs_comision,
          CASE
              when p.comision = '01' then 'CSCC - Comision por cambio de combinaciones'
              when p.comision = '02' then 'CSGC - Comision por gastos en cerrajerias'
              when p.comision = '03' then 'CSGV - Comision por gastos varios'
              when p.comision = '04' then 'CSLO - Comision por locacion'
            end ds_cjs_comision,
          p.periodo_liq cod_cjs_periodo_liq,
          case when CONCAT(SUBSTRING(p.fecha_liq,1,4),'-',SUBSTRING(p.fecha_liq,5,2),'-',SUBSTRING(p.fecha_liq,7,2))
              in ('9999-12-31', '0001-01-01','0000-00-00') then NULL
        	else CONCAT(SUBSTRING(p.fecha_liq,1,4),'-',SUBSTRING(p.fecha_liq,5,2),'-',SUBSTRING(p.fecha_liq,7,2)) end as dt_cjs_fecha_liq ,
          TRIM(p.cod_concepto) cod_cjs_concepto,
          cast((cast(p.imp_concepto as int)/100) as decimal(13,2)) as fc_cjs_imp_concepto,
          cast((cast(p.imp_impuesto as int)/100) as decimal(13,2)) as fc_cjs_imp_impuesto,
          CASE when TRIM(p.forma_pago) = '' then null
               else TRIM(p.forma_pago) end as cod_cjs_forma_pago,
          CASE
              when p.forma_pago = 'D' then 'DEBITO'
              when p.forma_pago = 'E' then 'EFECTIVO'
              when p.forma_pago = 'A' then 'ANULACION DE COMISION'
          end as ds_cjs_forma_pago,
          CAST(p.entidad_debito AS BIGINT) cod_cjs_entidad_debito,
          CAST(p.sucursal_debito AS BIGINT) cod_cjs_sucursal_debito,
          CAST(p.cuenta_debito AS BIGINT) cod_cjs_cuenta_debito,
          case when TRIM(p.divisa_debito) = '' then null
               else TRIM(p.divisa_debito) end as cod_cjs_divisa_debito,
          TRIM(case when CONCAT(SUBSTRING(p.fecha_pago,1,4),'-',SUBSTRING(p.fecha_pago,5,2),'-',SUBSTRING(p.fecha_pago,7,2))
            in ('9999-12-31', '0001-01-01','0000-00-00') then null
        	else CONCAT(SUBSTRING(p.fecha_pago,1,4),'-',SUBSTRING(p.fecha_pago,5,2),'-',SUBSTRING(p.fecha_pago,7,2)) end) as dt_cjs_fecha_pago ,
          CASE when TRIM(p.codigo_canal) = '' then null
               else TRIM(p.codigo_canal) end as cod_cjs_canal,
          CASE
              when p.codigo_canal = '00' then 'SELLSTATION'
              when p.codigo_canal = '18' then 'BANCA PRIVADA'
              when p.codigo_canal = '40' then 'CRM'
              when p.codigo_canal = 'BT' then 'BATCH'
          end as ds_cjs_canal,
          CAST(p.numper AS BIGINT) cod_per_nup,
          CASE when TRIM(p.usuario_oper) = '' then null
               else TRIM(p.usuario_oper) end as cod_cjs_usuario_oper ,
          p.partition_date
  from bi_corp_staging.acse_comisiones_pendientes p
  where p.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CajaSeg') }}'