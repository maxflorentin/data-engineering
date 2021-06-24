set mapred.job.queue.name=root.dataeng;

DROP VIEW IF EXISTS bi_corp_common.vbt_div_compra_venta;
CREATE VIEW IF NOT EXISTS bi_corp_common.vbt_div_compra_venta AS
select fecha_op as dt_div_oper,
       fecha_hora_ingreso as ts_div_ingreso,
       cast(scb as bigint) as cod_suc_sucursal_boleto,
       cast(scc as bigint) as cod_suc_sucursal_cuenta,
       cast(cuenta as bigint) as cod_div_cuenta,
       trim(nro_tarjeta) as ds_div_nro_tarjeta,
       cast(nup as bigint) as cod_per_nup,
       trim(can) as cod_div_canal,
       trim(desc_canal) as ds_div_canal,
       trim(clas_bol) as ds_div_clase_boleto,
       trim(cv) as ds_div_tipo_boleto,
       IF(trim(anu) = 'X',1,0) as flag_inv_boleto_anulado,
       trim(descripcion_operacion) ds_div_operacion,
       trim(bd) as ds_div_tipo_moneda,
       cast(cotizac as bigint)/1000000 as fc_div_cotizacion,
       trim(io) as cod_div_oper_origen,
       trim(descrip_cod_orig) as ds_div_oper_origen,
       trim(mori) as cod_div_moneda_origen,
       trim(desc_morig) as ds_div_moneda_origen,
       cast(monto_orig as bigint)/100 as fc_div_monto_origen,
       trim(id) as cod_div_oper_destino,
       trim(descrip_cod_dest) as ds_div_oper_destino,
       trim(mdes) as cod_div_moneda_destino,
       trim(desc_mdest) as ds_div_moneda_destino,
       cast(monto_destino as bigint)/100 as fc_div_monto_destino,
       trim(ofic_tran) as ds_div_oficial_tran,
       trim(ofic_auto) as ds_div_oficial_auto,
       partition_date
from bi_corp_staging.amec_compra_venta_mon_ext
where
    partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_maltc_tcdt081', dag_id='LOAD_CMN_Divisas-Daily') }}';