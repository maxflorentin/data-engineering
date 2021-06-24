set mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

with moneda as (
SELECT DISTINCT
CASE WHEN COD_BCRA IN ('null','') THEN CAST(NULL AS STRING)
	 ELSE TRIM(COD_BCRA)
END AS COD_BCRA_DIVISA,
CASE WHEN MONCOD IN ('null','') THEN CAST(NULL AS STRING)
	 ELSE MONCOD
END AS COD_DIVISA
FROM BI_CORP_STAGING.COMEX_RIO39_OPP_MONEDAS
WHERE PARTITION_DATE= IF("{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_OBCM') }}"<='2021-03-16','2021-03-16',"{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_OBCM') }}")
)


INSERT OVERWRITE TABLE bi_corp_common.bt_obcm_transacciones
PARTITION (partition_date)
select
distinct ide_empresa											    cod_obcm_empresa,
CASE WHEN TRIM(ide_usuario)='null'
	 THEN NULL ELSE TRIM(ide_usuario) END 			                cod_obcm_usuario,
CASE WHEN TRIM(tlog.ide_transaccion)='null'
	 THEN NULL ELSE TRIM(tlog.ide_transaccion) END					cod_obcm_transaccion,
CASE WHEN TRIM(nom_operacion)='null'
	 THEN NULL ELSE TRIM(nom_operacion) END						    ds_obcm_nom_operacion,
CASE WHEN TRIM(nom_producto)='null'
	 THEN NULL ELSE TRIM(nom_producto) END						    ds_obcm_nom_producto,
CASE WHEN TRIM(nom_req_frontend)='null'
	 THEN NULL ELSE TRIM(nom_req_frontend) END					    ds_obcm_nom_reqfrontend,
CASE WHEN TRIM(cod_estado_trn)='null'
	 THEN NULL ELSE TRIM(cod_estado_trn) END						ds_obcm_estado_trn,
CASE WHEN TRIM(tim_ini_trn)='null'
	 THEN NULL ELSE SUBSTRING(tim_ini_trn,1,19)  END				ts_obcm_inicio_trn,
CASE WHEN TRIM(tim_fin_trn)='null'
	 THEN NULL ELSE SUBSTRING(tim_fin_trn,1,19) END					ts_obcm_fin_trn,
cast(mon_transaccion as decimal(15,4))						        fc_obcm_monto,
cast(nro_nup_empresa AS BIGINT)						                cod_per_nup_empresa,
cast(nro_nup_individuo AS BIGINT)					                cod_per_nup_individuo,
CASE WHEN TRIM(ide_solicitud)='null'
	 THEN NULL ELSE TRIM(ide_solicitud) END						    cod_obcm_solicitud,
CASE WHEN TRIM(cod_operacion)='null'
	 THEN NULL ELSE TRIM(cod_operacion) END						    cod_obcm_operacion,
CASE WHEN TRIM(cod_estado_sol)='null'
	 THEN NULL ELSE TRIM(cod_estado_sol) END						cod_obcm_estado_sol,
cast(mon_importe1 as decimal(15,4))						            fc_obcm_monto_imp1,
CASE WHEN TRIM(cod_moneda1)='null'THEN NULL
	 WHEN TRIM(cod_moneda1)='ARS' or TRIM(cod_moneda1)='0' THEN 'ARS'
	 WHEN TRIM(cod_moneda1)='USD' or TRIM(cod_moneda1)='1' THEN 'USD'
	 ELSE mon1.cod_bcra_divisa end  								cod_div_divisa1,
cast(can_items1 AS INT)				                                ds_obcm_cant_item1,
cast(mon_importe2 as decimal(15,4))	                                fc_obcm_monto_imp2,
CASE WHEN TRIM(cod_moneda2)='null'THEN NULL
	 WHEN TRIM(cod_moneda2)='ARS' or TRIM(cod_moneda2)='0' THEN 'ARS'
	 WHEN TRIM(cod_moneda2)='USD' or TRIM(cod_moneda2)='1' THEN 'USD'
	 ELSE mon2.cod_bcra_divisa end  								cod_div_divisa2,
cast(can_items2 AS INT)				                                ds_obcm_cant_item2,
tlog.partition_date
from bi_corp_staging.obcm_tlog_tabla tlog
 join bi_corp_staging.obcm_tpcs_log_operacion top on
(tlog.ide_transaccion=top.ide_transaccion and top.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_OBCM') }}")
left join moneda mon1 on mon1.COD_DIVISA=TRIM(cod_moneda1)
left join moneda mon2 on mon2.COD_DIVISA=TRIM(cod_moneda2)
where tlog.partition_date= "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_OBCM') }}"
AND "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_OBCM') }}">='2021-03-25'

UNION ALL

select
distinct ide_empresa											    cod_obcm_empresa,
CASE WHEN TRIM(ide_usuario)='null'
	 THEN NULL ELSE TRIM(ide_usuario) END 			                cod_obcm_usuario,
CASE WHEN TRIM(tlog.ide_transaccion)='null'
	 THEN NULL ELSE TRIM(tlog.ide_transaccion) END					cod_obcm_transaccion,
CASE WHEN TRIM(nom_operacion)='null'
	 THEN NULL ELSE TRIM(nom_operacion) END						    ds_obcm_nom_operacion,
CASE WHEN TRIM(nom_producto)='null'
	 THEN NULL ELSE TRIM(nom_producto) END						    ds_obcm_nom_producto,
CASE WHEN TRIM(nom_req_frontend)='null'
	 THEN NULL ELSE TRIM(nom_req_frontend) END					    ds_obcm_nom_reqfrontend,
CASE WHEN TRIM(cod_estado_trn)='null'
	 THEN NULL ELSE TRIM(cod_estado_trn) END						ds_obcm_estado_trn,
CASE WHEN TRIM(tim_ini_trn)='null'
	 THEN NULL ELSE SUBSTRING(tim_ini_trn,1,19)  END				ts_obcm_inicio_trn,
CASE WHEN TRIM(tim_fin_trn)='null'
	 THEN NULL ELSE SUBSTRING(tim_fin_trn,1,19) END					ts_obcm_fin_trn,
cast(mon_transaccion as decimal(15,4))						        fc_obcm_monto,
cast(nro_nup_empresa AS BIGINT)						                cod_per_nup_empresa,
cast(nro_nup_individuo AS BIGINT)					                cod_per_nup_individuo,
CASE WHEN TRIM(ide_solicitud)='null'
	 THEN NULL ELSE TRIM(ide_solicitud) END						    cod_obcm_solicitud,
CASE WHEN TRIM(cod_operacion)='null'
	 THEN NULL ELSE TRIM(cod_operacion) END						    cod_obcm_operacion,
CASE WHEN TRIM(cod_estado_sol)='null'
	 THEN NULL ELSE TRIM(cod_estado_sol) END						cod_obcm_estado_sol,
cast(mon_importe1 as decimal(15,4))						            fc_obcm_monto_imp1,
CASE WHEN TRIM(cod_moneda1)='null'THEN NULL
	 WHEN TRIM(cod_moneda1)='ARS' or TRIM(cod_moneda1)='0' THEN 'ARS'
	 WHEN TRIM(cod_moneda1)='USD' or TRIM(cod_moneda1)='1' THEN 'USD'
	 ELSE mon1.cod_bcra_divisa end  								cod_div_divisa1,
cast(can_items1 AS INT)				                                ds_obcm_cant_item1,
cast(mon_importe2 as decimal(15,4))	                                fc_obcm_monto_imp2,
CASE WHEN TRIM(cod_moneda2)='null'THEN NULL
	 WHEN TRIM(cod_moneda2)='ARS' or TRIM(cod_moneda2)='0' THEN 'ARS'
	 WHEN TRIM(cod_moneda2)='USD' or TRIM(cod_moneda2)='1' THEN 'USD'
	 ELSE mon2.cod_bcra_divisa end  								cod_div_divisa2,
cast(can_items2 AS INT)				                                ds_obcm_cant_item2,
tlog.partition_date
from bi_corp_staging.rio106_tlog_tabla tlog
 join bi_corp_staging.rio106_tpcs_log_operacion top on
(tlog.ide_transaccion=top.ide_transaccion and top.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_OBCM') }}")
left join moneda mon1 on mon1.COD_DIVISA=TRIM(cod_moneda1)
left join moneda mon2 on mon2.COD_DIVISA=TRIM(cod_moneda2)
where tlog.partition_date= "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_OBCM') }}"
AND "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_OBCM') }}"<'2021-03-25'
;




