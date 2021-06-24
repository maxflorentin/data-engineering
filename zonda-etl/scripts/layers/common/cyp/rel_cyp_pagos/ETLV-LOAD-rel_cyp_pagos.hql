set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;


INSERT OVERWRITE TABLE bi_corp_common.rel_cyp_pagos
PARTITION(partition_date)


select
waprp035_ide_pgo_origen					cod_cyp_ide_pgoorigen,
waprp035_cod_form_pgo					cod_cyp_form_pgo,
waprp035_cod_mone						cod_cyp_moneda,
waprp035_nro_instr 						ds_cyp_nro_instr,
waprp035_ide_pgo_destino				cod_cyp_ide_pgodestino,
waprp035_tpo_relacion					ds_cyp_tpo_relacion,
waprp035_nro_empr_origen				cod_cyp_nro_emprorigen,
waprp035_nro_dig_origen					cod_cyp_nro_digorigen,
waprp035_nro_prod_origen				cod_cyp_nro_prodorigen,
waprp035_nro_instan_origen				ds_cyp_nro_intanorigen,
waprp035_nro_empr_destino				cod_cyp_nro_emprdestino,
waprp035_nro_dig_destino				cod_cyp_nro_digdestino,
waprp035_nro_prod_destino				ds_cyp_nro_intandestino,
waprp035_fec_pgo_origen					dt_cyp_pgo_origen,
waprp035_fec_pgo_destino 				dt_cyp_pgo_destino,
cast(waprp035_nro_cuit_clte as bigint)	ds_cyp_cuit_clte,
waprp035_cod_est_relacion				cod_cyp_est_relacion,
partition_date							partition_date
from   bi_corp_staging.aprp_waprp035
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}';
