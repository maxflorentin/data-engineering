set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------CREACION DE TABLAS TEMPORALES-------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------

--TEMPORAL CON LA INFORMACION DE LOS ESTADOS DE LAS CUENTAS DEL DIA

DROP TABLE IF EXISTS temp_cuentas_cacs;
CREATE TEMPORARY TABLE temp_cuentas_cacs AS
select	distinct cast(num_nup as bigint) as cod_nup,
		case when cacs_state_code = '' then cast(null as string) else trim(cacs_state_code) end as ds_codigoestadocacs
from	bi_corp_staging.cacs_mf_cuentas
where 	trim(account_type) in ('CL','CY') and trim(location_code) in ('054701','063101')
and trim(state_flag) = 'A' and partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_cacs_mf_cuentas', dag_id='LOAD_CMN_TRIAD_KEYS-Daily') }}';


--TEMPORAL CON EL PERIMETRO DE CLIENTES A CONSULTAR EN TRIAD DEL DIA

DROP TABLE IF EXISTS temp_perimetro_nups;
CREATE TEMPORARY TABLE temp_perimetro_nups AS
select 	distinct cast(dcu_customer_id as bigint) as cod_nup
from	bi_corp_staging.triad_col_trdfldcu
where 	partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_TRIAD_KEYS-Daily') }}';


--TEMPORAL CON TODAS LAS KEY DE LOS CLIENTES DEL DIA (KEYS ENTRE 467 Y 600; SE SEPARAN POR TEMA DE PERFORMANCE)

DROP TABLE IF EXISTS temp_keys_nup_467_600;
CREATE TEMPORARY TABLE temp_keys_nup_467_600 AS
select 	cast(k.rkt_customer_id as bigint) as cod_nup,
        trim(k.rkt_account_id) as ds_accountid,
        cast(substring(trim(k.rkt_account_id),1,4)as int) as cod_sucursal,
        cast(substring(trim(k.rkt_account_id),5,12)as bigint) as  cod_cuenta,
		substring(trim(k.rkt_account_id),17,2) as cod_producto,
		case
		    when substring(trim(k.rkt_account_id),19,2)='AR' then 'ARS'
		    when substring(trim(k.rkt_account_id),19,2)='US' then 'USD'
		    else substring(trim(k.rkt_account_id),19,2) end as cod_divisa,
		cast(k467.kt_keys_value_n as int) as int_k467,
		cast(k468.kt_keys_value_n as int) as  int_k468,
		cast(k540.kt_keys_value_n as int) as  int_k540,
		cast(k542.kt_keys_value_n as int) as int_k542,
		cast(k544.kt_keys_value_n as int) as int_k544,
		cast(k552.kt_keys_value_n as int) as int_k552,
		cast(k555.kt_keys_value_n as int) as int_k555,
		cast(k590.kt_keys_value_n as int) as int_k590,
		cast(k593.kt_keys_value_n as int) as int_k593,
		cast(k596.kt_keys_value_n as int) as int_k596,
		cast(k597.kt_keys_value_n as int) as int_k597,
		cast(k598.kt_keys_value_n as int) as int_k598,
		cast(k599.kt_keys_value_n as int) as int_k599,
		cast(k600.kt_keys_value_n as int) as int_k600
from 	bi_corp_staging.triad_rr_trfrrkt k
lateral view inline (array(rkt_kt_keys_entries[466])) k467
lateral view inline (array(rkt_kt_keys_entries[467])) k468
lateral view inline (array(rkt_kt_keys_entries[539])) k540
lateral view inline (array(rkt_kt_keys_entries[541])) k542
lateral view inline (array(rkt_kt_keys_entries[543])) k544
lateral view inline (array(rkt_kt_keys_entries[551])) k552
lateral view inline (array(rkt_kt_keys_entries[554])) k555
lateral view inline (array(rkt_kt_keys_entries[589])) k590
lateral view inline (array(rkt_kt_keys_entries[592])) k593
lateral view inline (array(rkt_kt_keys_entries[595])) k596
lateral view inline (array(rkt_kt_keys_entries[596])) k597
lateral view inline (array(rkt_kt_keys_entries[597])) k598
lateral view inline (array(rkt_kt_keys_entries[598])) k599
lateral view inline (array(rkt_kt_keys_entries[599])) k600
where k.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_TRIAD_KEYS-Daily') }}'
;

--TEMPORAL CON TODAS LAS KEY DE LOS CLIENTES DEL DIA (KEYS ENTRE 601 Y 839; SE SEPARAN POR TEMA DE PERFORMANCE)

DROP TABLE IF EXISTS temp_keys_nup_601_839;
CREATE TEMPORARY TABLE temp_keys_nup_601_839 AS
select 	cast(k.rkt_customer_id as bigint) as cod_nup,
        trim(k.rkt_account_id) as ds_accountid,
        cast(substring(trim(k.rkt_account_id),1,4)as int) as cod_sucursal,
        cast(substring(trim(k.rkt_account_id),5,12)as bigint) as  cod_cuenta,
		substring(trim(k.rkt_account_id),17,2) as cod_producto,
		case
		    when substring(trim(k.rkt_account_id),19,2)='AR' then 'ARS'
		    when substring(trim(k.rkt_account_id),19,2)='US' then 'USD'
		    else substring(trim(k.rkt_account_id),19,2) end as cod_divisa,
		cast(k601.kt_keys_value_n as int) as int_k601,
		cast(k602.kt_keys_value_n as int) as int_k602,
		cast(k603.kt_keys_value_n as int) as int_k603,
		cast(k604.kt_keys_value_n as int) as int_k604,
		cast(k605.kt_keys_value_n as int) as int_k605,
		cast(k839.kt_keys_value_n as int) as int_k839
from 	bi_corp_staging.triad_rr_trfrrkt k
lateral view inline (array(rkt_kt_keys_entries[600])) k601
lateral view inline (array(rkt_kt_keys_entries[601])) k602
lateral view inline (array(rkt_kt_keys_entries[602])) k603
lateral view inline (array(rkt_kt_keys_entries[603])) k604
lateral view inline (array(rkt_kt_keys_entries[604])) k605
lateral view inline (array(rkt_kt_keys_entries[838])) k839
where k.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_TRIAD_KEYS-Daily') }}'
;


--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------VUELCO FINAL EN TABLA COMMON-------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------

INSERT OVERWRITE TABLE bi_corp_common.bt_rcp_triadkeys
PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_TRIAD_KEYS-Daily') }}')
SELECT  T1.cod_nup as cod_rcp_nup,
        T3.cod_cuenta as cod_rcp_cuenta,
        T3.cod_producto as cod_rcp_producto,
        T3.cod_divisa as cod_rcp_divisa,
        T3.cod_sucursal as cod_rcp_sucursal,
        T3.ds_accountid as ds_rcp_accountid,
        T2.ds_codigoestadocacs as ds_rcp_codigoestadocacs,
        T3.int_k467,
        T3.int_k468,
        T3.int_k540,
        T3.int_k542,
        T3.int_k544,
        T3.int_k552,
        T3.int_k555,
        T3.int_k590,
        T3.int_k593,
        T3.int_k596,
        T3.int_k597,
        T3.int_k598,
        T3.int_k599,
        T3.int_k600,
        T4.int_k601,
        T4.int_k602,
        T4.int_k603,
        T4.int_k604,
        T4.int_k605,
        T4.int_k839
FROM temp_perimetro_nups T1
LEFT JOIN temp_cuentas_cacs T2 ON T1.cod_nup=T2.cod_nup
LEFT JOIN temp_keys_nup_467_600 T3 ON T1.cod_nup=T3.cod_nup
LEFT JOIN temp_keys_nup_601_839 T4 ON T3.cod_nup=T4.cod_nup and T3.ds_accountid=T4.ds_accountid
;