set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------CREACION DE TABLAS TEMPORALES-------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------TEMPORAL CON EL PERIMETRO DE CLIENTES A CONSULTAR EN TRIAD DEL DIA

DROP TABLE IF EXISTS temp_perimetro_nups;
CREATE TEMPORARY TABLE temp_perimetro_nups AS
SELECT DISTINCT CAST(trim(dcu_customer_id) AS BIGINT) AS cod_nup,
		concat(substring(trim(cast(dcu_date_first_rel as string)),1,4),'-',substring(trim(cast(dcu_date_first_rel as string)),5,2),'-',substring(trim(cast(dcu_date_first_rel as string)),7,2)) as dt_firstrel,
		concat(substring(trim(cast(dcu_date_of_birth as string)),1,4),'-',substring(trim(cast(dcu_date_of_birth as string)),5,2),'-',substring(trim(cast(dcu_date_of_birth as string)),7,2)) as dt_datebirth,
		dcu_date_first_rel,
		dcu_date_of_birth
from	bi_corp_staging.triad_cfm_trdfldcu
where 	partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_TRIAD_LINKAGE-Monthly') }}';

----------------------------------------TEMPORAL DEL DIA CON INFORMACION DMG

DROP TABLE IF EXISTS temp_dmg_nups;
CREATE TEMPORARY TABLE temp_dmg_nups AS
select 	cast(dmg_customer_id as bigint) as cod_nup,
		TRIM(dmg_account_id) AS ds_accountid,
		cast(substring(trim(dmg_account_id),1,4)as int) as cod_sucursal,
        cast(substring(trim(dmg_account_id),5,12)as bigint) as  cod_cuenta,
		substring(trim(dmg_account_id),17,2) as cod_producto,
		case
		    when substring(trim(dmg_account_id),19,2)='AR' then 'ARS'
		    when substring(trim(dmg_account_id),19,2)='US' then 'USD'
		    else substring(trim(dmg_account_id),19,2) end as cod_divisa,
		case
			when dmg_date_closed!=0 then concat(substring(trim(cast(dmg_date_closed as string)),1,4),'-',substring(trim(cast(dmg_date_closed as string)),5,2),'-',substring(trim(cast(dmg_date_closed as string)),7,2))
			else cast(null as string) end as dt_closed,
		case
			when dmg_date_open!=0 then concat(substring(trim(cast(dmg_date_open as string)),1,4),'-',substring(trim(cast(dmg_date_open as string)),5,2),'-',substring(trim(cast(dmg_date_open as string)),7,2))
			else cast(null as string) end as dt_open,
		dmg_date_open,
		dmg_date_closed
from	bi_corp_staging.triad_cfm_trdfldmg
where 	partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_TRIAD_LINKAGE-Monthly') }}';


----------------------------------------TEMPORAL DEL DIA CON INFORMACION DE AVG BALANCES

DROP TABLE IF EXISTS temp_nups_avg_balances;
CREATE TEMPORARY TABLE temp_nups_avg_balances AS
select 	cast(ca.dca_customer_id as bigint) as cod_nup,
		trim(ca.dca_account_id) as ds_accountid,
		cast(substring(trim(ca.dca_account_id),1,4)as int) as cod_sucursal,
        cast(substring(trim(ca.dca_account_id),5,12)as bigint) as  cod_cuenta,
		substring(trim(ca.dca_account_id),17,2) as cod_producto,
		case
		    when substring(trim(ca.dca_account_id),19,2)='AR' then 'ARS'
		    when substring(trim(ca.dca_account_id),19,2)='US' then 'USD'
		    else substring(trim(ca.dca_account_id),19,2) end as cod_divisa,
		case
			when ca.dca_date_open!=0 then concat(substring(trim(cast(ca.dca_date_open as string)),1,4),'-',substring(trim(cast(ca.dca_date_open as string)),5,2),'-',substring(trim(cast(ca.dca_date_open as string)),7,2))
			else cast(null as string) end as dt_dcaopen,
		case
			when ca.dca_date_closed!=0 then concat(substring(trim(cast(ca.dca_date_closed as string)),1,4),'-',substring(trim(cast(ca.dca_date_closed as string)),5,2),'-',substring(trim(cast(ca.dca_date_closed as string)),7,2))
			else cast(null as string) end as dt_dcaclosed,
		cast(m1.avg_balance as decimal(22,6)) as  avg_balance_1,
		cast(m2.avg_balance as decimal(22,6)) as avg_balance_2,
		cast(m3.avg_balance as decimal(22,6)) as avg_balance_3,
		cast(m4.avg_balance as decimal(22,6)) as avg_balance_4,
		cast(m5.avg_balance as decimal(22,6)) as avg_balance_5,
		cast(m6.avg_balance as decimal(22,6)) as avg_balance_6,
		cast(m7.avg_balance as decimal(22,6)) as avg_balance_7,
		cast(m8.avg_balance as decimal(22,6)) as avg_balance_8,
		cast(m9.avg_balance as decimal(22,6)) as avg_balance_9,
		cast(m10.avg_balance as decimal(22,6)) as avg_balance_10,
		cast(m11.avg_balance as decimal(22,6)) as avg_balance_11,
		cast(m12.avg_balance as decimal(22,6)) as avg_balance_12,
		cast(m1.val_cust_credits as decimal(22,6)) as val_cust_credits_1,
		cast(m2.val_cust_credits as decimal(22,6)) as val_cust_credits_2,
		cast(m3.val_cust_credits as decimal(22,6)) as val_cust_credits_3
from	bi_corp_staging.triad_cfm_trdfldca ca
lateral view inline (array(dca_monthly_segment[0])) m1
lateral view inline (array(dca_monthly_segment[1])) m2
lateral view inline (array(dca_monthly_segment[2])) m3
lateral view inline (array(dca_monthly_segment[3])) m4
lateral view inline (array(dca_monthly_segment[4])) m5
lateral view inline (array(dca_monthly_segment[5])) m6
lateral view inline (array(dca_monthly_segment[6])) m7
lateral view inline (array(dca_monthly_segment[7])) m8
lateral view inline (array(dca_monthly_segment[8])) m9
lateral view inline (array(dca_monthly_segment[9])) m10
lateral view inline (array(dca_monthly_segment[10])) m11
lateral view inline (array(dca_monthly_segment[11])) m12
where ca.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_TRIAD_LINKAGE-Monthly') }}';

----------------------------------------TEMPORAL DEL DIA CON INFORMACION DE BALANCES

DROP TABLE IF EXISTS temp_nups_balances;
CREATE TEMPORARY TABLE temp_nups_balances AS
select 	cast(ln.dln_customer_id as bigint) as cod_nup,
		trim(ln.dln_account_id) as ds_accountid,
		cast(substring(trim(ln.dln_account_id),1,4)as int) as cod_sucursal,
        cast(substring(trim(ln.dln_account_id),5,12)as bigint) as  cod_cuenta,
		substring(trim(ln.dln_account_id),17,2) as cod_producto,
		case
		    when substring(trim(ln.dln_account_id),19,2)='AR' then 'ARS'
		    when substring(trim(ln.dln_account_id),19,2)='US' then 'USD'
		    else substring(trim(ln.dln_account_id),19,2) end as cod_divisa,
		case
			when ln.dln_date_open!=0 then concat(substring(trim(cast(ln.dln_date_open as string)),1,4),'-',substring(trim(cast(ln.dln_date_open as string)),5,2),'-',substring(trim(cast(ln.dln_date_open as string)),7,2))
			else null end as dt_dlnopen,
		case
			when ln.dln_date_closed!=0 then concat(substring(trim(cast(ln.dln_date_closed as string)),1,4),'-',substring(trim(cast(ln.dln_date_closed as string)),5,2),'-',substring(trim(cast(ln.dln_date_closed as string)),7,2))
			else null end as dt_dlnclosed,
		cast(m1.balance as decimal(22,6)) as balance_1,
		cast(m2.balance as decimal(22,6)) as balance_2,
		cast(m3.balance as decimal(22,6)) as balance_3,
		cast(m4.balance as decimal(22,6)) as balance_4,
		cast(m5.balance as decimal(22,6)) as balance_5,
		cast(m6.balance as decimal(22,6)) as balance_6,
		cast(m7.balance as decimal(22,6)) as balance_7,
		cast(m8.balance as decimal(22,6)) as balance_8,
		cast(m9.balance as decimal(22,6)) as balance_9,
		cast(m10.balance as decimal(22,6)) as balance_10,
		cast(m11.balance as decimal(22,6)) as balance_11,
		cast(m12.balance as decimal(22,6)) as balance_12
from	bi_corp_staging.triad_cfm_trdfldln ln
lateral view inline (array(dln_monthly_segment[0])) m1
lateral view inline (array(dln_monthly_segment[1])) m2
lateral view inline (array(dln_monthly_segment[2])) m3
lateral view inline (array(dln_monthly_segment[3])) m4
lateral view inline (array(dln_monthly_segment[4])) m5
lateral view inline (array(dln_monthly_segment[5])) m6
lateral view inline (array(dln_monthly_segment[6])) m7
lateral view inline (array(dln_monthly_segment[7])) m8
lateral view inline (array(dln_monthly_segment[8])) m9
lateral view inline (array(dln_monthly_segment[9])) m10
lateral view inline (array(dln_monthly_segment[10])) m11
lateral view inline (array(dln_monthly_segment[11])) m12
where ln.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_TRIAD_LINKAGE-Monthly') }}';

----------------------------------------TEMPORAL DEL DIA CON INFORMACION DE BALANCES CYCLES M

DROP TABLE IF EXISTS temp_cyclesbalances;
CREATE TEMPORARY TABLE temp_cyclesbalances AS
select 	cast(drv_customer_id as bigint) as cod_nup,
		trim(drv_account_id) as ds_accountid,
		cast(substring(trim(drv_account_id),1,4)as int) as cod_sucursal,
        cast(substring(trim(drv_account_id),5,12)as bigint) as  cod_cuenta,
		substring(trim(drv_account_id),17,2) as cod_producto,
		case
		    when substring(trim(drv_account_id),19,2)='AR' then 'ARS'
		    when substring(trim(drv_account_id),19,2)='US' then 'USD'
		    else substring(trim(drv_account_id),19,2) end as cod_divisa,
		case
			when drv_date_open!=0 then concat(substring(trim(cast(drv_date_open as string)),1,4),'-',substring(trim(cast(drv_date_open as string)),5,2),'-',substring(trim(cast(drv_date_open as string)),7,2))
			else cast(null as string) end as dt_drvopen,
		case
			when drv_date_closed!=0 then concat(substring(trim(cast(drv_date_closed as string)),1,4),'-',substring(trim(cast(drv_date_closed as string)),5,2),'-',substring(trim(cast(drv_date_closed as string)),7,2))
			else cast(null as string) end as dt_drvclosed,
		cast(m1.cycle_balance as decimal(22,6)) as cycle_balance_1,
		cast(m1.val_payments as decimal(22,6)) as val_payments_1,
		cast(m1.limit as decimal(22,6)) as limit_1,
		cast(m1.val_cash_adv as decimal(22,6)) as val_cash_adv_1,
	    cast(m1.amount_due as decimal(22,6)) as amount_due_1,
		cast(m2.cycle_balance as decimal(22,6)) as cycle_balance_2,
		cast(m2.amount_due as decimal(22,6)) as amount_due_2,
		cast(m2.val_payments as decimal(22,6)) as val_payments_2,
		cast(m2.limit as decimal(22,6)) as limit_2,
		cast(m3.cycle_balance as decimal(22,6)) as cycle_balance_3,
		cast(m3.val_payments as decimal(22,6)) as val_payments_3,
		cast(m3.amount_due as decimal(22,6)) as amount_due_3,
		cast(m3.limit as decimal(22,6)) as limit_3,
		cast(m4.cycle_balance as decimal(22,6)) as cycle_balance_4,
		cast(m4.val_payments as decimal(22,6)) as val_payments_4,
		cast(m4.amount_due as decimal(22,6)) as amount_due_4,
		cast(m5.cycle_balance as decimal(22,6)) as cycle_balance_5,
		cast(m5.val_payments as decimal(22,6)) as val_payments_5,
		cast(m5.amount_due as decimal(22,6)) as amount_due_5,
		cast(m6.cycle_balance as decimal(22,6)) as cycle_balance_6,
		cast(m6.val_payments as decimal(22,6)) as val_payments_6,
		cast(m6.amount_due as decimal(22,6)) as amount_due_6,
		cast(m7.cycle_balance as decimal(22,6)) as cycle_balance_7,
		cast(m7.val_payments as decimal(22,6)) as val_payments_7,
		cast(m7.amount_due as decimal(22,6)) as amount_due_7,
		cast(m8.cycle_balance as decimal(22,6)) as cycle_balance_8,
		cast(m8.val_payments as decimal(22,6)) as val_payments_8,
		cast(m8.amount_due as decimal(22,6)) as amount_due_8,
		cast(m9.cycle_balance as decimal(22,6)) as cycle_balance_9,
		cast(m9.val_payments as decimal(22,6)) as val_payments_9,
		cast(m9.amount_due as decimal(22,6)) as amount_due_9,
		cast(m10.cycle_balance as decimal(22,6)) as cycle_balance_10,
		cast(m10.val_payments as decimal(22,6)) as val_payments_10,
		cast(m10.amount_due as decimal(22,6)) as amount_due_10,
		cast(m11.cycle_balance as decimal(22,6)) as cycle_balance_11,
		cast(m11.val_payments as decimal(22,6)) as val_payments_11,
		cast(m11.amount_due as decimal(22,6)) as amount_due_11,
		cast(m12.val_payments as decimal(22,6)) as val_payments_12,
		cast(m12.cycle_balance as decimal(22,6)) as cycle_balance_12,
		cast(m12.amount_due as decimal(22,6)) as amount_due_12
from	bi_corp_staging.triad_cfm_trdfldrv
lateral view inline (array(drv_monthly_segment[0])) m1
lateral view inline (array(drv_monthly_segment[1])) m2
lateral view inline (array(drv_monthly_segment[2])) m3
lateral view inline (array(drv_monthly_segment[3])) m4
lateral view inline (array(drv_monthly_segment[4])) m5
lateral view inline (array(drv_monthly_segment[5])) m6
lateral view inline (array(drv_monthly_segment[6])) m7
lateral view inline (array(drv_monthly_segment[7])) m8
lateral view inline (array(drv_monthly_segment[8])) m9
lateral view inline (array(drv_monthly_segment[9])) m10
lateral view inline (array(drv_monthly_segment[10])) m11
lateral view inline (array(drv_monthly_segment[11])) m12
where partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_TRIAD_LINKAGE-Monthly') }}';


----------------------------------------TEMPORAL DEL DIA CON INFORMACION DE DEL CYCLE DELQ

DROP TABLE IF EXISTS temp_cyclesdelq;
CREATE TEMPORARY TABLE temp_cyclesdelq AS
select 	cast(drv_customer_id as bigint) as cod_nup,
		trim(drv_account_id) as ds_accountid,
		cast(substring(trim(drv_account_id),1,4)as int) as cod_sucursal,
        cast(substring(trim(drv_account_id),5,12)as bigint) as  cod_cuenta,
		substring(trim(drv_account_id),17,2) as cod_producto,
		case
		    when substring(trim(drv_account_id),19,2)='AR' then 'ARS'
		    when substring(trim(drv_account_id),19,2)='US' then 'USD'
		    else substring(trim(drv_account_id),19,2) end as cod_divisa,
		case
			when drv_date_open!=0 then concat(substring(trim(cast(drv_date_open as string)),1,4),'-',substring(trim(cast(drv_date_open as string)),5,2),'-',substring(trim(cast(drv_date_open as string)),7,2))
			else cast(null as string) end as dt_drvopen,
		case
			when drv_date_closed!=0 then concat(substring(trim(cast(drv_date_closed as string)),1,4),'-',substring(trim(cast(drv_date_closed as string)),5,2),'-',substring(trim(cast(drv_date_closed as string)),7,2))
			else cast(null as string) end as dt_drvclosed,
		cast(c1.cycles_delq as decimal(22,6)) as cycles_delq_1,
		cast(c2.cycles_delq as decimal(22,6)) as cycles_delq_2,
		cast(c3.cycles_delq as decimal(22,6)) as cycles_delq_3,
		cast(c4.cycles_delq as decimal(22,6)) as cycles_delq_4,
		cast(c5.cycles_delq as decimal(22,6)) as cycles_delq_5,
		cast(c6.cycles_delq as decimal(22,6)) as cycles_delq_6,
		cast(c7.cycles_delq as decimal(22,6)) as cycles_delq_7,
		cast(c8.cycles_delq as decimal(22,6)) as cycles_delq_8,
		cast(c9.cycles_delq as decimal(22,6)) as cycles_delq_9,
		cast(c10.cycles_delq as decimal(22,6)) as cycles_delq_10,
		cast(c11.cycles_delq as decimal(22,6)) as cycles_delq_11,
		cast(c12.cycles_delq as decimal(22,6)) as cycles_delq_12
from	bi_corp_staging.triad_cfm_trdfldrv
lateral view inline (array(drv_monthly_segment2[0])) c1
lateral view inline (array(drv_monthly_segment2[1])) c2
lateral view inline (array(drv_monthly_segment2[2])) c3
lateral view inline (array(drv_monthly_segment2[3])) c4
lateral view inline (array(drv_monthly_segment2[4])) c5
lateral view inline (array(drv_monthly_segment2[5])) c6
lateral view inline (array(drv_monthly_segment2[6])) c7
lateral view inline (array(drv_monthly_segment2[7])) c8
lateral view inline (array(drv_monthly_segment2[8])) c9
lateral view inline (array(drv_monthly_segment2[9])) c10
lateral view inline (array(drv_monthly_segment2[10])) c11
lateral view inline (array(drv_monthly_segment2[11])) c12
where partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_TRIAD_LINKAGE-Monthly') }}';


-----------------------------------------------CREO TEMPORAL CON LA INFORMACION DE CYCLE BALANCE Y CYCLE DELQ JUNTAS
DROP TABLE IF EXISTS temp_nups_cyclesbalances;
CREATE TEMPORARY TABLE temp_nups_cyclesbalances AS
SELECT 	T1.cod_nup,
		T1.ds_accountid,
		T1.cod_sucursal,
        T1.cod_cuenta,
		T1.cod_producto,
		T1.cod_divisa,
		T1.dt_drvopen,
		T1.dt_drvclosed,
		T1.cycle_balance_1,
		T1.val_payments_1,
		T1.limit_1,
		T1.val_cash_adv_1,
	    T1.amount_due_1,
		T1.cycle_balance_2,
		T1.amount_due_2,
		T1.val_payments_2,
		T1.limit_2,
		T1.cycle_balance_3,
		T1.val_payments_3,
		T1.amount_due_3,
		T1.limit_3,
		T1.cycle_balance_4,
		T1.val_payments_4,
		T1.amount_due_4,
		T1.cycle_balance_5,
		T1.val_payments_5,
		T1.amount_due_5,
		T1.cycle_balance_6,
		T1.val_payments_6,
		T1.amount_due_6,
		T1.cycle_balance_7,
		T1.val_payments_7,
		T1.amount_due_7,
		T1.cycle_balance_8,
		T1.val_payments_8,
		T1.amount_due_8,
		T1.cycle_balance_9,
		T1.val_payments_9,
		T1.amount_due_9,
		T1.cycle_balance_10,
		T1.val_payments_10,
		T1.amount_due_10,
		T1.cycle_balance_11,
		T1.val_payments_11,
		T1.amount_due_11,
		T1.val_payments_12,
		T1.cycle_balance_12,
		T1.amount_due_12,
		T2.cycles_delq_1,
		T2.cycles_delq_2,
		T2.cycles_delq_3,
		T2.cycles_delq_4,
		T2.cycles_delq_5,
		T2.cycles_delq_6,
		T2.cycles_delq_7,
		T2.cycles_delq_8,
		T2.cycles_delq_9,
		T2.cycles_delq_10,
		T2.cycles_delq_11,
		T2.cycles_delq_12
FROM temp_cyclesbalances T1
LEFT JOIN temp_cyclesdelq T2
ON T1.cod_nup=T2.cod_nup and T1.ds_accountid=T2.ds_accountid;


----------------------------------------TEMPORAL DEL DIA CON EL MAESTRO DE NUP Y ACCOUNTS ID

DROP TABLE IF EXISTS temp_nups_accountid_master;
CREATE TEMPORARY TABLE temp_nups_accountid_master AS
SELECT DISTINCT cod_nup,
                ds_accountid,
		cast(substring(trim(ds_accountid),1,4)as int) as cod_sucursal,
        	cast(substring(trim(ds_accountid),5,12)as bigint) as  cod_cuenta,
		substring(trim(ds_accountid),17,2) as cod_producto,
		case
			when substring(trim(ds_accountid),19,2)='AR' then 'ARS'
			when substring(trim(ds_accountid),19,2)='US' then 'USD'
		else substring(trim(ds_accountid),19,2) end as cod_divisa,
                dt_firstrel,
                dt_datebirth
FROM
  (SELECT T1.cod_nup,
          T1.dt_firstrel,
          T1.dt_datebirth,
          T2.ds_accountid
   FROM temp_perimetro_nups T1
   LEFT JOIN temp_dmg_nups T2 ON T1.cod_nup=T2.cod_nup
   UNION ALL
   SELECT T3.cod_nup,
                    T3.dt_firstrel,
                    T3.dt_datebirth,
                    T4.ds_accountid
   FROM temp_perimetro_nups T3
   LEFT JOIN temp_nups_avg_balances T4 ON T3.cod_nup=T4.cod_nup
   UNION ALL
   SELECT T5.cod_nup,
                    T5.dt_firstrel,
                    T5.dt_datebirth,
                    T6.ds_accountid
   FROM temp_perimetro_nups T5
   LEFT JOIN temp_nups_balances T6 ON T5.cod_nup=T6.cod_nup
   UNION ALL
   SELECT T7.cod_nup,
                    T7.dt_firstrel,
                    T7.dt_datebirth,
                    T8.ds_accountid
   FROM temp_perimetro_nups T7
   LEFT JOIN temp_nups_cyclesbalances T8 ON T7.cod_nup=T8.cod_nup) TABLE
WHERE ds_accountid IS NOT NULL;

--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------VUELCO FINAL EN TABLA COMMON-------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------

INSERT OVERWRITE TABLE bi_corp_common.bt_rcp_triadlinkage
PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_TRIAD_LINKAGE-Monthly') }}')
SELECT T1.cod_nup as cod_rcp_nup,
T1.cod_sucursal as cod_rcp_sucursal,
T1.cod_cuenta as cod_rcp_cuenta,
T1.cod_producto as cod_rcp_producto,
T1.cod_divisa as cod_rcp_divisa,
T1.ds_accountid as ds_rcp_accountid,
T1.dt_firstrel as dt_rcp_firstrel,
T2.dt_closed as dt_rcp_dmgclosed,
T2.dt_open as dt_rcp_dmgopen,
T5.dt_drvopen as drvopen,
T1.dt_datebirth as dt_rcp_birth,
T3.dt_dcaopen,
T3.dt_dcaclosed,
T5.dt_drvclosed,
T4.dt_dlnopen,
T4.dt_dlnclosed,
T4.balance_1 as fc_rcp_balance1,
T4.balance_2 as fc_rcp_balance2,
T4.balance_3 as fc_rcp_balance3,
T4.balance_4 as fc_rcp_balance4,
T4.balance_5 as fc_rcp_balance5,
T4.balance_6 as fc_rcp_balance6,
T4.balance_7 as fc_rcp_balance7,
T4.balance_8 as fc_rcp_balance8,
T4.balance_9 as fc_rcp_balance9,
T4.balance_10 as fc_rcp_balance10,
T4.balance_11 as fc_rcp_balance11,
T4.balance_12 as fc_rcp_balance12,
T3.avg_balance_1 as fc_rcp_avgbalance1,
T3.avg_balance_2 as fc_rcp_avgbalance2,
T3.avg_balance_3 as fc_rcp_avgbalance3,
T3.avg_balance_4 as fc_rcp_avgbalance4,
T3.avg_balance_5 as fc_rcp_avgbalance5,
T3.avg_balance_6 as fc_rcp_avgbalance6,
T3.avg_balance_7 as fc_rcp_avgbalance7,
T3.avg_balance_8 as fc_rcp_avgbalance8,
T3.avg_balance_9 as fc_rcp_avgbalance9,
T3.avg_balance_10 as fc_rcp_avgbalance10,
T3.avg_balance_11 as fc_rcp_avgbalance11,
T3.avg_balance_12 as fc_rcp_avgbalance12,
T3.val_cust_credits_1 as fc_rcp_valcustcredits1,
T3.val_cust_credits_2 as fc_rcp_valcustcredits2,
T3.val_cust_credits_3 as fc_rcp_valcustcredits3,
T5.cycle_balance_1 as fc_rcp_cyclebalance1,
T5.cycle_balance_2 as fc_rcp_cyclebalance2,
T5.cycle_balance_3 as fc_rcp_cyclebalance3,
T5.cycle_balance_4 as fc_rcp_cyclebalance4,
T5.cycle_balance_5 as fc_rcp_cyclebalance5,
T5.cycle_balance_6 as fc_rcp_cyclebalance6,
T5.cycle_balance_7 as fc_rcp_cyclebalance7,
T5.cycle_balance_8 as fc_rcp_cyclebalance8,
T5.cycle_balance_9 as fc_rcp_cyclebalance9,
T5.cycle_balance_10 as fc_rcp_cyclebalance10,
T5.cycle_balance_11 as fc_rcp_cyclebalance11,
T5.cycle_balance_12 as fc_rcp_cyclebalance12,
T5.cycles_delq_1 as fc_rcp_cycledelq1,
T5.cycles_delq_2 as fc_rcp_cycledelq2,
T5.cycles_delq_3 as fc_rcp_cycledelq3,
T5.cycles_delq_4 as fc_rcp_cycledelq4,
T5.cycles_delq_5 as fc_rcp_cycledelq5,
T5.cycles_delq_6 as fc_rcp_cycledelq6,
T5.cycles_delq_7 as fc_rcp_cycledelq7,
T5.cycles_delq_8 as fc_rcp_cycledelq8,
T5.cycles_delq_9 as fc_rcp_cycledelq9,
T5.cycles_delq_10 as fc_rcp_cycledelq10,
T5.cycles_delq_11 as fc_rcp_cycledelq11,
T5.cycles_delq_12 as fc_rcp_cycledelq12,
T5.limit_1 as fc_rcp_limit1,
T5.limit_2 as fc_rcp_limit2,
T5.limit_3 as fc_rcp_limit3,
T5.val_payments_1 as fc_rcp_valpayments1,
T5.val_payments_2 as fc_rcp_valpayments2,
T5.val_payments_3 as fc_rcp_valpayments3,
T5.val_payments_4 as fc_rcp_valpayments4,
T5.val_payments_5 as fc_rcp_valpayments5,
T5.val_payments_6 as fc_rcp_valpayments6,
T5.val_payments_7 as fc_rcp_valpayments7,
T5.val_payments_8 as fc_rcp_valpayments8,
T5.val_payments_9 as fc_rcp_valpayments9,
T5.val_payments_10 as fc_rcp_valpayments10,
T5.val_payments_11 as fc_rcp_valpayments11,
T5.val_payments_12 as fc_rcp_valpayments12,
T5.amount_due_1 as fc_rcp_amountdue1,
T5.amount_due_2 as fc_rcp_amountdue2,
T5.amount_due_3 as fc_rcp_amountdue3,
T5.amount_due_4 as fc_rcp_amountdue4,
T5.amount_due_5 as fc_rcp_amountdue5,
T5.amount_due_6 as fc_rcp_amountdue6,
T5.amount_due_7 as fc_rcp_amountdue7,
T5.amount_due_8 as fc_rcp_amountdue8,
T5.amount_due_9 as fc_rcp_amountdue9,
T5.amount_due_10 as fc_rcp_amountdue10,
T5.amount_due_11 as fc_rcp_amountdue11,
T5.amount_due_12 as fc_rcp_amountdue12,
T5.val_cash_adv_1 as fc_rcp_valcashadv1
FROM temp_nups_accountid_master T1
LEFT JOIN temp_dmg_nups T2 ON T1.cod_nup=T2.cod_nup and T1.ds_accountid=T2.ds_accountid
LEFT JOIN temp_nups_avg_balances  T3 ON T1.cod_nup=T3.cod_nup and T1.ds_accountid=T3.ds_accountid
LEFT JOIN temp_nups_balances T4 ON T1.cod_nup=T4.cod_nup and T1.ds_accountid=T4.ds_accountid
LEFT JOIN temp_nups_cyclesbalances T5 ON T1.cod_nup=T5.cod_nup and T1.ds_accountid=T5.ds_accountid;