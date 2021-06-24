SET hive.merge.size.per.task=4000000;
SET hive.merge.smallfiles.avgsize=16000000;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

create temporary table analytics.comex_temp as

  Select
  ds_coe_segmento,
  ds_coe_subsegmento,
  cod_coe_sucursal,
  ds_coe_canal,
  count( distinct concat( cod_coe_operacion, ds_coe_secuencia)) as fc_kpi_operaciones,
  count (distinct cod_per_nup) as fc_kpi_clientes

  from bi_corp_common.stk_coe_operaciones coe

  Where partition_Date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_Comex-Daily') }}'
  and substring (dt_coe_fecha_operacion,0,10) = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_Comex-Daily') }}'
  and ds_coe_estado_operacion = 'Liquidadas'
  group by
  ds_coe_segmento,
  ds_coe_subsegmento,
  cod_coe_sucursal,
  ds_coe_canal

;


INSERT  overwrite TABLE bi_corp_business.tbl_comex PARTITION (cod_kpi_kpi, partition_date)

SELECT

       comex.ds_coe_segmento                                                    AS cod_kpi_segmento,
       comex.ds_coe_subsegmento                                                 AS cod_kpi_subsegmento,
       NULL                                                     				AS ds_kpi_producto,
       NULL                                                  					AS ds_kpi_subproducto,
       comex.ds_coe_canal                                                       AS cod_kpi_canal,
       NULL                                                 					AS ds_kpi_destino,
       NULL                                                                     AS cod_kpi_zona,
       comex.cod_coe_sucursal                                                   AS cod_kpi_sucursal,
       NULL                                                       				AS cod_kpi_divisa,
       NULL                                                		                AS fc_kpi_metrica,
       NULL                                                                     AS fc_kpi_metricamismodiamesanterior,
       NULL                                                                     AS fc_kpi_metricadiaanterior,
       NULL                                                                     AS fc_kpi_metricafindemesanterior,
       NULL                                                                     AS fc_kpi_metricamismodiaanoanterior,
       NULL                                                                     AS fc_kpi_metricafindeanoanterior,
       NULL                                                                     AS fc_kpi_metricasemanaanterior,
       comex.fc_kpi_clientes                                                    AS fc_kpi_clientes,
       mismodiamesanterior.fc_kpi_clientesmismodiamesanterior                   AS fc_kpi_clientesmismodiamesanterior,
       tmenos1.fc_kpi_clientesdiaanterior                                       AS fc_kpi_clientesdiaanterior,
       findemesanterior.fc_kpi_clientesfindemesanterior                         AS fc_kpi_clientesfindemesanterior,
       mismodiaanoanterior.fc_kpi_clientesmismodiaanoanterior                   AS fc_kpi_clientesmismodiaanoanterior,
       findeanoanterior.fc_kpi_clientesfindeanoanterior                         AS fc_kpi_clientesfindeanoanterior,
       semanaanterior.fc_kpi_clientessemanaanterior                             AS fc_kpi_clientessemanaanterior,
       comex.fc_kpi_operaciones                                                 AS fc_kpi_operaciones,
       mismodiamesanterior.fc_kpi_operacionesmismodiamesanterior                AS fc_kpi_operacionesmismodiamesanterior,
       tmenos1.fc_kpi_operacionesdiaanterior                                    AS fc_kpi_operacionesdiaanterior,
       findemesanterior.fc_kpi_operacionesfindemesanterior                      AS fc_kpi_operacionesfindemesanterior,
       mismodiaanoanterior.fc_kpi_operacionesmismodiaanoanterior                AS fc_kpi_operacionesmismodiaanoanterior,
       findeanoanterior.fc_kpi_operacionesfindeanoanterior                      AS fc_kpi_operacionesfindeanoanterior,
       semanaanterior.fc_kpi_operacionessemanaanterior                          AS fc_kpi_operacionessemanaanterior,
       '000006'                                                              	  AS cod_kpi_kpi,
       '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_Comex-Daily') }}'   as partition_date


FROM
(

Select
CASE WHEN ds_coe_segmento    	IS NULL THEN  'sin dato' ELSE ds_coe_segmento END as ds_coe_segmento,
CASE WHEN ds_coe_subsegmento    IS NULL THEN  'sin dato' ELSE ds_coe_subsegmento END as ds_coe_subsegmento,
CASE WHEN cod_coe_sucursal    	IS NULL THEN  'sin dato' ELSE cod_coe_sucursal END as cod_coe_sucursal,
CASE WHEN ds_coe_canal    		IS NULL THEN  'sin dato' ELSE ds_coe_canal END as ds_coe_canal,
fc_kpi_operaciones,
fc_kpi_clientes
from analytics.comex_temp
) comex

FULL OUTER JOIN
(
SELECT
CASE WHEN cod_kpi_segmento    	IS NULL THEN  'sin dato' ELSE cod_kpi_segmento 		END as cod_kpi_segmento,
CASE WHEN cod_kpi_subsegmento   IS NULL THEN  'sin dato' ELSE cod_kpi_subsegmento 	END as cod_kpi_subsegmento,
CASE WHEN cod_kpi_sucursal    	IS NULL THEN  'sin dato' ELSE cod_kpi_sucursal 		END as cod_kpi_sucursal,
CASE WHEN cod_kpi_canal    		IS NULL THEN  'sin dato' ELSE cod_kpi_canal 		END as cod_kpi_canal,
fc_kpi_operaciones        as fc_kpi_operacionesmismodiamesanterior,
fc_kpi_clientes           as fc_kpi_clientesmismodiamesanterior
FROM bi_corp_business.tbl_comex
Where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_working_day', dag_id='LOAD_BSN_Comex-Daily') }}'
) mismodiamesanterior
  ON comex.ds_coe_segmento      = mismodiamesanterior.cod_kpi_segmento
  and comex.ds_coe_subsegmento  = mismodiamesanterior.cod_kpi_subsegmento
  and comex.cod_coe_sucursal    = mismodiamesanterior.cod_kpi_sucursal
  and comex.ds_coe_canal        = mismodiamesanterior.cod_kpi_canal


  FULL OUTER JOIN
(
SELECT

CASE WHEN cod_kpi_segmento    	IS NULL THEN  'sin dato' ELSE cod_kpi_segmento 		END as cod_kpi_segmento,
CASE WHEN cod_kpi_subsegmento   IS NULL THEN  'sin dato' ELSE cod_kpi_subsegmento 	END as cod_kpi_subsegmento,
CASE WHEN cod_kpi_sucursal    	IS NULL THEN  'sin dato' ELSE cod_kpi_sucursal 		END as cod_kpi_sucursal,
CASE WHEN cod_kpi_canal    		IS NULL THEN  'sin dato' ELSE cod_kpi_canal 		END as cod_kpi_canal,
fc_kpi_operaciones      as fc_kpi_operacionesdiaanterior,
fc_kpi_clientes         as fc_kpi_clientesdiaanterior
FROM bi_corp_business.tbl_comex
Where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_working_day', dag_id='LOAD_BSN_Comex-Daily') }}'
) tmenos1
ON comex.ds_coe_segmento      = tmenos1.cod_kpi_segmento
and comex.ds_coe_subsegmento  = tmenos1.cod_kpi_subsegmento
and comex.cod_coe_sucursal    = tmenos1.cod_kpi_sucursal
and comex.ds_coe_canal        = tmenos1.cod_kpi_canal

  FULL OUTER JOIN
(
SELECT
CASE WHEN cod_kpi_segmento    	IS NULL THEN  'sin dato' ELSE cod_kpi_segmento 		END as cod_kpi_segmento,
CASE WHEN cod_kpi_subsegmento   IS NULL THEN  'sin dato' ELSE cod_kpi_subsegmento 	END as cod_kpi_subsegmento,
CASE WHEN cod_kpi_sucursal    	IS NULL THEN  'sin dato' ELSE cod_kpi_sucursal 		END as cod_kpi_sucursal,
CASE WHEN cod_kpi_canal    		IS NULL THEN  'sin dato' ELSE cod_kpi_canal 		END as cod_kpi_canal,
fc_kpi_operaciones      as fc_kpi_operacionesfindemesanterior,
fc_kpi_clientes         as fc_kpi_clientesfindemesanterior
FROM bi_corp_business.tbl_comex

Where  partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_last_working_day', dag_id='LOAD_BSN_Comex-Daily') }}'
) findemesanterior
ON comex.ds_coe_segmento      = findemesanterior.cod_kpi_segmento
and comex.ds_coe_subsegmento  = findemesanterior.cod_kpi_subsegmento
and comex.cod_coe_sucursal    = findemesanterior.cod_kpi_sucursal
and comex.ds_coe_canal        = findemesanterior.cod_kpi_canal

	  FULL OUTER JOIN
(
SELECT
CASE WHEN cod_kpi_segmento    	IS NULL THEN  'sin dato' ELSE cod_kpi_segmento 		END as cod_kpi_segmento,
CASE WHEN cod_kpi_subsegmento   IS NULL THEN  'sin dato' ELSE cod_kpi_subsegmento 	END as cod_kpi_subsegmento,
CASE WHEN cod_kpi_sucursal    	IS NULL THEN  'sin dato' ELSE cod_kpi_sucursal 		END as cod_kpi_sucursal,
CASE WHEN cod_kpi_canal    		IS NULL THEN  'sin dato' ELSE cod_kpi_canal 		END as cod_kpi_canal,
fc_kpi_operaciones      as fc_kpi_operacionesmismodiaanoanterior,
fc_kpi_clientes         as fc_kpi_clientesmismodiaanoanterior
FROM bi_corp_business.tbl_comex
Where  partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_year_working_day', dag_id='LOAD_BSN_Comex-Daily') }}'
) mismodiaanoanterior
ON comex.ds_coe_segmento      = mismodiaanoanterior.cod_kpi_segmento
and comex.ds_coe_subsegmento  = mismodiaanoanterior.cod_kpi_subsegmento
and comex.cod_coe_sucursal    = mismodiaanoanterior.cod_kpi_sucursal
and comex.ds_coe_canal        = mismodiaanoanterior.cod_kpi_canal

		  FULL OUTER JOIN
(
SELECT
CASE WHEN cod_kpi_segmento    	IS NULL THEN  'sin dato' ELSE cod_kpi_segmento 		END as cod_kpi_segmento,
CASE WHEN cod_kpi_subsegmento   IS NULL THEN  'sin dato' ELSE cod_kpi_subsegmento 	END as cod_kpi_subsegmento,
CASE WHEN cod_kpi_sucursal    	IS NULL THEN  'sin dato' ELSE cod_kpi_sucursal 		END as cod_kpi_sucursal,
CASE WHEN cod_kpi_canal    		IS NULL THEN  'sin dato' ELSE cod_kpi_canal 		END as cod_kpi_canal,
fc_kpi_operaciones      as fc_kpi_operacionesfindeanoanterior,
fc_kpi_clientes         as fc_kpi_clientesfindeanoanterior
FROM bi_corp_business.tbl_comex
Where  partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_year_last_working_day', dag_id='LOAD_BSN_Comex-Daily') }}'
) findeanoanterior
ON comex.ds_coe_segmento      = findeanoanterior.cod_kpi_segmento
and comex.ds_coe_subsegmento  = findeanoanterior.cod_kpi_subsegmento
and comex.cod_coe_sucursal    = findeanoanterior.cod_kpi_sucursal
and comex.ds_coe_canal        = findeanoanterior.cod_kpi_canal

		 FULL OUTER JOIN
(
SELECT
CASE WHEN cod_kpi_segmento    	IS NULL THEN  'sin dato' ELSE cod_kpi_segmento 		END as cod_kpi_segmento,
CASE WHEN cod_kpi_subsegmento   IS NULL THEN  'sin dato' ELSE cod_kpi_subsegmento 	END as cod_kpi_subsegmento,
CASE WHEN cod_kpi_sucursal    	IS NULL THEN  'sin dato' ELSE cod_kpi_sucursal 		END as cod_kpi_sucursal,
CASE WHEN cod_kpi_canal    		IS NULL THEN  'sin dato' ELSE cod_kpi_canal 		END as cod_kpi_canal,
fc_kpi_operaciones      as fc_kpi_operacionessemanaanterior,
fc_kpi_clientes         as fc_kpi_clientessemanaanterior
FROM bi_corp_business.tbl_comex
Where  partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_week_same_date', dag_id='LOAD_BSN_Comex-Daily') }}'
) semanaanterior
ON comex.ds_coe_segmento      = semanaanterior.cod_kpi_segmento
and comex.ds_coe_subsegmento  = semanaanterior.cod_kpi_subsegmento
and comex.cod_coe_sucursal    = semanaanterior.cod_kpi_sucursal
and comex.ds_coe_canal        = semanaanterior.cod_kpi_canal
;