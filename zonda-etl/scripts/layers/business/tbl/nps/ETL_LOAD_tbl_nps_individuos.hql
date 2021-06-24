SET hive.merge.mapfiles=TRUE;
SET hive.merge.mapredfiles=TRUE;
SET hive.merge.size.per.task=4000000;
SET hive.merge.smallfiles.avgsize=16000000;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;


create temporary table analytics.nps_temp as

select
'000005' as cod_kpi_kpi,
count (cod_nps_respuesta) as cant_clientes,
ds_nps_agrupados
from bi_corp_common.bt_nps_nps
Where
year (dt_nps_fecha_encuesta) = year('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_NPS-Daily') }}') and month(dt_nps_fecha_encuesta) = month ('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_NPS-Daily') }}')
and dt_nps_fecha_encuesta <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_NPS-Daily') }}'
and ds_nps_tipo_encuesta in ('RELACIONAL','RELACIONAL DUO')
and ds_nps_orden = 1
group by  ds_nps_agrupados
;


INSERT  overwrite TABLE bi_corp_business.tbl_nps PARTITION (cod_kpi_kpi, partition_date)

SELECT

       NULL                                                    					        AS ds_kpi_segmento,
       NULL                                                 					          AS ds_kpi_subsegmento,
       NULL                                                     				        AS cod_kpi_producto,
       NULL                                                  					          AS cod_kpi_subproducto,
       NULL                                                        				      AS cod_kpi_canal,
       NULL                                                 					          AS ds_kpi_destino,
       NULL                                                                     AS cod_kpi_zona,
       NULL                                                     				        AS cod_kpi_sucursal,
       NULL                                                       				      AS cod_kpi_divisa,
       nps.fc_kpi_metrica                                                 		  AS fc_kpi_metrica,
       mismodiamesanterior.fc_kpi_metricamismodiamesanterior                    AS fc_kpi_metricamismodiamesanterior,
       tmenos1.fc_kpi_metricadiaanterior                                        AS fc_kpi_metricadiaanterior,
       findemesanterior.fc_kpi_metricafindemesanterior                          AS fc_kpi_metricafindemesanterior,
       mismodiaanoanterior.fc_kpi_metricamismodiaanoanterior                    AS fc_kpi_metricamismodiaanoanterior,
       findeanoanterior.fc_kpi_metricafindeanoanterior                          AS fc_kpi_metricafindeanoanterior,
       semanaanterior.fc_kpi_metricasemanaanterior                              AS fc_kpi_metricasemanaanterior,
       NULL                                                      				        AS fc_kpi_clientes,
       NULL                   													                        AS fc_kpi_clientesmismodiamesanterior,
       NULL                                       								              AS fc_kpi_clientesdiaanterior,
       NULL                         											                      AS fc_kpi_clientesfindemesanterior,
       NULL                   													                        AS fc_kpi_clientesmismodiaanoanterior,
       NULL                         											                      AS fc_kpi_clientesfindeanoanterior,
       NULL                             										                    AS fc_kpi_clientessemanaanterior,
       NULL                                                    					        AS fc_kpi_operaciones,
       NULL                														                          AS fc_kpi_operacionesmismodiamesanterior,
       NULL                                    									                AS fc_kpi_operacionesdiaanterior,
       NULL                      												                        AS fc_kpi_operacionesfindemesanterior,
       NULL                														                          AS fc_kpi_operacionesmismodiaanoanterior,
       NULL                      												                        AS fc_kpi_operacionesfindeanoanterior,
       NULL                          											                      AS fc_kpi_operacionessemanaanterior,
       '000005'                                                              	  AS cod_kpi_kpi,
       '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_NPS-Daily') }}' as partition_date


FROM
(

Select
cod_kpi_kpi,
sum (a.Clientes_promotores) / (sum (a.Clientes_promotores)+sum (a.Clientes_neutros)+sum (a.Clientes_detractores)) - sum (a.Clientes_detractores) / (sum (a.Clientes_promotores)+sum (a.Clientes_neutros)+sum (a.Clientes_detractores))  as fc_kpi_metrica
from (
Select
cod_kpi_kpi,
case when ds_nps_agrupados = '1' 	then cant_clientes else 0 end Clientes_detractores,
case when ds_nps_agrupados = '2' 	then cant_clientes else 0 end Clientes_neutros,
case when ds_nps_agrupados = '3' 	then cant_clientes else 0 end Clientes_promotores
FROM analytics.nps_temp ) a
group by cod_kpi_kpi
) nps

LEFT JOIN
(
SELECT
       cod_kpi_kpi,
	   fc_kpi_metrica															AS fc_kpi_metricamismodiamesanterior

FROM bi_corp_business.tbl_nps
Where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_working_day', dag_id='LOAD_BSN_NPS-Daily') }}'
) mismodiamesanterior
  ON mismodiamesanterior.cod_kpi_kpi = nps.cod_kpi_kpi

  LEFT  JOIN
(
SELECT
        cod_kpi_kpi,
	   fc_kpi_metrica															AS fc_kpi_metricadiaanterior
FROM bi_corp_business.tbl_nps
Where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_working_day', dag_id='LOAD_BSN_NPS-Daily') }}'
) tmenos1
	ON  tmenos1.cod_kpi_kpi = nps.cod_kpi_kpi

  LEFT JOIN
(
SELECT
        cod_kpi_kpi,
	   fc_kpi_metrica															AS fc_kpi_metricafindemesanterior
FROM bi_corp_business.tbl_nps
Where  partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_last_working_day', dag_id='LOAD_BSN_NPS-Daily') }}'
) findemesanterior
	ON  findemesanterior.cod_kpi_kpi = nps.cod_kpi_kpi

	  LEFT  JOIN
(
SELECT
    cod_kpi_kpi,
	   fc_kpi_metrica															AS fc_kpi_metricamismodiaanoanterior
FROM bi_corp_business.tbl_nps
Where  partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_year_working_day', dag_id='LOAD_BSN_NPS-Daily') }}'
) mismodiaanoanterior
	ON  mismodiaanoanterior.cod_kpi_kpi = nps.cod_kpi_kpi

		 LEFT  JOIN
(
SELECT
cod_kpi_kpi,
	   fc_kpi_metrica															AS fc_kpi_metricafindeanoanterior
FROM bi_corp_business.tbl_nps
Where  partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_year_last_working_day', dag_id='LOAD_NPS_Cheques-Daily') }}'
) findeanoanterior
	ON  findeanoanterior.cod_kpi_kpi = nps.cod_kpi_kpi

		 LEFT  JOIN
(
SELECT
        cod_kpi_kpi,
	   fc_kpi_metrica															AS fc_kpi_metricasemanaanterior
FROM bi_corp_business.tbl_nps
Where  partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_week_same_date', dag_id='LOAD_BSN_NPS-Daily') }}'
) semanaanterior
	ON  semanaanterior.cod_kpi_kpi = nps.cod_kpi_kpi;
