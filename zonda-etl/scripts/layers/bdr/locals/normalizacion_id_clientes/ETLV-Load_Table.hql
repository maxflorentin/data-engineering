set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

CREATE TEMPORARY TABLE perimetro_clientes_especies AS
SELECT DISTINCT trim(cod_emisor) as id_cte_source, 'especies' as source
FROM santander_business_risk_arda.especies
WHERE
data_date_part <=  '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_especies', dag_id='BDR_LOAD_Tables-Monthly') }}'
AND fec_info <= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
AND  cod_cartera in ('ALC', 'INV')
AND cod_emisor  is not null and cod_emisor != '';

INSERT INTO TABLE bi_corp_bdr.normalizacion_id_clientes
SELECT lpad(100000000 + z.rownum, 9, '0') as id_cte_bdr,
z.id_cte_source,
z.source,
current_date as inserted_date
FROM (
SELECT x.*, ROW_NUMBER() OVER( ORDER BY x.id_cte_source ) AS rownum
FROM
(
SELECT p.*
FROM perimetro_clientes_especies p
LEFT JOIN bi_corp_bdr.normalizacion_id_clientes h ON h.id_cte_source = p.id_cte_source
WHERE h.id_cte_source is null
) x
) z;
