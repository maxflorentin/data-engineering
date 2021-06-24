"SET mapred.job.queue.name=root.dataeng;
select concat(SUBSTRING(data_date_part, 7, 2),'/', SUBSTRING(data_date_part, 5, 2),'/',SUBSTRING(data_date_part, 1, 4)) AS FECHA,
cod_especie_sam,
precio_compra,
precio_venta,
(precio_compra * 100) AS precio_compra_100,
(precio_venta * 100) AS precio_venta_100
from santander_business_risk_arda.precios_cierre_bna
where cod_mercado = 'OFI'
and data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date_filter', dag_id='REPORT_IIGG_DIARIO') }}'
"
