CREATE OR REPLACE VIEW  bi_corp_common.control_usd_view
AS select sucursal,
cuenta,
cod_op,
cast( regexp_replace(importe,",",".") as double) as importe,
cast(concat(
substr(extraction_date, 0, 10),' ',
regexp_replace(substr(extraction_date, 12,8),'\\.','\\:'),
substr(extraction_date, 20,6),'000') as timestamp)as extraction_date,
partition_date
from bi_corp_staging.control_usd
where partition_date>'2019-10-29'
union all
select sucursal,
cuenta,
cod_op,
cast( regexp_replace(importe,",",".") as double) as importe,
cast(concat(
substr(extraction_date, 0, 10),' ',
regexp_replace(substr(extraction_date, 12,8),'\\.','\\:'),
substr(extraction_date, 20,6),'000') as timestamp)as extraction_date,
partition_date
from bi_corp_staging.control_usd_history
;