SET mapred.job.queue.name=root.dataeng;
select ivct_nu_nup 
, max(case when ivct_cd_ramo in ('21') then 1 else 0 end) as cotizacion_vivienda
, max(case when ivct_cd_ramo in ('30','32','33','35','36','39') then 1 else 0 end) as cotizacion_auto
, max(case when ivct_cd_ramo not in ('21','30','32','33','35','36','39') then 1 else 0 end) as cotizacion_otros
, max(case when ivct_nu_poliza is not null then 1 else 0 end) as cotizacion_alta
from bi_corp_staging.rio6_cotizaciones
where ivct_cd_tipo_mercado = 'OM'
and ivct_fe_operacion between '{}' and '{}'
and ivct_nu_nup is not null 
group by ivct_nu_nup