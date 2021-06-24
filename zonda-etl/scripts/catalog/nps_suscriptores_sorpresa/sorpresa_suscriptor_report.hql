"set mapred.job.queue.name=root.dataeng;
DROP TABLE IF EXISTS MAX_DATE_SORP;
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_DATE_SORP as
select max(partition_date) max_date
from bi_corp_staging.rio102_sorpresa_cascada;
select
sor.nup,
sor.cascada_yn,
sor.canal_alta,
sor.partition_date
from bi_corp_staging.rio102_sorpresa_cascada sor, MAX_DATE_SORP max
where sor.partition_date = max.max_date
and sor.fecha_fin is null;
"