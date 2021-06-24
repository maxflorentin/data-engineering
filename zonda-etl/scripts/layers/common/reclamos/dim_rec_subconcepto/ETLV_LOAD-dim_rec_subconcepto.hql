SET mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

-- Calculamos la maxima particion
DROP TABLE IF EXISTS max_partition_subconcepto;
CREATE TEMPORARY TABLE max_partition_subconcepto AS
SELECT cod_subcpto, max(partition_date) max_partition_date
FROM bi_corp_staging.rio56_subconcepto
where partition_date <= '2021-02-21'
group by cod_subcpto;

INSERT OVERWRITE TABLE bi_corp_common.dim_rec_subconcepto
SELECT
trim(cod_entidad) cod_rec_entidad ,
trim(cod_subcpto) cod_rec_subconcepto ,
trim(desc_subcpto) ds_rec_subconcepto ,
case when trim(desc_detall_subcpto)='null' then null else trim(desc_detall_subcpto) end AS ds_rec_detalle_subconcepto ,
trim(est_subcpto) cod_rec_estado_subconcepto,
'2021-02-21' partition_date
FROM
(
SELECT cod_entidad , a.cod_subcpto , desc_subcpto , desc_detall_subcpto , est_subcpto ,partition_date
FROM bi_corp_staging.rio56_subconcepto a
inner join max_partition_subconcepto b on a.cod_subcpto=b.cod_subcpto and a.partition_date=b.max_partition_date
) A;