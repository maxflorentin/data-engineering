set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


CREATE TEMPORARY TABLE p_garantia AS
SELECT distinct 'garantia_miga' source
                ,concat_ws('_', trim(num_persona), trim(cod_divisa)) id_cto_source
from bi_corp_bdr.garant_miga 

CREATE TEMPORARY TABLE max_id_cto_gara_miga AS
SELECT COALESCE(MAX(cast(id_cto_bdr as BIGINT)), 0) as max_id_cto
FROM bi_corp_bdr.normalizacion_id_garantias;


INSERT INTO TABLE bi_corp_bdr.normalizacion_id_garantias
PARTITION (partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}')
SELECT lpad(CAST(y.max_id_cto AS BIGINT) + z.rownum, 9, '0') as id_cto_bdr,
        z.source,
        z.id_cto_source
FROM 
    (
        SELECT x.*, 99999999 + ROW_NUMBER() OVER() AS rownum
        FROM
            (
                SELECT p.*
                FROM p_garantia p
                LEFT JOIN bi_corp_bdr.normalizacion_id_garantias h ON h.id_cto_source = p.id_cto_source
                WHERE h.id_cto_source is null
            ) x
    ) z
INNER JOIN
    max_id_cto_gara_miga y;


