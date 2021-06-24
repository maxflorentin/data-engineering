SET mapred.job.queue.name=root.dataeng;
INSERT OVERWRITE LOCAL DIRECTORY '/aplicaciones/bi/zonda/tmp/bajadaArda/metricas_rda'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
SELECT
    *
FROM bi_corp_staging.amas_wamas02
LIMIT 1000;