"
SET mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.dim_per_mails
PARTITION(partition_date)
SELECT malpe_pedt003.penumper cod_per_nup
, CAST(trim(malpe_pedt003.pesecdom) AS INT) ds_per_secuencia
, trim(substr(malpe_pedt003.peobserv, 3, 50)) ds_per_mail
, trim(substr(malpe_pedt003.peobserv, 141, 3)) ds_per_fuente
, trim(substr(malpe_pedt003.peobserv, 144, 2)) ds_per_sec_fuente
, trim(substr(malpe_pedt003.peobserv, 146, 1)) ds_per_estado
, malpe_pedt003.pefecalt dt_per_fecha_alta
, to_date(malpe_pedt003.pehstamp) dt_per_fecha_modificacion
, malpe_pedt003.partition_date partition_date
FROM bi_corp_staging.malpe_pedt003 
WHERE malpe_pedt003.petipdom LIKE 'ELE'
AND malpe_pedt003.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_Malpe-Daily') }}'
"