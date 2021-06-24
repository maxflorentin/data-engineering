"
SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Tengo entendido que aca estan todas, pero seguro estan las de Plazo Fijo

INSERT OVERWRITE TABLE bi_corp_common.dim_can_canalapertura
    PARTITION (partition_date)
SELECT substr(gen_clave,1,2) as cod_can_canalaperuta ,
trim(substr(gen_Datos,1,40)) as ds_can_canalapertura,
partition_date
FROM bi_corp_staging.tcdtgen
WHERE gentabla = '0222' and partition_date = "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Plazo_Fijo-Daily') }}"


"