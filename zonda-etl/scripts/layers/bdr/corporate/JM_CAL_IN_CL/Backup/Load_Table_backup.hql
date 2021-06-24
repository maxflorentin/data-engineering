set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.JM_CAL_IN_CL_bkup partition(partition_date)
SELECT
e9954_feoperac,
e9954_s1emp,
e9954_idnumcli,
e9954_feccali,
e9954_tipmodel,
e9954_tipmode2,
e9954_idmodel,
e9954_tipo,
e9954_idpunsco,
e9954_feccaduc,
e9954_c1tarpun,
e9954_c1spid,
e9954_c1digcon,
e0621_fecultmo,
e9954_motivfor,
e9954_idpunsc2,
e9954_fecrepfi,
e9954_fecinofc,
partition_date
FROM bi_corp_bdr.JM_CAL_IN_CL
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Backup') }}'
;