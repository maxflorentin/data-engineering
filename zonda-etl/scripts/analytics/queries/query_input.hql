SET mapred.job.queue.name=root.dataeng;
select * from bi_corp_staging.watson_chat where partition_date = date_format('{}', 'yyyy-MM-dd') and intent != 'CHIT_hola'