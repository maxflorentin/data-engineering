"SET mapred.job.queue.name=root.dataeng;
 SELECT 1 FROM bi_corp_staging.getnet_merchants
 where partition_date = '2020-20-20'"