SET mapred.job.queue.name=root.dataeng;
insert OVERWRITE table analytics.watson_intent_clasificados partition (partition_date) select * from mytempTable