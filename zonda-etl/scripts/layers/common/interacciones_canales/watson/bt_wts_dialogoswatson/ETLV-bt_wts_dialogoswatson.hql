"
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.bt_wts_dialogoswatson
PARTITION(partition_date)
SELECT 
	TRIM(canal) cod_wts_canal ,
	IF(jsessionid = 'NA', NULL, TRIM(jsessionid)) cod_wts_jsessionid ,
	fechahora ,
	TRIM(emisortipo) cod_wts_emisortipo ,
	TRIM(emisornomrbre) ds_wts_emisornombre ,
	TRIM(message) ds_wts_message ,
	IF(intent = 'null', NULL, TRIM(intent)) ds_wts_intent ,
	IF(confidence = 'null', NULL, TRIM(confidence)) fc_wts_confidenceintent ,
	IF(ask_answer_was_useful = 'NA', NULL, IF(ask_answer_was_useful = 'false', 0, 1)) flag_wts_respuestautil ,
	IF(possiblequestions = 'null' OR possiblequestions = '[]', NULL, TRIM(possiblequestions)) ds_wts_possiblequestions , 
	IF(suggestion_topics = 'null' OR suggestion_topics = '[]', NULL, TRIM(suggestion_topics)) ds_wts_suggestiontopics ,
	IF(`options` = '[]', NULL, TRIM(`options`)) ds_wts_options ,
	CAST(nupcliente AS INT) cod_per_nup ,
	TRIM(idgenesys) cod_wts_idgenesys ,
	IF(transferidoahumano IS NULL, NULL, IF(transferidoahumano = 'NO', 0, 1)) flag_wts_transferidohumano ,
	IF(fuepossiblequestions IS NULL, NULL, IF(fuepossiblequestions = 'false', 0, 1)) flag_wts_possiblequestions ,
	IF(fuesuggestion_topics IS NULL, NULL, IF(fuesuggestion_topics = 'False', 0, 1)) flag_wts_suggestiontopics ,
	IF(fueoptions IS NULL, NULL,IF(fueoptions = 'False', 0, 1)) flag_wts_options ,
	partition_date
FROM bi_corp_staging.watson_chat
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Watson-Daily') }}' ;
"