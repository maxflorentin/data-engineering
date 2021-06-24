SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT  overwrite TABLE bi_corp_common.bt_cc_asistentevirtual PARTITION (partition_date)

Select
nupcliente 		as cod_per_nup,
idgenesys 		as cod_cc_genesys,
canal 			as ds_cc_canal,
jsessionid		as cod_cc_sesion,
fechahora 		as ts_cc_chat,
emisortipo		as ds_cc_emisortipo,
emisornomrbre 	as ds_cc_emisornombre,
message			as ds_cc_mensaje,
CASE WHEN intent = 'null' THEN  NULL ELSE TRIM(intent) END  as ds_cc_intencion,
CASE WHEN confidence = 'null' THEN  NULL ELSE TRIM(confidence) END  as ds_cc_confidencia,
CASE WHEN ask_answer_was_useful ='true' THEN 1
	 WHEN ask_answer_was_useful = 'false' THEN 0
ELSE NULL END as flag_cc_utilpreguntarespuesta,
CASE WHEN possiblequestions = 'null' THEN  NULL ELSE TRIM(possiblequestions) END  as ds_cc_preguntaposible,
CASE WHEN suggestion_topics = 'null' THEN  NULL ELSE TRIM(suggestion_topics) END  as ds_cc_temasugerncia,
CASE WHEN `options` = 'null' THEN  NULL ELSE TRIM(`options`) END  as ds_cc_opciones,
CASE WHEN transferidoahumano ='SI' THEN 1 ELSE 0 END as flag_cc_transferidoahumano,
CASE WHEN fuepossiblequestions ='True' THEN 1 ELSE 0 END as flag_cc_fueposiblepregunta,
CASE WHEN fuesuggestion_topics ='True' THEN 1 ELSE 0 END as flag_cc_fueposiblesugenrencia,
CASE WHEN fueoptions ='True' THEN 1 ELSE 0 END as flag_cc_fueopciones,
partition_date

FROM
	bi_corp_staging.watson_chat
Where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}';
