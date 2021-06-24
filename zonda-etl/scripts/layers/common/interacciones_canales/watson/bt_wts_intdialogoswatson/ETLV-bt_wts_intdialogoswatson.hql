"
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.bt_wts_intdialogoswatson
PARTITION(partition_date)
SELECT 
	TRIM(header) cod_wts_header ,
	TRIM(id) cod_wts_conversacion ,
	from_unixtime(unix_timestamp(fecha ,'ddMMyyyy HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') ts_wts_conversacion ,
	TRIM(canal) cod_wts_canal ,
	IF(nup = 0, NULL, CAST(nup AS INT)) cod_per_nup ,
	IF(mora_temprana = 'true', 1, 0) flag_wts_moratemprana ,
	IF(mora_tardia = 'true', 1, 0) flag_wts_moratardia ,
	IF(transferido_humano = 'true', 1, 0) flag_wts_transferidohumano ,
	IF(intent_mora = 'true', 1, 0) flag_wts_intencionmora ,
	IF(jsessionid = 'NA', NULL, TRIM(jsessionid)) cod_wts_jsessionid ,
	IF(pdfpsueldo = 'true', 1, 0) flag_wts_pdfsueldo ,
	TRIM(conversationid) cod_wts_sesionwatson ,
	TRIM(pregunta) ds_wts_pregunta ,
	TRIM(respuesta) ds_wts_respuesta ,
	IF(final_intent = 'NA', NULL, TRIM(final_intent)) ds_wts_finalintent ,
	IF(intent = 'NA', NULL, TRIM(intent)) ds_wts_intent ,
	confidence_intent fc_wts_confidenceintent ,
	IF(ask_answer_was_useful = 'true', 1, 0) flag_wts_respuestautil ,
	IF(possiblequestions = '[]', NULL, TRIM(possiblequestions)) ds_wts_possiblequestions ,
	IF(suggestiontopics = '[]', NULL, TRIM(suggestiontopics)) ds_wts_suggestiontopics ,
	IF(`options` = '[]', NULL, TRIM(`options`)) ds_wts_options ,
	IF(waspossiblequstions = 'true', 1, 0) flag_wts_possiblequestions ,
	IF(was_suggestion_topics = 'true', 1, 0) flag_wts_suggestiontopics ,
	IF(wasoptions = 'true', 1, 0) flag_wts_options ,
	from_unixtime(unix_timestamp(fechainicio ,'ddMMyyyy HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') ts_wts_pregunta ,
	from_unixtime(unix_timestamp(fechafin ,'ddMMyyyy HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') ts_wts_respuesta ,
	IF(list_intent = 'NA', NULL, TRIM(list_intent)) ds_wts_listintent ,
	IF(finalintent_aux1 = 'NA', NULL, TRIM(finalintent_aux1)) ds_wts_finalintentaux1 ,
	confidencefinalintent_aux1 fc_wts_confidencefinalintentaux1 ,
	IF(finalintent_aux2 = 'NA', NULL, TRIM(finalintent_aux2)) ds_wts_finalintentaux2 ,
	confidencefinalintent_aux2 fc_wts_confidencefinalintentaux2 ,
	IF(telefono = 'NA', NULL, TRIM(telefono)) ds_wts_telefono ,
	IF(espas = 'true', 1, 0) flag_wts_espas,
	IF(id_gec = 'NA', NULL, id_gec) cod_wts_gec ,
	IF(from1 = 'NA', NULL, TRIM(from1)) ds_wts_from1 ,
	IF(intecionderivacion = 'true', 1, 0) flag_wts_intencionderivacion ,
	IF(asesordisponible = 'true', 1, 0) flag_wts_asesordisponible ,
	IF(ofreciomail = 'true', 1, 0) flag_wts_ofreciomail ,
	IF(fuerahorario = 'true', 1, 0) flag_wts_fuerahorario ,
	partition_date
FROM bi_corp_staging.watson_dialog
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Watson-Daily') }}' ;
"