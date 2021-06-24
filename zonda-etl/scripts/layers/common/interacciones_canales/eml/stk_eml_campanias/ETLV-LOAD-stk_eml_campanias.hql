"
SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.stk_eml_campanias
PARTITION (partition_date)

select
DISTINCT id 										cod_eml_campania,
name 									ds_eml_campania,
folder									ds_eml_carpeta,
vista									ds_eml_vista,
extid									ds_eml_extid,
purpose									ds_eml_proposito,
listname								ds_eml_nombre_lista,
htmlmgepath 							ds_eml_ruta_html,
case when enablelinktrack='1' then 1
	 else 0 end 						flag_eml_rastreo_linkcampania,
case when enableexterntrack='1' then 1
	 else 0 end 						flag_eml_rastreo_analisisterceros,
	 subject							ds_eml_asunto,
	 fromname							ds_eml_nombre_campania,
case when useutf8='1' then 1
	 else 0 end							flag_eml_utf8,
locale									ds_eml_config_regional,
case when trackhtmlopens='1' then 1
	 else 0 end 						flag_eml_abrio_html,
case when tipotrackconvers='1' then 1
	 else 0 end 						flag_eml_conversacion,
case when sendtextifhtmlunknown='1' then 1
	 else 0 end 						flag_eml_html_desconocido,
unsubscroptn							ds_eml_baja_campania,
autocloseopt							ds_eml_cierre_automitico,
mktstrategy	 							ds_eml_mkt_estrategia,
mktprogram								ds_eml_mkt_programa,
fromemail								ds_eml_email_campania,
replytoemail							ds_eml_email_responda,
partition_date
from bi_corp_staging.RESPONSYS_campanias
where partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_EML') }}"
"
