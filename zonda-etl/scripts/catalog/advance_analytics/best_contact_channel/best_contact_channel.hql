"set mapred.job.queue.name=root.dataeng;
select cast (cod_per_nup  as int) ,
  digital_30_n ,
  notificaciones ,
  sucursal_60 ,
  atm_60 ,
  homebanking_60 ,
  mobile_60 ,
  call_60_sl ,
  call_60_tc ,
  chat_60 ,
  email_60 ,
  redes_60 ,
  notificacion_hb_60 ,
  notificacion_mob_60 ,
  asistente_appm_60 ,
  asistente_tban_60 ,
  canal_pref_60 ,
  homebanking_3060 ,
  notificacion_hb_3060 ,
  asistente_appm_3060 ,
  notificacion_mob_3060 ,
  email_3060 ,
  canal_pref_3060 ,
  redes_3060 ,
  sucursal_3060 ,
  mobile_3060 ,
  call_3060_tc ,
  call_3060_sl ,
  chat_3060 ,
  atm_3060 ,
  asistente_tban_3060 ,
  sucursal_30 ,
  atm_30 ,
  homebanking_30 ,
  mobile_30 ,
  call_30_sl ,
  call_30_tc ,
  chat_30 ,
  email_30 ,
  redes_30 ,
  notificacion_hb_30 ,
  notificacion_mob_30 ,
  asistente_appm_30 ,
  asistente_tban_30 ,
  canal_pref_30 ,
  sucursal_7 ,
  atm_7 ,
  homebanking_7 ,
  mobile_7 ,
  call_7_sl ,
  call_7_tc ,
  chat_7 ,
  email_7 ,
  redes_7 ,
  notificacion_hb_7 ,
  notificacion_mob_7 ,
  asistente_appm_7 ,
  asistente_tban_7 ,
  canal_pref_7 ,
  canal_reciente ,
  fecha_reciente ,
  partition_date 
from bi_corp_staging.mlops_bccdd
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='REPORT_AA_BEST_CONTACT') }}' "