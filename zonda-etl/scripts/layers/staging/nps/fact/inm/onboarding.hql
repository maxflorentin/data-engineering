set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;
set hive.exec.max.dynamic.partitions=500;
set hive.exec.max.dynamic.partitions.pernode=500;

insert overwrite table bi_corp_staging.qualtrics_nps_onboarding
partition (partition_date)
SELECT fecha_proceso as start_date, fecha_proceso as end_date,
null as status,
null as ip_address,
null as progress,
null as duration,
null as finished,
null as recorded_date,
id_respuesta as response_id,
null as recipient_last_name,
null as recipient_first_name,
direcc_correo_electronico as recipient_email,
null as external_reference,
null as location_latitude,
null location_longitude,
'email' as distribution_channel,
'ES' as user_language,
CASE WHEN nps_onb BETWEEN 0 AND 6 THEN 1
	 WHEN nps_onb BETWEEN 7 AND 8 THEN 2
	 WHEN nps_onb BETWEEN 9 AND 10 THEN 3 END as q1_nps_group,
null as q1,
null as q2,
from_unixtime(unix_timestamp(fecha_encuesta ,'yyyy-MM-dd HH:mm:ss'), 'dd/MM/yyyy') fecha_encuesta,
id_respuesta responseid,
id_respuesta,
desc_centro_alta,
edad,
antiguedad_en_meses,
nro_sucursal,
negociodesc,
telefono_celular1,
telefono_celular2,
canal_venta,
desc_canal_venta,
color,
desc_departamento,
nro_doc,
tipo_doc,
tipo_evento,
fecha_evento,
ejecutivo,
legajo,
sexo,
grado_vinculacion,
nombre,
nup,
telefono_fijo1,
telefono_fijo2,
producto_contab,
desc_prod_contab,
subprod_contab,
desc_subproducto,
rentabilidad,
region,
region_centro_alta,
desc_sector,
renta,
grupousuariodesc,
ubicacion,
puesto_usr_venta,
zona,
zona_suc_alta,
alerta_de_rescate,
alerta_onboarding,
alerta_rescate_onbarding,
satisf_atenc_onboarding,
entrega_onboarding,
tiempo_contrat_onboarding,
pregunta_inicial_onboarding,
recepcion_tarj_onboarding,
clarid_infor_onboarding,
document_necesaria_onboarding,
operar_tarj_onboarding,
document_firmada_onboarding,
satisf_canal_onboarding,
satisf_gral_onboarding,
satisf_rep_exec_onboarding,
wow_onboarding,
alerta_onboarding_select,
satisf_aten_onboarding_select,
tiempo_contrat_onboar_select,
tiempo_entrega_onboar_select,
onboarding_select_mot,
coment_claridad_onboar_select,
explicacion_benef_onboarding,
conocer_neces_onboar_select,
satisf_canal_onboar_select,
satisf_gral_onboar_select,
wow_onboarding_select,
continuar_con_banco,
recomendar_banco,
satisfacc_atencion_comun,
satisfacc_banco,
inter_asun_cte_act_serv,
trato_cordia_act_serv,
profes_actitud_serv,
simplicidad_actit_serv,
rapid_efic_resol_act_serv,
etiquetas,
interes_proceso_onboarding,
trato_cordialidad_onboard,
asis_claves_onboarding,
asis_personal_onboarding,
comunicacion_onboarding,
fecha_entrega_onboarding,
tiempo_entrega_onboarding,
lugar_entrega_onboarding,
val_datos_onboarding,
especif_prod_onboarding,
docum_onboarding,
info_onboarding,
simp_tramite_onboarding,
interes_proceso_slct,
trato_cordialidad_slct,
asis_claves_slct,
asis_personal_slct,
comunicacion_slct,
fecha_entrega_slct,
tiempo_entrega_slct,
lugar_entrega_slct,
val_datos_slct,
especif_productos_slct,
docum_slct,
info_slct,
simp_tramite_slct,
tiene_debito,
marca_ges_remoto,
rec_ejecutivo,
com_ale_nps,
com_mej_nps,
com_rec_nps,
nps_ejecutivo_slct,
com_ale_nps_slct,
com_mej_nps_slct,
com_rec_nps_slct,
null as gateway_alias,
null as recipient_id,
null as test,
null as transaction_date,
segmento,
canal,
nro_ubicacion,
direcc_correo_electronico,
nps_onb_slct,
comentarios_onbo_slct,
nps_onb,
comentarios_onboarding,
momento_critico,
null as usuario_qualtrics,
CASE WHEN value rlike '[0-9]' THEN 'Q1' ELSE 'Q2' END as variable,
value as respuesta,
CASE WHEN value rlike '[0-9]' THEN 'Q1' ELSE 'Q2' END as question_id,
CASE WHEN value rlike '[0-9]' THEN '¿Qué tan probable es que recomiendes la gestión de entrega de las tarjetas a un familiar, amigo o colega?'
ELSE 'Por favor, contanos el motivo de tu evaluación:' END as question_desc,
partition_date
FROM bi_corp_staging.rio61_nps_onboarding lateral view explode(array(nps_onb,comentarios_onboarding)) orig_table_alias as value
where partition_date BETWEEN '2019-01-01' and '2020-01-01'