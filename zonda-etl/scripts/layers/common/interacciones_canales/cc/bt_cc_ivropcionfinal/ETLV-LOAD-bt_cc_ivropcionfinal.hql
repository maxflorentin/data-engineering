"
SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT  overwrite TABLE bi_corp_common.bt_cc_ivropcionfinal
PARTITION (partition_date)

select
DISTINCT SUBSTRING(fecha,1,19)   																				ts_cc_fecha,
sesion  																				cod_cc_sesion,
CASE WHEN vdn_entrada='null' then NULL else  trim(vdn_entrada)	end 					cod_cc_vdn_entrada,
CASE WHEN codigoapp='null' then NULL else  trim(codigoapp)	end 						cod_cc_codigoapp,
CASE WHEN opcion_ivr='null' then NULL else  trim(opcion_ivr)	end 					ds_cc_opcion_ivr,
CASE WHEN reclamo= 'NULL' or reclamo='null'
     THEN NULL ELSE trim(reclamo) 	end													ds_cc_reclamo,
case when siniestro='S' then 1 else 0 end 												flag_cc_siniestro,
CASE WHEN vdn_salida='null' then NULL else  trim(vdn_salida)	end 					cod_cc_vdn_salida,
CASE WHEN vq='NULL' or vq='null' then NULL else  trim(vq)	end 						ds_cc_vq,
CASE WHEN grupo_agentes='null' OR TRIM(grupo_agentes)='NULL'
then NULL else  trim(grupo_agentes)	end 				                                cod_cc_grupo_agentes,
CASE WHEN grupo_agentes_des='null' OR TRIM(grupo_agentes_des)='NULL'
then NULL else  trim(grupo_agentes_des)	end 		                                    ds_cc_grupo_agentes,
CASE WHEN vq_estadistica= 'NULL' or vq_estadistica='null'
     THEN NULL ELSE trim(vq_estadistica)  END 											ds_cc_vq_estadistica,
CASE WHEN habilidad_1= 'NULL' or habilidad_1='null' THEN NULL ELSE trim(habilidad_1)  	END					ds_cc_habilidad1,
CASE WHEN habilidad_2= 'NULL' or habilidad_2='null'THEN NULL ELSE trim(habilidad_2)  	END					ds_cc_habilidad2,
CASE WHEN trim(operador_tit_cartera)='NULL'or operador_tit_cartera='null'
THEN NULL ELSE trim(operador_tit_cartera) END 											ds_cc_operador_titcartera,
CASE WHEN trim(ultimo_operador_solicitud)= 'NULL' or ultimo_operador_solicitud='null'
     THEN NULL ELSE trim(ultimo_operador_solicitud)  END							    ds_cc_ultimo_operadorsolicitud,
CASE WHEN ultimo_grupo_reclamo= 'NULL' or ultimo_grupo_reclamo='null'
     THEN NULL ELSE trim(ultimo_grupo_reclamo)  END										ds_cc_ultimo_gruporeclamo,
CASE WHEN puntaje_prioridad= 'NULL' or puntaje_prioridad='null'
     THEN NULL ELSE trim(puntaje_prioridad) END											ds_cc_puntaje_prioridad,
CASE WHEN alertas= 'NULL' or alertas='null'
     THEN NULL ELSE trim(alertas) 		END												ds_cc_puntaje_alertas,
CASE WHEN pf_vencido= 'NULL' or pf_vencido='null'
     THEN NULL ELSE trim(pf_vencido) 	END    											ds_cc_puntaje_pfvencido,
CASE WHEN tc_a_vencer= 'NULL' or tc_a_vencer='null'
     THEN NULL ELSE trim(tc_a_vencer) 	END    											ds_cc_tc_avencer,
CASE WHEN usuario_olb= 'NULL' or usuario_olb='null'
     THEN NULL ELSE trim(usuario_olb) 	END    											ds_cc_usuario_olb,
CASE WHEN mora= 'NULL' or mora='null'
     THEN NULL ELSE trim(mora) 	 END   													ds_cc_mora,
CASE WHEN correo= 'NULL' or correo='null'
     THEN NULL ELSE trim(correo) END    											    ds_cc_correo,
CASE WHEN solicitud= 'S'
     THEN 1 ELSE 0 END	    											                flag_cc_solicitud,
CASE WHEN campania_activa= 'NULL' or campania_activa='null'
     THEN NULL ELSE trim(campania_activa) END    										ds_cc_campania_activa,
CASE WHEN encuesta_negativa= 'NULL' or encuesta_negativa='null'
     THEN NULL ELSE trim(encuesta_negativa) END    										ds_cc_encuesta_negativa,
CASE WHEN audio= 'NULL' or audio='null'
     THEN NULL ELSE trim(audio) END    													ds_cc_audio,
CASE WHEN accion= 'NULL' or accion='null'
     THEN NULL ELSE trim(accion) END    												ds_cc_accion,
partition_date																			partition_date

from bi_corp_staging.rio32_histdatosruteo_na
where partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}"

union all

select
SUBSTRING(fecha,1,19)   																				ts_cc_fecha,
sesion  																				cod_cc_sesion,
CASE WHEN vdn_entrada='null' then NULL else  trim(vdn_entrada)	end 					cod_cc_vdn_entrada,
CASE WHEN codigoapp='null' then NULL else  trim(codigoapp)	end 						cod_cc_codigoapp,
CASE WHEN opcion_ivr='null' then NULL else  trim(opcion_ivr)	end 					ds_cc_opcion_ivr,
CASE WHEN reclamo= 'NULL' or reclamo='null'
     THEN NULL ELSE trim(reclamo) 	end													ds_cc_reclamo,
case when siniestro='S' then 1 else 0 end 												flag_cc_siniestro,
CASE WHEN vdn_salida='null' then NULL else  trim(vdn_salida)	end 					cod_cc_vdn_salida,
CASE WHEN vq='null' or TRIM(vq)='NULL' then NULL else  trim(vq)	end 									ds_cc_vq,
CASE WHEN grupo_agentes='null' or TRIM(grupo_agentes)='NULL' then NULL else  trim(grupo_agentes)	end 				cod_cc_grupo_agentes,
CASE WHEN grupo_agentes_des='null' or TRIM(grupo_agentes_des)='NULL'  then NULL else  trim(grupo_agentes_des)	end 		ds_cc_grupo_agentes,
CASE WHEN vq_estadistica= 'NULL' or vq_estadistica='null'
     THEN NULL ELSE trim(vq_estadistica)  END 											ds_cc_vq_estadistica,
CASE WHEN habilidad_1= 'NULL' or habilidad_1='null' THEN NULL ELSE trim(habilidad_1)  	END					ds_cc_habilidad1,
CASE WHEN habilidad_2= 'NULL' or habilidad_2='null'THEN NULL ELSE trim(habilidad_2)  	END					ds_cc_habilidad2,
CASE WHEN trim(operador_tit_cartera)='NULL'or operador_tit_cartera='null'
THEN NULL ELSE trim(operador_tit_cartera) END 											ds_cc_operador_titcartera,
CASE WHEN TRIM(ultimo_operador_solicitud)= 'NULL' or ultimo_operador_solicitud='null'
     THEN NULL ELSE trim(ultimo_operador_solicitud)  END							    ds_cc_ultimo_operadorsolicitud,
CASE WHEN ultimo_grupo_reclamo= 'NULL' or ultimo_grupo_reclamo='null'
     THEN NULL ELSE trim(ultimo_grupo_reclamo)  END										ds_cc_ultimo_gruporeclamo,
CASE WHEN puntaje_prioridad= 'NULL' or puntaje_prioridad='null'
     THEN NULL ELSE trim(puntaje_prioridad) END											ds_cc_puntaje_prioridad,
CASE WHEN alertas= 'NULL' or alertas='null'
     THEN NULL ELSE trim(alertas) 		END												ds_cc_puntaje_alertas,
CASE WHEN pf_vencido= 'NULL' or pf_vencido='null'
     THEN NULL ELSE trim(pf_vencido) 	END    											ds_cc_puntaje_pfvencido,
CASE WHEN tc_a_vencer= 'NULL' or tc_a_vencer='null'
     THEN NULL ELSE trim(tc_a_vencer) 	END    											ds_cc_tc_avencer,
CASE WHEN usuario_olb= 'NULL' or usuario_olb='null'
     THEN NULL ELSE trim(usuario_olb) 	END    											ds_cc_usuario_olb,
CASE WHEN mora= 'NULL' or mora='null'
     THEN NULL ELSE trim(mora) 	 END   													ds_cc_mora,
CASE WHEN correo= 'NULL' or correo='null'
     THEN NULL ELSE trim(correo) END    											    ds_cc_correo,
CASE WHEN solicitud= 'S'
     THEN 1 ELSE 0 END	    											                flag_cc_solicitud,
CASE WHEN campania_activa= 'NULL' or campania_activa='null'
     THEN NULL ELSE trim(campania_activa) END    										ds_cc_campania_activa,
CASE WHEN encuesta_negativa= 'NULL' or encuesta_negativa='null'
     THEN NULL ELSE trim(encuesta_negativa) END    										ds_cc_encuesta_negativa,
NULL   																					ds_cc_audio,
NULL   																					ds_cc_accion,
partition_date																			partition_date

from bi_corp_staging.rio32_histdatosruteo
where partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}" and  vdn_entrada!='null'

"