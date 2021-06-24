"
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.bt_cc_sesion
PARTITION(partition_date)
select
sesion																														COD_CC_SESION ,
TRIM(operadora)													    														COD_CC_OPERADORA,
clave                                                                                                                       COD_CC_CLAVE,
CASE WHEN TRIM(tipodoc)='null' or TRIM(tipodoc)=''	then NULL ELSE TRIM(tipodoc)	END										COD_CC_TIPODOC,
CAST(se.documento AS BIGINT)												    										    DS_CC_NUMDOC,
CASE WHEN razonsocial='null' THEN NULL else trim(razonsocial)	END															DS_CC_RAZONSOCIAL,
CASE WHEN cantproductos='null' THEN NULL else trim(cantproductos)	END	    												DS_CC_CANTPRODUCTOS,
CASE WHEN tipopersona='null' or TRIM(tipopersona)='' THEN NULL else trim(tipopersona)	END	 								DS_CC_TIPOPERSONA,
CASE WHEN TRIM(cantfaxpendientes)='null' THEN NULL else trim(cantfaxpendientes)	END											DS_CC_CANTFAXPENDIENTES,
CASE WHEN trim(firmantesesion)='S' then 1 else 0 end 																		FLAG_CC_FIRMANTESESION,
CASE WHEN trim(indsinonimo)='1' then 1 else 0 end 																		    FLAG_CC_INDSINONIMO,
trim(mdlwok)																												DS_CC_MDLWOK,
CASE WHEN nodoarbolivr='null' or TRIM(nodoarbolivr)='' THEN NULL else trim(nodoarbolivr)	END	 						    DS_CC_NODOARBOLIVR,
SUBSTRING(se.fecha,1,19)																									TS_CC_FECHA,
CASE WHEN nombrealtair='null' or TRIM(nombrealtair)='' THEN NULL else trim(nombrealtair)	END	 						    DS_CC_NOMBRE,
CASE WHEN primerapellido='null' or TRIM(primerapellido)='' THEN NULL else trim(primerapellido)	END	 						DS_CC_PRIMERAPELLIDO,
CASE WHEN segundoapellido='null' or TRIM(segundoapellido)='' THEN NULL else trim(segundoapellido)	END	 					DS_CC_SEGUNDOAPELLIDO,
cast(nrounicopersonaaltair as bigint) 																						COD_PER_NUP,
CASE WHEN IDRACF='null' or TRIM(IDRACF)='' THEN NULL else trim(IDRACF)	END	 												DS_CC_IDRACF,
CASE WHEN tipopublicidad='null' or TRIM(tipopublicidad)='' THEN NULL else trim(tipopublicidad)	END	 						DS_CC_TIPOPUBLICIDAD,
CASE WHEN trim(marcaip)='S' then 1 else 0 end 																				FLAG_CC_MARCAIP,
CASE WHEN trim(marcaanph)='S' then 1 else 0 end 																			FALG_CC_MARCAANPH,
CASE WHEN semaforofacturacion='null' or TRIM(semaforofacturacion)='' THEN NULL else trim(semaforofacturacion)	END	 	    DS_CC_SEMAFOROFACTURACION,
CASE WHEN semafororentabilidad='null' or TRIM(semafororentabilidad)='' THEN NULL else trim(semafororentabilidad)	END	 	DS_CC_SEMAFORORENTABILIDAD,
CASE WHEN semafororiesgo='null' or TRIM(semafororiesgo)='' THEN NULL else trim(semafororiesgo)	END	 						DS_CC_SEMAFORORIESGO,
CASE WHEN idcontacto='null' or TRIM(idcontacto)='' THEN NULL else trim(idcontacto)	END	 									COD_CC_CONTACTO,
CASE WHEN idservicio='null' or TRIM(idservicio)='' THEN NULL else trim(idservicio)	END	 									COD_CC_SERVICIO,
CASE WHEN idconversacion='null' or TRIM(idconversacion)='' THEN NULL else trim(idconversacion)	END	 						COD_CC_CONVERSACION,
CASE WHEN esmonoproducto='null' or TRIM(esmonoproducto)='' THEN NULL else trim(esmonoproducto)	END	 					    DS_CC_ESMONOPRODUCTO,
CASE WHEN tipoclave='null' or TRIM(tipoclave)='' THEN NULL else trim(tipoclave)	END	 					    				DS_CC_TIPOCLAVE ,
CASE WHEN trim(mora)='S' then 1 else 0 end 																				    FLAG_CC_MORA,
CASE WHEN trim(horariomora)='S' then 1 else 0 end 																			FLAG_CC_HORARIOMORA,
CASE WHEN SESIONID IS NOT NULL THEN 1 ELSE 0 END 																			FLAG_CC_ABANDONO_IVR,
TRIM(descrip)																												DS_CC_ABANDONO,
se.partition_date																											PARTITION_DATE


from bi_corp_staging.rio32_HISTCLIENTESSESION SE
LEFT JOIN bi_corp_staging.rio32_histlog_callback BACK
ON (SE.SESION=BACK.SESIONID and
back.partition_date=IF("{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}"<"2019-12-03","2019-12-03","{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}"))
where se.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}"

"