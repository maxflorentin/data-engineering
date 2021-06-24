"

SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;


INSERT OVERWRITE TABLE bi_corp_common.bt_cc_reintransaccion
PARTITION(partition_date)

select
DISTINCT headerid 						                                                                                COD_CC_HEADERID,
sessionid						                                                                                COD_CC_SESION,
hist.nombre							                                                                            COD_CC_NOMBRE,
trim(descripcion)								                                                                DS_CC_NOMBRE,
version 						                                                                                DS_CC_VERSION,
CASE WHEN numero='null' then NULL else 	trim(numero) end						                                DS_CC_NUMERO,
SUBSTRING(fechainicio,1,19)						                                                                TS_CC_FECHAINICIO,
CASE WHEN fechafinalizacion='null' then NULL else SUBSTRING(fechafinalizacion,1,19)	end				            TS_CC_FECHAFINALIZACION,
CASE WHEN backend='null' then NULL else trim(backend)	end							                            DS_CC_BACKEND,
CASE WHEN usuario='null' then NULL else trim(usuario)	end						                                DS_CC_USUARIO,
CASE WHEN terminal='null' then NULL else trim(terminal)	end								                        DS_CC_TERMINAL,
CASE WHEN status='null' then NULL else trim(status)	end							                                DS_CC_STATUS,
CASE WHEN estado='null' then NULL else trim(estado)	end							                                DS_CC_ESTADO,
CASE WHEN esreversible='1' then 1 else 0 end                                                                    FLAG_CC_REVERSIBLE,
CASE WHEN esreversa='1' then 1 else 0 end                                                                       FLAG_CC_REVERSA,
CASE WHEN revertida='1' then 1 else 0 end                                                                       FLAG_CC_REVERTIDA,
CASE WHEN canalid='null' then NULL else TRIM(canalid) end 							                            COD_CC_CANAL,
CASE WHEN cast(canalid as int)= 1 THEN 'IVR'
     WHEN cast(canalid as int)= 2 THEN 'SUPERLINEA'  ELSE NULL END                                              DS_CC_CANAL,
CASE WHEN canalversion='null' then NULL else TRIM(canalversion) end                                             DS_CC_CANALVERSION,
CASE WHEN subcanalid='null' then NULL else TRIM(subcanalid) end 						                        COD_CC_SUBCANAL,
CASE WHEN documentotipo='null' or trim(documentotipo)='' then NULL else TRIM(documentotipo) end                 COD_CC_DOCUMENTO,
CAST(documentonumero as bigint)		                                                                            DS_CC_NUMDOC,
CASE WHEN cuentaorigentipo='null' or trim(cuentaorigentipo)='' then NULL else TRIM(cuentaorigentipo) end        COD_CC_CUENTAORIGENTIPO,
CAST(cuentaorigennumero AS BIGINT) 		                                                                        DS_CC_CUENTAORIGENNUMERO,
CAST(cuentaorigensucursal AS INT) 		                                                                        DS_CC_CUENTAORIGENSUCURSAL,
CASE WHEN cuentadestinotipo='null' or trim(cuentadestinotipo)='' then NULL else TRIM(cuentadestinotipo) end     COD_CC_CUENTADESTINOTIPO,
CAST(cuentadestinonumero AS BIGINT) 		                                                                    DS_CC_CUENTADESTINONUMERO,
CAST(cuentadestinosucursal AS INT) 		                                                                        DS_CC_CUENTADESTINOSUCURSAL,
CAST(importe AS decimal(19,4)) 		                                                                            DS_CC_IMPORTE,
CASE WHEN tipocliente='null' or trim(tipocliente)='' then NULL else TRIM(tipocliente) end                       DS_CC_TIPOCLIENTE,
CASE WHEN bimonetaria='null' or trim(bimonetaria)='' then NULL else TRIM(bimonetaria) end                       DS_CC_BIMONETARIA,
CASE WHEN agendada='1' then 1 else 0 end                                                                        FLAG_CC_AGENDADA             ,
CASE WHEN canaltipo='null' or trim(canaltipo)='' then NULL else TRIM(canaltipo) end                             DS_CC_CANALTIPO,
CASE WHEN subcanaltipo='null' or trim(subcanaltipo)='' then NULL else TRIM(subcanaltipo) end                    DS_CC_SUBCANALTIPO,
CASE WHEN trim(idsecundario)='null' then NULL else TRIM(idsecundario) end 					                    DS_CC_IDSECUNDARIO,
CASE WHEN serviciopagado='null' or trim(serviciopagado)='' then NULL else TRIM(serviciopagado) end              DS_CC_SERVICIOPAGADO,
CASE WHEN simulacion='1' then 1 else 0 end                                                                      FLAG_CC_SIMULACION       ,
CASE WHEN identificacionpin='1' then 1 else 0 end                                                               FLAG_CC_IDENTIFICACIONPIN      ,
CASE WHEN verificacion='1' then 1 else 0 end                                                                    FLAG_CC_VERIFICACION       ,
CASE WHEN esgenuino='1' then 1 else 0 end                                                                       FLAG_CC_GENUINO      ,
aplicacion									                                                                    DS_CC_APLICACION,
hist.partition_date                                                                                             PARTITION_DATE

from bi_corp_staging.rio32_reg_elect_headers_hist   hist
left join bi_corp_staging.rio32_rln_transacciones tr on (tr.nombre=hist.nombre
        and tr.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}")
where hist.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}"

"