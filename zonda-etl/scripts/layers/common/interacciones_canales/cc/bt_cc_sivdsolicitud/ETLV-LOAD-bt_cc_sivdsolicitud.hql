SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT  overwrite TABLE bi_corp_common.bt_cc_sivdsolicitud PARTITION (partition_date)

Select
DISTINCT solicitud 	as cod_cc_solicitud,
circular 	as cod_cc_circular,
CASE WHEN trim(promocion) ='' THEN NULL ELSE promocion END as cod_cc_promocion,
CASE WHEN trim(delivery) ='' THEN NULL ELSE delivery END as cod_cc_delivery,
canal 		as cod_cc_canal,
ca.descri        as ds_cc_canal,
cast(CASE WHEN trim(sucursal) ='' THEN NULL ELSE sucursal END as int) as cod_cc_sucursal,
CASE WHEN trim(familia) ='' THEN NULL ELSE familia END as cod_cc_familia,
CASE WHEN trim(programa) ='' THEN NULL ELSE programa END as cod_cc_programa,
CASE WHEN trim(lista) ='' THEN NULL ELSE lista END as cod_cc_lista,
CASE WHEN trim(vehiculo) ='' THEN NULL ELSE vehiculo END as cod_cc_vehiculo,
CASE WHEN trim(oferta1) ='' THEN NULL ELSE oferta1 END as cod_cc_oferta1,
CASE WHEN trim(oferta2) ='' THEN NULL ELSE oferta2 END as cod_cc_oferta2,
CASE WHEN trim(oferta3) ='' THEN NULL ELSE oferta3 END as cod_cc_oferta3,
anio 	as cod_cc_anio,
mes 	as cod_cc_mes,
estado 	as cod_cc_estado,
tip.descri    as ds_cc_estado,
concat (SUBSTRING(cambioestado,0,4),'-', SUBSTRING(cambioestado,5,2),'-',SUBSTRING(cambioestado,7,2))as dt_cc_cambioestado,
CASE WHEN trim(uniqueid) ='' THEN NULL ELSE uniqueid END as cod_cc_uniqueid,
solicitud_asol as cod_cc_solicitudasol,
CASE WHEN trim(returncode_asol) ='' THEN NULL ELSE returncode_asol END as cod_cc_returncodeasol,
CASE WHEN trim(observaciones_asol) ='' THEN NULL ELSE observaciones_asol END as cod_cc_observacionesasol,
nrocuenta_asol as cod_cc_nrocuentaasol,
usuario as cod_cc_legajo,
CASE WHEN cast(fechaimpresol as int) = 0 THEN NULL ELSE concat (SUBSTRING(fechaimpresol,0,4),'-', SUBSTRING(fechaimpresol,5,2),'-',SUBSTRING(fechaimpresol,7,2)) END as ts_cc_returncodeasol,
CASE WHEN trim(domicilioresumen) ='' THEN NULL ELSE domicilioresumen END as cod_cc_domicilioresumen,
CASE WHEN trim(promocion_asol) ='' THEN NULL ELSE promocion_asol END as cod_cc_promocionasol,
CASE WHEN trim(ultoperacionvisita) ='' THEN NULL ELSE ultoperacionvisita END as cod_cc_ultoperacionvisita,
CASE WHEN trim(comentario) = '' THEN NULL ELSE comentario END as ds_cc_comentario,
CASE WHEN trim(campania) = '' THEN NULL ELSE campania END as cod_cc_campania,
prioperacion	as cod_cc_prioperacion,
concat (SUBSTRING(fecha_prioperacion,0,4),'-', SUBSTRING(fecha_prioperacion,5,2),'-',SUBSTRING(fecha_prioperacion,7,2)) as dt_cc_prioperacion,
circuito as cod_cc_circuito,
cir.descri as ds_cc_circuito,
concat (SUBSTRING(rpt_fcambio,0,4),'-', SUBSTRING(rpt_fcambio,6,2),'-',SUBSTRING(rpt_fcambio,9,2),' ',SUBSTRING(rpt_fcambio,12,2),':', SUBSTRING(rpt_fcambio,15,2),':',SUBSTRING(rpt_fcambio,18,2))as ts_cc_rptcambio,
CASE WHEN trim(idjournal) = '' THEN NULL ELSE idjournal END as cod_cc_idjournal,
CASE WHEN oficial = 'null' THEN NULL ELSE oficial END as cod_cc_oficial,
CASE WHEN solicitud_portal = 'null' THEN NULL ELSE solicitud_portal END as cod_cc_solicitudportal,
CASE WHEN dependencia = 'null' THEN NULL ELSE dependencia END as cod_cc_dependencia,
sol.partition_date


from bi_corp_staging.rio3_solicitud sol
left join bi_corp_staging.rio3_circuito cir on (cir.codigo=sol.circuito and cir.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}')
left join  bi_corp_staging.rio3_tipoestadosolicitud tip on (trim(tip.codigo)=trim(sol.estado) and tip.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}')
left join bi_corp_staging.rio3_canal ca on (ca.codigo=sol.canal and ca.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}')

Where sol.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}';


