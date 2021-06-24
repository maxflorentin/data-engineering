set mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT  overwrite TABLE bi_corp_common.stk_cc_sivdcampania PARTITION (partition_date)

Select
DISTINCT codigo 	as cod_cc_campania,
descri	as ds_cc_campania,
CASE WHEN activo = 'S' THEN 1 ELSE 0 END as flag_cc_activo,
CASE WHEN agenda = 'S' THEN 1 ELSE 0 END as flag_cc_agenda,
CASE WHEN producto ='    ' THEN NULL ELSE producto END as cod_cc_producto,
CASE WHEN appsource =' ' THEN NULL ELSE appsource END as cod_cc_appsource,
cast (cantcontactos as bigint) as fc_cc_cantcontactos,
prioridad as ds_cc_prioridad,
cast (cantoperaciones as bigint) as fc_cc_cantoperaciones,

CASE WHEN fecdesde = 'null' THEN NULL ELSE concat (SUBSTRING(fecdesde,0,4),'-', SUBSTRING(fecdesde,6,2),'-',SUBSTRING(fecdesde,9,2),' ',SUBSTRING(fecdesde,12,2),':', SUBSTRING(fecdesde,15,2),':',SUBSTRING(fecdesde,18,2)) END as ts_cc_desde,
CASE WHEN fechasta = 'null' THEN NULL ELSE concat (SUBSTRING(fechasta,0,4),'-', SUBSTRING(fechasta,6,2),'-',SUBSTRING(fechasta,9,2),' ',SUBSTRING(fechasta,12,2),':', SUBSTRING(fechasta,15,2),':',SUBSTRING(fechasta,18,2)) END as ts_cc_hasta,
CASE WHEN canal =' ' THEN NULL ELSE canal END as cod_cc_canal,
CASE WHEN fecdesde_crm = 'null' THEN NULL ELSE concat (SUBSTRING(fecdesde_crm,0,4),'-', SUBSTRING(fecdesde_crm,6,2),'-',SUBSTRING(fecdesde_crm,9,2),' ',SUBSTRING(fecdesde_crm,12,2),':', SUBSTRING(fecdesde_crm,15,2),':',SUBSTRING(fecdesde_crm,18,2)) END as ts_cc_desdecrm,
id_camp_buc	as cod_cc_campbuc,

CASE WHEN tipo_campania = 'null' THEN NULL ELSE tipo_campania END as cod_cc_tipocampania,
cast( CASE WHEN cant_op_agente = 'null' THEN NULL ELSE cant_op_agente END as bigint) as fc_cc_cantopagente,
CASE WHEN nroshot = 'null' THEN NULL ELSE nroshot END as cod_cc_nroshot,
CASE WHEN modelo_contacto = 'null' THEN NULL ELSE modelo_contacto END as cod_cc_modelocontacto,
modalidad_outxorigen as ds_cc_modalidadoutxorigen,
CASE WHEN fec_pri_vigencia = 'null' THEN NULL ELSE concat (SUBSTRING(fec_pri_vigencia,0,4),'-', SUBSTRING(fec_pri_vigencia,6,2),'-',SUBSTRING(fec_pri_vigencia,9,2),' ',SUBSTRING(fec_pri_vigencia,12,2),':', SUBSTRING(fec_pri_vigencia,15,2),':',SUBSTRING(fec_pri_vigencia,18,2)) END as ts_cc_privigencia,
CASE WHEN usuario_modif = 'null' THEN NULL ELSE usuario_modif END as cod_cc_legusuariomodif,
CASE WHEN fecha_modif = 'null' THEN NULL ELSE concat (SUBSTRING(fecha_modif,0,4),'-', SUBSTRING(fecha_modif,6,2),'-',SUBSTRING(fecha_modif,9,2),' ',SUBSTRING(fecha_modif,12,2),':', SUBSTRING(fecha_modif,15,2),':',SUBSTRING(fecha_modif,18,2)) END as ts_cc_modificacion,
pais	as cod_cc_pais,
cast (porcentaje_cont_contactar as bigint) as fc_cc_porccontactar,
CASE WHEN setgestion = 'null' THEN NULL ELSE setgestion END as cod_cc_setgestion,
CASE WHEN listallamado = 'null' THEN NULL ELSE listallamado END as cod_cc_listallamado,
CASE WHEN fec_ult_vigencia = 'null' THEN NULL ELSE concat (SUBSTRING(fec_ult_vigencia,0,4),'-', SUBSTRING(fec_ult_vigencia,6,2),'-',SUBSTRING(fec_ult_vigencia,9,2),' ',SUBSTRING(fec_ult_vigencia,12,2),':', SUBSTRING(fec_ult_vigencia,15,2),':',SUBSTRING(fec_ult_vigencia,18,2)) END as ts_cc_ultvigencia,
CASE WHEN blanquear_origen_discador = 'S' THEN 1 ELSE 0 END as flag_cc_blanquearorigendiscador,
partition_date
FROM bi_corp_staging.rio3_origen
Where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}';
