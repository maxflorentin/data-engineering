SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT  overwrite TABLE bi_corp_common.stk_cc_sivdcontacto PARTITION (partition_date)

Select
DISTINCT contacto 	as cod_cc_contacto,
tipodoc 	as cod_cc_tipodoc,
CAST(nrodoc  AS BIGINT)    as ds_per_numdoc,
expedida	as cod_cc_expedida,
concat (SUBSTRING(nacio,0,4),'-', SUBSTRING(nacio,5,2),'-',SUBSTRING(nacio,7,2)) as  cod_cc_nacio,
concat (SUBSTRING(fechaalta,0,4),'-', SUBSTRING(fechaalta,5,2),'-',SUBSTRING(fechaalta,7,2))as dt_cc_alta,
apellido	as ds_cc_apellido,
nombre		as ds_cc_nombre,
CASE WHEN teledisc1 =' ' THEN NULL ELSE teledisc1 END as ds_cc_teledisc1,
CASE WHEN tel_1 =' ' THEN NULL ELSE tel_1 END as ds_cc_tel1,
CASE WHEN postal1 =' ' THEN NULL ELSE postal1 END as ds_cc_postal1,
CASE WHEN teledisc2 =' ' THEN NULL ELSE teledisc2 END as ds_cc_teledisc2,
CASE WHEN tel_2 =' ' THEN NULL ELSE tel_2 END as ds_cc_tel2,
CASE WHEN postal2 =' ' THEN NULL ELSE postal2 END as ds_cc_postal2,
CASE WHEN interno ='          ' THEN NULL ELSE interno END as ds_cc_interno,
origen		as cod_cc_campania,
medio		as cod_cc_medio,
estado		as cod_cc_estado,
usuario		as cod_cc_legajo,
CASE WHEN clifondos =' ' THEN NULL ELSE clifondos END as ds_cc_clifondos,
CASE WHEN nivclifon =' ' THEN NULL ELSE nivclifon END as ds_cc_nivclifon,
CASE WHEN cliente ='S' THEN 1 ELSE 0 END as flag_cc_unicaoperacion,
cast( CASE WHEN sucursal ='   ' THEN NULL ELSE sucursal END as int) as cod_cc_sucursal,
cast(trim(clavehost) as bigint)   as cod_per_nup,
CASE WHEN conytipodoc ='   ' THEN NULL ELSE conytipodoc END as cod_cc_conytipodoc,
CASE WHEN conynrodoc =' ' THEN NULL ELSE conynrodoc END as cod_cc_conynrodoc,
CASE WHEN capturado ='null' THEN NULL ELSE capturado END as ds_cc_capturado,
CASE WHEN incontactable =' ' THEN NULL ELSE incontactable END as ds_cc_incontactable,
cantoperac as fc_cc_cantoperac,
ultoperacion as cod_cc_ultoperacion,
tipo_persona	as cod_cc_tipopersona,
tipo_cliente	as cod_cc_tipocliente,
CASE WHEN estado2 ='null' THEN NULL ELSE estado2 END as cod_cc_estado2,
partition_date
from bi_corp_staging.rio3_contacto
Where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}';
