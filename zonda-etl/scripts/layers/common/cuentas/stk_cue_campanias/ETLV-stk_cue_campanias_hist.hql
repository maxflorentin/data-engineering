set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS temp_campanias_aux;
CREATE TEMPORARY TABLE temp_campanias_aux
AS
 SELECT
 cac.cac_entidad,
 cac.cac_cuenta,
 cac.cac_centro_alta,
 cac.cac_divisa,
 cac.cac_fecha_desde,
 cac.cac_fecha_hasta,
 cac.cac_campania,
 cac.partition_date
 FROM bi_corp_staging.malbgc_bgdtcac cac
 WHERE cac.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Campania-Hist') }}'
 AND '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Campania-Hist') }}' >= cac.cac_fecha_desde
 AND '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Campania-Hist') }}' <= cac.cac_fecha_hasta;


INSERT OVERWRITE TABLE bi_corp_common.stk_cue_campanias
PARTITION(partition_date)
SELECT
DISTINCT
cac.cac_entidad as cod_cue_entidad,
cac.cac_cuenta as cod_cue_cuenta,
cac.cac_centro_alta as cod_suc_sucursal,
cac.cac_fecha_desde as dt_cue_fecha_desde,
cac.cac_fecha_hasta as dt_cue_fecha_hasta,
cac.cac_campania as cod_cue_campania,
cac.cac_divisa as cod_cue_divisa,
IF(cam.cam_plan='', NULL, cam.cam_plan)  as cod_cue_plan,
IF(cam.cam_producto='', NULL, cam.cam_producto) as cod_cue_producto,
IF(cam.cam_subprodu='', NULL, cam.cam_subprodu)  as cod_cue_subproducto,
IF(cam.cam_plazo='', NULL, cam.cam_plazo) as int_cue_plazo,
cac.partition_date as partition_date

FROM temp_campanias_aux cac

LEFT JOIN bi_corp_staging.malbgc_bgdtcam cam
ON cac.CAC_CAMPANIA = cam.cam_campania
and cac.partition_date = cam.partition_date