
-----------------------------------------------------------------------------------------

-- cd_escenario --> No esta clara la utilización de este campo.
-- cd_gestion --> Hacer la busqueda sobre el CAMPO1  de la tabla tbl_combo_iatx_tmk para ver las descripciones
-- cd_resultado --> Resultado de la gestion CAMPO CODIGO  de la tabla tbl_combo_iatx_tmk.
-- cd_identificacion_resultado --> Resultado de la gestion, CAMPO3   de la tabla tbl_combo_iatx_tmk.



ALTER TABLE bi_corp_common.bt_suc_interaccionproducto 
ADD COLUMNS (ds_suc_gestion  STRING) cascade ;


SET hive.execution.engine = spark;
SET spark.yarn.queue = root.dataeng;
SET hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.max.dynamic.partitions=1300;
set hive.exec.max.dynamic.partitions.pernode=1300;

INSERT 	overwrite TABLE bi_corp_common.bt_suc_interaccionproducto PARTITION (partition_date)

SELECT DISTINCT TRIM(IP.cd_interaccion_producto) cod_suc_interaccionproducto
	, TRIM(IP.cd_interaccion) cod_suc_interaccion
	, CAST(IP.id_acc AS INT) cod_suc_acc
	, TRIM(IP.ds_campana_o_producto) ds_suc_campanaoproducto 
	, IF(TRIM(IP.mc_campana) = 'S',1,0) flag_suc_campana
	, CAST(IP.cd_escenario AS INT) cod_suc_escenario
	, CAST(IP.cd_envio AS INT) cod_suc_envio
	, TRIM(IP.ds_producto_crm) ds_suc_productocrm
	, CAST(IP.cd_gestion AS INT) cod_suc_gestion
	, CAST(IP.cd_resultado AS INT) cod_suc_resultado
	, TRIM(IP.json) ds_suc_json
	, TRIM(IP.ds_comentario) ds_suc_comentario
	, IF(TRIM(IP.mc_envio_mail) = 'S',1,0) flag_suc_enviomail
	, to_date(IP.dt_agenda_horario) dt_suc_agendahorario
	, SUBSTRING(IP.dt_agenda_horario, 1, 19) ts_suc_agendahorario
	, to_date(IP.dt_modificacion) dt_suc_modificacion
	, SUBSTRING(IP.dt_modificacion, 1, 19) ts_suc_modificacion
	, to_date(IP.dt_gestion) dt_suc_gestion
	, SUBSTRING(IP.dt_gestion, 1, 19) ts_suc_gestion
	, CAST(IP.cd_identificacion_resultado AS INT) cod_suc_identificacionresultado
	, TRIM(IP.cd_producto_crm) cod_suc_productocrm
	, CAST(IP.vl_nro_solicitud AS INT) cod_suc_vlnumerosolicitud
	, TRIM(IP.cd_canal_solicitud) cod_suc_canalsolicitud
	, TRIM(IP.id_camp_buc) cod_suc_campbuc
	, TRIM(IP.ds_contacto) ds_suc_contacto
	, IF(TRIM(IP.seguimientotec) = 'S',1,0) flag_suc_seguimientotec
	, TRIM(UPPER(GE.descripcion)) ds_suc_gestion 
	, IP.partition_date
FROM bi_corp_staging.rio151_tbl_interaccion_producto IP
LEFT JOIN bi_corp_staging.rio151_tbl_combo_iatx_tmk GE 
	ON TRIM(GE.campo1) = TRIM(IP.cd_gestion) 
	AND GE.mc_activo = 'S' 
	AND GE.codigo IS NOT NULL
	AND GE.partition_date = '2021-03-11' ;



SELECT DISTINCT cod_suc_gestion FROM bi_corp_common.bt_suc_interaccionproducto
WHERE partition_date = '2021-03-10'
-- NULL
-- 2
-- 10
-- 13
-- 15
-- 16
-- 38
-- 78
-- 279
-- 301
-- 340
-- 722
-- 733
-- 734
-- 866
-- 890
-- 891
-- 893
-- 985



select c.codigo , c.descripcion , g.descri 
from bi_corp_staging.rio151_tbl_combo_iatx_tmk c
left join bi_corp_staging.rio3_gestion  g 
ON c.codigo = g.codigo 
AND c.partition_date  = g.partition_date 
where partition_date = '2021-03-11' 
--AND TRIM(descripcion) = 'INGRESADA A SCORING'

SELECT * FROM bi_corp_staging.rio56_tipo_gestion 


select
	*
from
	bi_corp_staging.rio151_tbl_combo_iatx_tmk
where
	partition_date = '2021-03-11'
	and mc_activo = 'S'
	and TRIM(campo1) = '78'

select distinct grupo from bi_corp_staging.rio151_tbl_combo_iatx_tmk where grupo like '%GEST%'

--ESTADO GESTION AVISOS
--ESTADO GESTION MULTICANALIDAD
--ESTADO GESTION TUBI
--ESTADO GESTION CAMPANA
--MOTIVOS GESTION ALERTAS
--ESTADO GESTION OPERATIVA
--ESTADO GESTION SATISFACCION
--ESTADO GESTION NO CONTACTE
--RESULTADO GESTION TK


-- WHERE IP.partition_date = '2021-03-10'



