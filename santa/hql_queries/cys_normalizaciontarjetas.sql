

SELECT * FROM bi_corp_staging.rio30_transacciones 


--__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/


SELECT *
FROM santander_business_risk_arda.RELACION_CIERRE_SUCURSAL 
WHERE data_date_part = '20201130' LIMIT 10 ;

SELECT * FROM santander_business_risk_arda.saldos_altair_mes_actual LIMIT 2 ;


SHOW PARTITIONS santander_business_risk_arda.saldos_altair_mes_anterior

SHOW PARTITIONS santander_business_risk_arda.saldos_altair_mes_actual


--__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/


SELECT cod_bcaaut_sigla
	, ds_bcaaut_tipoequipo
	, cod_bcaaut_sucursal
	, ds_bcaaut_sucursal
	, cod_bcaaut_zona
	, ds_bcaaut_operador
	, ds_bcaaut_tiposucursal
	, dec_bcaaut_disponibilidad
	, dec_bcaaut_indisponibilidad
	, dec_bcaaut_harddefault
	, dec_bcaaut_supplyout
	, dec_bcaaut_cashout
	, dec_bcaaut_comunicacion
	, dec_bcaaut_banelco
	, dec_bcaaut_balanceo
	, dec_bcaaut_supervisor
	, dec_bcaaut_vandalismo
	, dec_bcaaut_logistica
	, dec_bcaaut_corteenergia
	, dec_bcaaut_dispensador
	, dec_bcaaut_modulodeposito
	, dec_bcaaut_modulocheques
FROM -- tabla dispo


-- SHOW PARTITIONS bi_corp_staging.rio44_ba_equipos_dispo_atm_men_tb 
-- SHOW PARTITIONS bi_corp_staging.rio44_ba_disponibilidad_atm
-- SHOW PARTITIONS bi_corp_staging.rio44_ba_equipos_dispo_tas_cerrado
-- SHOW PARTITIONS bi_corp_staging.rio44_ba_disponibilidad_tas
-- SHOW PARTITIONS bi_corp_staging.rio44_ba_disponibilidad_mensual


SELECT * FROM bi_corp_staging.rio44_ba_disponibilidad_atm 

SELECT DISTINCT g.ide_gestion_nro, g.partition_date, g.fec_bandeja_actual, g.ide_gestion_sector, g.cod_bandeja_actual, e.cod_bandeja, c.tpo_gestion
FROM bi_corp_staging.rio56_gestiones g
inner join bi_corp_staging.rio56_circuito c
on (g.ide_circuito = c.ide_circuito)
AND c.partition_date >= g.partition_date 
LEFT JOIN bi_corp_staging.rio56_gestion_estados e 
ON TRIM(e.ide_gestion_nro) = TRIM(g.ide_gestion_nro) 
AND e.partition_date >= g.partition_date 
where
(g.partition_date > '2020-09-00' and g.partition_date <= '2020-11-30')
AND g.ide_gestion_nro
in (
    '19432192',
    '19451611',
    '19453770',
    '19455541',
    '19456928',
    '19456965',
    '19466421',
    '19466960',
    '19485304')
--and SUBSTRING(fec_bandeja_actual, 1,7) in ('2020-10')
--and SUBSTRING(fec_bandeja_actual, 1,7) in ('2020-10','2020-09')
--and g.ide_gestion_sector in ('SCLI')
--and cod_bandeja_actual in ('Cerrado', 'Asignado para Resolver')
--and cod_bandeja_actual in ('Cerrado', 'Asignado para Resolver')
--and c.tpo_gestion in ('1','2')
order by 1 asc


SELECT * FROM bi_corp_staging.rio56_gestion_estados LIMIT 19 ;



SELECT *
FROM bi_corp_staging.nesr_encolador_det_ticket 
WHERE partition_date = '2020-12-28' LIMIT 50 ;
AND nom_motivo LIKE '%�%' 
GROUP BY nom_motivo ;

SHOW PARTITIONS bi_corp_staging.garra_log_cuotaphone ;

CREATE TEMPORARY TABLE cuotaphone AS
SELECT CAST(gitccuo_num_persona AS BIGINT) cod_per_nup 	-- int 
	, TRIM(gitccuo_cod_entigen) cod_tcr_entidad 	-- int 
	, CAST(gitccuo_cod_centro AS INT) cod_suc_sucursal 	-- int 
	, CAST(gitccuo_num_contrato AS BIGINT) cod_tcr_cuenta 	-- bigint
	, TRIM(gitccuo_cod_producto) cod_tcr_producto 	-- string
	, TRIM(gitccuo_cod_subprodu) cod_tcr_subproducto 	-- string
	, TRIM(gitccuo_cod_divisa) cod_tcr_divisa 	-- string
	, CAST(gitccuo_num_secuencia AS INT)  int_tcr_secuencia 	-- int 
	, TRIM(gitccuo_fec_cuotaphone) dt_tcr_cuotaphone 	-- date 
	, CAST(gitccuo_cuenta_visa AS BIGINT) cod_tcr_cuentabase 	-- bigint
	, TRIM(gitccuo_cod_marca_ini) cod_tcr_marcainicial 	-- string
	, TRIM(gitccuo_cod_submarca_ini) cod_tcr_submarcainicial 	-- string
	, TRIM(gitccuo_cod_marca_seg_ini) cod_tcr_marcasegini 	-- string
	, TRIM(gitccuo_tip_reestruct_ini) cod_tcr_tiporeestructuracionini 	-- string
	, TRIM(gitccuo_cod_marca_act) cod_tcr_marcaactual 	-- string
	, TRIM(gitccuo_cod_submarca_act) cod_tcr_submarcaactual 	-- string
	, TRIM(gitccuo_fec_cambio_seg) dt_tcr_cambioseg 	-- date 
	, TRIM(gitccuo_cod_marca_seg_act) cod_tcr_marcasegact 	-- string 
	, TRIM(gitccuo_tip_reestruct_act) cod_tcr_tiporeestructuracionact 	-- string
	, TRIM(gitccuo_fec_cura) dt_tcr_cura 	-- date 
	, IF(TRIM(gitccuo_fec_empeora) = '9999-12-31', NULL, TRIM(gitccuo_fec_empeora)) dt_tcr_empeora 	-- date 
	, IF(TRIM(gitccuo_fec_normaliza) = '9999-12-31', NULL, TRIM(gitccuo_fec_normaliza)) dt_tcr_normaliza 	-- date 
	, TRIM(gitccuo_motivo_cambio) ds_tcr_motivocambio 	-- string
	, CAST(gitccuo_num_ree AS INT) int_tcr_ree 	-- int 
	, SUBSTRING(gitccuo_timest_umo, 1, 10) dt_tcr_carga 	-- date 
	, partition_date 	-- date
FROM bi_corp_staging.garra_log_cuotaphone 
WHERE partition_date = '2021-01-04' LIMIT 100 ;

DESCRIBE cuotaphone ;



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_tcr_normalizaciontarjetas (
	cod_per_nup	bigint ,
	cod_tcr_entidad	string ,
	cod_suc_sucursal	int ,
	cod_tcr_cuenta	bigint ,
	cod_tcr_producto	string ,
	cod_tcr_subproducto	string ,
	cod_tcr_divisa	string ,
	int_tcr_secuencia	int ,
	dt_tcr_cuotaphone	string ,
	cod_tcr_cuentabase	bigint ,
	cod_tcr_marcainicial	string ,
	cod_tcr_submarcainicial	string ,
	cod_tcr_marcasegini	string ,
	cod_tcr_tiporeestructuracionini	string ,
	cod_tcr_marcaactual	string ,
	cod_tcr_submarcaactual	string ,
	dt_tcr_cambioseg	string ,
	cod_tcr_marcasegact	string ,
	cod_tcr_tiporeestructuracionact	string ,
	dt_tcr_cura	string ,
	dt_tcr_empeora	string ,
	dt_tcr_normaliza	string ,
	ds_tcr_motivocambio	string ,
	int_tcr_ree	int ,
	dt_tcr_carga	string )
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/santander/bi-corp/common/cys/stk_tcr_normalizaciontarjetas' ;
	 
set hive.execution.engine=spark;
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;
-----------------------------------------------
INSERT OVERWRITE TABLE bi_corp_common.stk_tcr_normalizaciontarjetas
PARTITION(partition_date)
SELECT CAST(gitccuo_num_persona AS BIGINT) cod_per_nup 	 
	, TRIM(gitccuo_cod_entigen) cod_tcr_entidad 	
	, CAST(gitccuo_cod_centro AS INT) cod_suc_sucursal 	 
	, CAST(gitccuo_num_contrato AS BIGINT) cod_tcr_cuenta 	
	, TRIM(gitccuo_cod_producto) cod_tcr_producto 	
	, TRIM(gitccuo_cod_subprodu) cod_tcr_subproducto 	
	, TRIM(gitccuo_cod_divisa) cod_tcr_divisa 	
	, CAST(gitccuo_num_secuencia AS INT)  int_tcr_secuencia 	 
	, TRIM(gitccuo_fec_cuotaphone) dt_tcr_cuotaphone 	 
	, CAST(gitccuo_cuenta_visa AS BIGINT) cod_tcr_cuentabase 	
	, TRIM(gitccuo_cod_marca_ini) cod_tcr_marcainicial 	
	, TRIM(gitccuo_cod_submarca_ini) cod_tcr_submarcainicial 	
	, TRIM(gitccuo_cod_marca_seg_ini) cod_tcr_marcasegini 	
	, TRIM(gitccuo_tip_reestruct_ini) cod_tcr_tiporeestructuracionini 	
	, TRIM(gitccuo_cod_marca_act) cod_tcr_marcaactual 	
	, TRIM(gitccuo_cod_submarca_act) cod_tcr_submarcaactual 	
	, TRIM(gitccuo_fec_cambio_seg) dt_tcr_cambioseg 	 
	, TRIM(gitccuo_cod_marca_seg_act) cod_tcr_marcasegact 	 
	, TRIM(gitccuo_tip_reestruct_act) cod_tcr_tiporeestructuracionact 	
	, TRIM(gitccuo_fec_cura) dt_tcr_cura 	 
	, IF(TRIM(gitccuo_fec_empeora) = '9999-12-31', NULL, TRIM(gitccuo_fec_empeora)) dt_tcr_empeora 	 
	, IF(TRIM(gitccuo_fec_normaliza) = '9999-12-31', NULL, TRIM(gitccuo_fec_normaliza)) dt_tcr_normaliza 	 
	, TRIM(gitccuo_motivo_cambio) ds_tcr_motivocambio 	
	, CAST(gitccuo_num_ree AS INT) int_tcr_ree 	 
	, SUBSTRING(gitccuo_timest_umo, 1, 10) dt_tcr_carga 	 
	, partition_date 	
FROM bi_corp_staging.garra_log_cuotaphone 
WHERE partition_date = '2020-01-18' ;

SELECT * FROM bi_corp_common.stk_tcr_normalizaciontarjetas LIMIT 100 ;

SELECT * FROM bi_corp_common.stk_tcr_normalizaciontarjetas WHERE partition_date='2021-01-06' LIMIT 50 ;

SELECT * FROM bi_corp_common.stk_cys_mapaindividuosnegocios WHERE partition_date='2020-11-30' LIMIT 50 ;

SELECT * FROM bi_corp_common.stk_cys_mapapymes WHERE partition_date='2020-11-30' LIMIT 50 ;

SHOW PARTITIONS bi_corp_common.stk_tcr_normalizaciontarjetas ;

SHOW PARTITIONS bi_corp_common.stk_cys_mapapymes ;

SHOW PARTITIONS bi_corp_common.stk_cys_mapaindividuosnegocios ;




DESCRIBE bi_corp_staging.nesr_encolador_d ;

CREATE TEMPORARY TABLE encolador AS
SELECT TRIM(ED.id_tkt) cod_suc_ticket
	, CAST(ED.id_suc AS INT) cod_suc_sucural
	, UPPER(TRIM(ED.sector_espera)) ds_suc_sector
	, TRIM(ED.apellidocliente) ds_per_apellido
	, TRIM(ED.nombrecliente) ds_per_nombre
	, TRIM(ED.cod_producto) cod_suc_producto
	, TRIM(ED.cod_motivo) cod_suc_motivovisita
	, TRIM(ED.mejorproducto) ds_suc_mejorproducto
	, TRIM(ED.segmento) cod_suc_segmento
	, TRIM(ED.semafororentabilidad) cod_suc_semafororentabilidad
	, TRIM(ED.semafororenta) cod_suc_semafororenta
	, TRIM(ED.productosegmento) cod_suc_productosegmento
	, TRIM(ED.contacto) cod_suc_contacto
	, DS.ds_tipodoc ds_per_tipodoc
	, CAST(ED.nrodoc AS BIGINT) ds_per_numdoc
	, TRIM(ED.subsegmento) cod_suc_subsegmento
	, CAST(ED.nup AS BIGINT) cod_per_nup
	, from_unixtime(unix_timestamp(TRIM(ED.fechaticket), 'yyyyMMddHHmmss'),"yyyy-MM-dd HH:mm:ss") ts_suc_ticket
	, TRIM(ED.campana) cod_suc_campana
	, CAST(ED.numero_servicio AS INT) int_suc_servicio
	, TRIM(ED.tipoatencion) cod_suc_tipoatencion
	, TRIM(ED.legajo) cod_suc_legajo
	, TRIM(ED.canalcomp) cod_suc_canalcomp
	, from_unixtime(unix_timestamp(TRIM(ED.fecha_atencion), 'yyyyMMddHHmmss'),"yyyy-MM-dd HH:mm:ss") ts_suc_atencion
	, TRIM(ED.oficial_atencion) cod_suc_oficialatencion
	, TRIM(ED.id_gestion) cod_suc_gestion
	, from_unixtime(unix_timestamp(TRIM(ED.fecha_proceso), 'yyyyMMddHHmmss'),"yyyy-MM-dd HH:mm:ss") ts_suc_proceso
	, TRIM(ED.procesado) cod_suc_procesado
	, from_unixtime(unix_timestamp(TRIM(ED.fecha_fin_atencion), 'yyyyMMddHHmmss'),"yyyy-MM-dd HH:mm:ss") ts_suc_finatencion
	, TRIM(ED.nombre_servicio) ds_suc_servicio
	, TRIM(UPPER(ED.motivo_enc)) ds_suc_motivoenc 
	, TRIM(ED.oficial_nombre) ds_suc_oficialnombre
	, TRIM(ED.origen) cod_suc_origen
	, TRIM(ED.estado) ds_suc_estado
	, TRIM(UPPER(ED.motivo_abandono)) ds_suc_motivoabandono
	, CAST(ED.tecn AS BIGINT) int_suc_tecn
	, ED.partition_date 
FROM bi_corp_staging.nesr_encolador_d ED
LEFT JOIN 
	(SELECT DISTINCT TRIM(substr(gen_clave, 1, 1)) tipodoc, gen_datos ds_tipodoc
	 FROM bi_corp_staging.tcdtgen WHERE gentabla = '0113') DS
	ON TRIM(ED.tipodoc) = DS.tipodoc 
WHERE partition_date = '2020-12-28' 
LIMIT 20 ;

DESCRIBE encolador ;

-- DROP TABLE bi_corp_common.bt_suc_direccionador

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_suc_direccionador ( 
	cod_suc_ticket	string ,
	cod_suc_sucursal	int ,
	ds_suc_sector	string ,
	ds_per_apellido	string ,
	ds_per_nombre	string ,
	cod_suc_producto	string ,
	cod_suc_motivovisita	string ,
	ds_suc_mejorproducto	string ,
	cod_suc_segmento	string ,
	cod_suc_semafororentabilidad	string ,
	cod_suc_semafororenta	string ,
	cod_suc_productosegmento	string ,
	cod_suc_contacto	string ,
	ds_per_tipodoc	string ,
	ds_per_numdoc	bigint ,
	cod_suc_subsegmento	string ,
	cod_per_nup	bigint ,
	ts_suc_ticket	string ,
	cod_suc_campana	string ,
	int_suc_servicio	int ,
	cod_suc_tipoatencion	string ,
	cod_suc_legajo	string ,
	cod_suc_canalcomp	string ,
	ts_suc_atencion	string ,
	cod_suc_oficialatencion	string ,
	cod_suc_gestion	string ,
	ts_suc_proceso	string ,
	cod_suc_procesado	string ,
	ts_suc_finatencion	string ,
	ds_suc_servicio	string ,
	ds_suc_motivoenc	string ,
	ds_suc_oficialnombre	string ,
	cod_suc_origen	string ,
	ds_suc_estado	string ,
	ds_suc_motivoabandono	string ,
	int_suc_tecn	bigint )
PARTITIONED BY ( partition_date string) 
STORED AS PARQUET 
LOCATION '/santander/bi-corp/common/interacciones_canales/sucursal/fact/bt_suc_direccionador'



SET hive.execution.engine = spark;
SET spark.yarn.queue = root.dataeng;
SET hive.exec.dynamic.partition.mode = nonstrict;
--------------------------------------------------
INSERT overwrite TABLE bi_corp_common.bt_suc_direccionador PARTITION (partition_date)
--------------------------------------------------
SELECT TRIM(ED.id_tkt) cod_suc_ticket
	, CAST(ED.id_suc AS INT) cod_suc_sucural
	, UPPER(TRIM(ED.sector_espera)) ds_suc_sector
	, TRIM(ED.apellidocliente) ds_per_apellido
	, TRIM(ED.nombrecliente) ds_per_nombre
	, TRIM(ED.cod_producto) cod_suc_producto
	, TRIM(ED.cod_motivo) cod_suc_motivovisita
	, TRIM(ED.mejorproducto) ds_suc_mejorproducto
	, TRIM(ED.segmento) cod_suc_segmento
	, TRIM(ED.semafororentabilidad) cod_suc_semafororentabilidad
	, TRIM(ED.semafororenta) cod_suc_semafororenta
	, TRIM(ED.productosegmento) cod_suc_productosegmento
	, TRIM(ED.contacto) cod_suc_contacto
	, DS.ds_tipodoc ds_per_tipodoc
	, CAST(ED.nrodoc AS BIGINT) ds_per_numdoc
	, TRIM(ED.subsegmento) cod_suc_subsegmento
	, CAST(ED.nup AS BIGINT) cod_per_nup
	, from_unixtime(unix_timestamp(TRIM(ED.fechaticket), 'yyyyMMddHHmmss'),"yyyy-MM-dd HH:mm:ss") ts_suc_ticket
	, TRIM(ED.campana) cod_suc_campana
	, CAST(ED.numero_servicio AS INT) int_suc_servicio
	, TRIM(ED.tipoatencion) cod_suc_tipoatencion
	, TRIM(ED.legajo) cod_suc_legajo
	, TRIM(ED.canalcomp) cod_suc_canalcomp
	, from_unixtime(unix_timestamp(TRIM(ED.fecha_atencion), 'yyyyMMddHHmmss'),"yyyy-MM-dd HH:mm:ss") ts_suc_atencion
	, TRIM(ED.oficial_atencion) cod_suc_oficialatencion
	, TRIM(ED.id_gestion) cod_suc_gestion
	, from_unixtime(unix_timestamp(TRIM(ED.fecha_proceso), 'yyyyMMddHHmmss'),"yyyy-MM-dd HH:mm:ss") ts_suc_proceso
	, TRIM(ED.procesado) cod_suc_procesado
	, from_unixtime(unix_timestamp(TRIM(ED.fecha_fin_atencion), 'yyyyMMddHHmmss'),"yyyy-MM-dd HH:mm:ss") ts_suc_finatencion
	, TRIM(ED.nombre_servicio) ds_suc_servicio
	, TRIM(UPPER(ED.motivo_enc)) ds_suc_motivoenc 
	, TRIM(ED.oficial_nombre) ds_suc_oficialnombre
	, TRIM(ED.origen) cod_suc_origen
	, TRIM(ED.estado) ds_suc_estado
	, TRIM(UPPER(ED.motivo_abandono)) ds_suc_motivoabandono
	, CAST(ED.tecn AS BIGINT) int_suc_tecn
	, ED.partition_date 
FROM bi_corp_staging.nesr_encolador_d ED
LEFT JOIN 
	(SELECT DISTINCT TRIM(substr(gen_clave, 1, 1)) tipodoc, gen_datos ds_tipodoc
	 FROM bi_corp_staging.tcdtgen WHERE gentabla = '0113') DS
	ON TRIM(ED.tipodoc) = DS.tipodoc 
WHERE partition_date = '2020-12-28' ;

'A253647 SERGIO, PAZ                               '

DESCRIBE bi_corp_staging.nesr_encolador_det_ticket ;

CREATE TEMPORARY TABLE sucu_det AS
SELECT from_unixtime(unix_timestamp(TRIM(ET.fecha), 'yyyyMMdd'),"yyyy-MM-dd") dt_suc_atencion
	, TRIM(ET.sucursal) cod_suc_sucursal
	, TRIM(ET.zona) cod_suc_zona
	, TRIM(UPPER(ET.tipo_cola)) ds_suc_tipocola
	, TRIM(ET.ticket) cod_suc_ticket
	, TRIM(ET.numero_servicio) int_suc_servicio
	, TRIM(ET.nombre_servicio) ds_suc_servicio
	, TRIM(ET.numero_puesto) int_suc_puesto
	, IF(TRIM(ET.nombre_puesto) = '', NULL, TRIM(ET.nombre_puesto)) ds_suc_puesto
	, IF(SUBSTRING(TRIM(ET.usuario), 8, 1) = ' ', SUBSTRING(TRIM(ET.usuario), 1, 7), NULL) cod_suc_legajo
	, IF(SUBSTRING(TRIM(ET.usuario), 8, 1) = ' ', SUBSTRING(TRIM(ET.usuario), 9, 42), NULL) ds_suc_nombreusuario
	, TRIM(ET.t_obj_espera) ts_suc_objespera
	, TRIM(ET.t_max_espera) ts_suc_maxespera
	, TRIM(ET.fecha_ingreso) ts_suc_ingreso
	, TRIM(ET.fecha_llamado) ts_suc_llamado 
	, TRIM(ET.fecha_fin_atencion) ts_suc_finatencion
	, TRIM(ET.tiempo_espera) ts_suc_espera
	, TRIM(ET.tiempo_atencion) ts_suc_tiempoatencion
	, IF(TRIM(ET.auxiliar1) = '', NULL, TRIM(ET.auxiliar1)) ds_suc_auxiliar1
	, IF(TRIM(ET.auxiliar2) = '', NULL, TRIM(ET.auxiliar2)) ds_suc_auxiliar2
	, IF(TRIM(ET.cod_motivo) = '', NULL, TRIM(ET.cod_motivo)) cod_suc_motivovisita
	, IF(TRIM(ET.nom_motivo) = '', NULL, TRIM(UPPER(ET.nom_motivo))) ds_suc_motivovisita
	, TRIM(ET.estado) cod_suc_estado
	, IF(TRIM(ET.motivo_abandono) = '', NULL, TRIM(ET.motivo_abandono)) ds_suc_motivoabandono
	, TRIM(ET.tecn) int_suc_tecn 
FROM bi_corp_staging.nesr_encolador_det_ticket ET 
WHERE partition_date = '2020-12-28' LIMIT 20 ;


DESCRIBE sucu_det ;

-- DROP TABLE bi_corp_common.bt_suc_detalleticket ;
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_suc_detalleticket ( 
	dt_suc_atencion	string ,
	cod_suc_sucursal	int ,
	cod_suc_zona	string ,
	ds_suc_tipocola	string ,
	cod_suc_ticket	string ,
	int_suc_servicio	int ,
	ds_suc_servicio	string ,
	int_suc_puesto	int ,
	ds_suc_puesto	string ,
	cod_suc_legajo	string ,
	ds_suc_nombreusuario	string ,
	ts_suc_objespera	string ,
	ts_suc_maxespera	string ,
	ts_suc_ingreso	string ,
	ts_suc_llamado	string ,
	ts_suc_finatencion	string ,
	ts_suc_espera	string ,
	ts_suc_tiempoatencion	string ,
	ds_suc_auxiliar1	string ,
	ds_suc_auxiliar2	string ,
	cod_suc_motivovisita	string ,
	ds_suc_motivovisita	string ,
	cod_suc_estado	string ,
	ds_suc_motivoabandono	string ,
	int_suc_tecn	bigint )
PARTITIONED BY ( partition_date string) 
STORED AS PARQUET 
LOCATION '/santander/bi-corp/common/interacciones_canales/sucursal/fact/bt_suc_detalleticket'


SET hive.execution.engine = spark;
SET spark.yarn.queue = root.dataeng;
SET hive.exec.dynamic.partition.mode = nonstrict;
--------------------------------------------------
INSERT overwrite TABLE bi_corp_common.bt_suc_detalleticket PARTITION (partition_date)
--------------------------------------------------
SELECT from_unixtime(unix_timestamp(TRIM(ET.fecha), 'yyyyMMdd'),"yyyy-MM-dd") dt_suc_atencion
	, TRIM(ET.sucursal) cod_suc_sucursal
	, TRIM(ET.zona) cod_suc_zona
	, TRIM(UPPER(ET.tipo_cola)) ds_suc_tipocola
	, TRIM(ET.ticket) cod_suc_ticket
	, TRIM(ET.numero_servicio) int_suc_servicio
	, TRIM(ET.nombre_servicio) ds_suc_servicio
	, TRIM(ET.numero_puesto) int_suc_puesto
	, IF(TRIM(ET.nombre_puesto) = '', NULL, TRIM(ET.nombre_puesto)) ds_suc_puesto
	, IF(SUBSTRING(TRIM(ET.usuario), 8, 1) = ' ', SUBSTRING(TRIM(ET.usuario), 1, 7), NULL) cod_suc_legajo
	, IF(SUBSTRING(TRIM(ET.usuario), 8, 1) = ' ', SUBSTRING(TRIM(ET.usuario), 9, 42), NULL) ds_suc_nombreusuario
	, TRIM(ET.t_obj_espera) ts_suc_objespera
	, TRIM(ET.t_max_espera) ts_suc_maxespera
	, TRIM(ET.fecha_ingreso) ts_suc_ingreso
	, TRIM(ET.fecha_llamado) ts_suc_llamado 
	, TRIM(ET.fecha_fin_atencion) ts_suc_finatencion
	, TRIM(ET.tiempo_espera) ts_suc_espera
	, TRIM(ET.tiempo_atencion) ts_suc_tiempoatencion
	, IF(TRIM(ET.auxiliar1) = '', NULL, TRIM(ET.auxiliar1)) ds_suc_auxiliar1
	, IF(TRIM(ET.auxiliar2) = '', NULL, TRIM(ET.auxiliar2)) ds_suc_auxiliar2
	, IF(TRIM(ET.cod_motivo) = '', NULL, TRIM(ET.cod_motivo)) cod_suc_motivovisita
	, IF(TRIM(ET.nom_motivo) = '', NULL, TRIM(UPPER(ET.nom_motivo))) ds_suc_motivovisita
	, TRIM(ET.estado) cod_suc_estado
	, IF(TRIM(ET.motivo_abandono) = '', NULL, TRIM(ET.motivo_abandono)) ds_suc_motivoabandono
	, TRIM(ET.tecn) int_suc_tecn 
	, ET.partition_date 
FROM bi_corp_staging.nesr_encolador_det_ticket ET 
WHERE ET.partition_date = '2020-12-28' ;



SELECT * FROM bi_corp_common.bt_suc_detalleticket WHERE partition_date = '2020-12-28' LIMIT 20 ;


SELECT * FROM bi_corp_common.bt_suc_direccionador WHERE partition_date = '2020-12-28' LIMIT 20 ;

-- SHOW PARTITIONS bi_corp_common.bt_suc_turnero ;

SELECT * FROM bi_corp_common.bt_suc_turnero 



-- 33.641.807
SELECT COUNT(1) FROM bi_corp_staging.afir_in00 ;

-- Maxi, además de la tabla con todos los registros y causales necesitamos que en “common” tengamos una en forma mensual con todos los registros con los causales 017, 018 y 019 
-- que son las gestiones de seguimiento de cartera de riesgos. La idea es tener en forma mensual esta tabla para ver si la fecha de rehabilitación ha cambiado por alguna gestión 
-- y con la otra tabla perderíamos este tracking de la novedad.
-- Rodrigo, para poder cargar la historia mensual hay forma de tener los datos históricos o si cambia el dato de la fecha de rehabilitación se pisa el dato y no se guarda ningún registro?

DESCRIBE bi_corp_staging.afir_in00 ;

CREATE TEMPORARY TABLE inh00 AS
SELECT CAST(INH.nro_inh AS BIGINT) cod_cys_inh
	, TRIM(INH.tpo_pers) cod_per_tipopersona 
	, TRIM(INH.ape_pers) ds_per_apellido
	, TRIM(INH.nom_pers) ds_per_nombre
	, TRIM(INH.cod_sex_pers) cod_per_sexo
	, TRIM(INH.fec_naci) dt_per_fechanacimiento
	, TRIM(DS.ds_tipodoc) ds_per_tipodoc
	, CAST(INH.nro_doc AS BIGINT) ds_per_numdoc
	, CAST(INH.cod_caus AS INT) cod_cys_causal
	, CAST(INH.sec_caus AS INT) int_cys_seccaus
	, TRIM(INH.fec_inh) dt_cys_inh
	, IF(TRIM(INH.fec_rehb) = '3011-11-27', NULL, TRIM(INH.fec_rehb)) dt_cys_rehb
	, IF(TRIM(INH.vigencia) = 'S', 1, 0) flag_cys_vigencia
	, last_day(to_date(INH.fec_inh)) partition_date
FROM bi_corp_staging.afir_in00 INH 
LEFT JOIN 
	(SELECT DISTINCT TRIM(substr(gen_clave, 1, 1)) tipodoc, gen_datos ds_tipodoc
	 FROM bi_corp_staging.tcdtgen WHERE gentabla = '0113') DS
	ON TRIM(INH.tpo_doc) = DS.tipodoc
LIMIT 10 ;


DESCRIBE inh00 ;

----------------------------------------------------------------------------------



SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.size.per.task=4000000;
SET hive.merge.smallfiles.avgsize=16000000;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_rcp_ventamoria 
PARTITION(partition_date) 

SELECT 
	CAST(idventa AS INT) idventa ,
	CAST(idfideicomiso AS INT) idfideicomiso ,
	CAST(idpreventa AS INT) idpreventa ,
	CAST(cotizacion AS INT) cotizacion ,
	TRIM(fecha_corte) fecha_corte ,
	IF(TRIM(ip) = '', NULL, TRIM(ip)) ip ,
	IF(TRIM(usuario) = '', NULL, TRIM(usuario)) usuario ,
	TRIM(estado) estado ,
	TRIM(fecha_preventa) fecha_preventa ,
	TRIM(clientes_agregados) clientes_agregados ,
	TRIM(clientes_excluidos) clientes_excluidos ,
	TRIM(fecha_venta) fecha_venta ,
	TRIM(usuario_alta) usuario_alta ,
	from_unixtime(unix_timestamp(timest_alta ,'dd-MM-yyyy HH:mm:ss'),'yyyy-MM-dd  HH:mm:ss') timest_alta , 
	TRIM(usuario_envio) usuario_envio ,
	from_unixtime(unix_timestamp(timest_envio ,'dd-MM-yyyy HH:mm:ss'),'yyyy-MM-dd  HH:mm:ss') timest_envio , 
	TRIM(usuario_aprobacion) usuario_aprobacion ,
	from_unixtime(unix_timestamp(timest_aprobacion ,'dd-MM-yyyy HH:mm:ss'),'yyyy-MM-dd  HH:mm:ss') timest_aprobacion , 
	IF(TRIM(motivo_rechazo) = '', NULL, TRIM(motivo_rechazo)) motivo_rechazo ,
	IF(TRIM(fec_ajuste1) = '', NULL, TRIM(fec_ajuste1)) fec_ajuste1 ,
	IF(TRIM(fec_ajuste2) = '', NULL, TRIM(fec_ajuste2)) fec_ajuste2 ,
	partition_date
FROM bi_corp_staging.moria_vc_ventas 




SELECT * FROM bi_corp_common.stk_rcp_ventamoria ;



--17/3/   2020-03-17
--26/7/   2019-07-26
--24/5/   2019-05-24
--20/12/  2018-12-20
--27/9/   2018-09-27
--26/7/   2018-09-26

SELECT * FROM bi_corp_staging.moria_vc_ventas ;


2019-07-26
2019-05-24
2020-03-20
2019-05-24
2019-07-26
--2020-03-20



-- SHOW PARTITIONS bi_corp_common.stk_tcr_normalizaciontarjetas

-- SHOW PARTITIONS bi_corp_common.bt_ren_blce_result ;
-- DROP TABLE bi_corp_common.bt_ren_blce_result ;


SELECT * FROM bi_corp_staging.afir_in00 
WHERE partition_date = '2021-01-18'


set hive.execution.engine=spark;
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;
-----------------------------------------------
INSERT OVERWRITE TABLE bi_corp_common.stk_cys_inhabilitados
PARTITION(partition_date)
SELECT * FROM (
SELECT CAST(INH.nro_inh AS BIGINT) cod_cys_inh
	, TRIM(INH.tpo_pers) cod_per_tipopersona 
	, TRIM(INH.ape_pers) ds_per_apellido
	, TRIM(INH.nom_pers) ds_per_nombre
	, TRIM(INH.cod_sex_pers) cod_per_sexo
	, TRIM(INH.fec_naci) dt_per_fechanacimiento
	, TRIM(DS.ds_tipodoc) ds_per_tipodoc
	, CAST(INH.nro_doc AS BIGINT) ds_per_numdoc
	, CAST(INH.cod_caus AS INT) cod_cys_causal
	, CAST(INH.sec_caus AS INT) int_cys_seccaus
	, TRIM(INH.fec_inh) dt_cys_inh
	, IF(TRIM(INH.fec_rehb) = '3011-11-27', NULL, TRIM(INH.fec_rehb)) dt_cys_rehb
	, IF(TRIM(INH.vigencia) = 'S', 1, 0) flag_cys_vigencia
	, last_day(to_date(INH.fec_inh)) partition_date
FROM bi_corp_staging.afir_in00 INH 
LEFT JOIN 
	(SELECT DISTINCT TRIM(substr(gen_clave, 1, 1)) tipodoc, gen_datos ds_tipodoc
	 FROM bi_corp_staging.tcdtgen WHERE gentabla = '0113') DS
	ON TRIM(INH.tpo_doc) = DS.tipodoc 
WHERE partition_date = '2021-01-26' 
) A ;

DESCRIBE inh ;

SHOW PARTITIONS bi_corp_common.stk_cys_inhabilitados ;

SHOW PARTITIONS bi_corp_staging.afir_in00 ;

set hive.execution.engine=spark;
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;
-----------------------------------------------
INSERT OVERWRITE TABLE bi_corp_common.stk_cys_inhabilitados
PARTITION(partition_date)
--------------------------------------------
SELECT * FROM inh ;

SELECT * FROM bi_corp_staging.afir_in00 WHERE TRIM(nro_inh) = '818306'

-- 00818306; 					-- nro_inh
-- J; 							-- tpo_pers
-- IRALDI JORGE A.;				-- ape_pers
-- ; 							-- nom_pers 
-- IRALDI ROBERTO;				-- 
-- IRALDI G;         			--                                
-- ;							-- 
--  ;							-- cod_sex_pers
-- 0001-01-01;					-- fec_naci
-- T ;							-- tpo_doc
-- 30709499471;					-- nro_doc
-- 072;							-- cod_caus
-- 001;							-- sec_caus
-- 2009-01-31;					-- fec_inh
-- 2011-01-31;					-- fec_rehb
-- N							-- vigencia

SELECT * FROM bi_corp_common.stk_cys_inhabilitados ;

SELECT partition_date , COUNT(1) num_doc_null
FROM bi_corp_staging.rio5_clientes WHERE nro_documento = 'null' AND nro_ejecucion = '2'
GROUP BY partition_date ;

-- SHOW PARTITIONS bi_corp_staging.rio5_clientes ;
-- DROP TABLE bi_corp_common.stk_cys_discrepanciasasdf


CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cys_discrepanciasasdf (
  int_cys_ejecucion int,
  dt_cys_periodo int,
  cod_per_nup bigint,
  ds_per_numdoc bigint,
  ds_cys_razonsocial string,
  ds_cys_clasificacionedfsindiscrepancia string,
  ds_cys_clasificaciondiscrepancia string,
  ds_clasificacionedf string,
  fc_cys_deudatotal decimal(17,4),
  fc_cys_previsiontotalsindiscrepancia decimal(17,4),
  fc_previsiontotal decimal(17,4)
)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/santander/bi-corp/common/recuperaciones/stk_cys_discrepanciasasdf' ;

set hive.execution.engine=spark;
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;
-----------------------------------------------
INSERT OVERWRITE TABLE bi_corp_common.stk_cys_discrepanciasasdf
PARTITION(partition_date) 
SELECT  CAST(nro_ejecucion AS INT) nro_ejecucion 
	, CAST(periodo AS INT) periodo 
	, CAST(nup AS BIGINT) nup 
	, CAST(nro_documento AS BIGINT) nro_documento 
	, IF(denominacion = 'null', NULL, TRIM(denominacion)) denominacion 
	, IF(clasif_efd_sin_disc = 'null', NULL, TRIM(clasif_efd_sin_disc)) clasif_efd_sin_disc 
	, IF(clasif_discrepancia = 'null', NULL, TRIM(clasif_discrepancia)) clasif_discrepancia 
	, IF(clasif_efd = 'null', NULL, TRIM(clasif_efd)) clasif_efd 
	, TRIM(deuda_total) deuda_total
	, TRIM(prev_total) prev_total
	, TRIM(prev_total_sin_disc) prev_total_sin_disc
	, partition_date
FROM bi_corp_staging.rio5_clientes
WHERE TRIM(nro_ejecucion) = '2' ;


SELECT A.partition_date , A.cant_rows , B.numdoc_null
FROM (
		SELECT partition_date , COUNT(1) cant_rows 
		FROM bi_corp_common.stk_cys_discrepanciasasdf
		GROUP BY partition_date ) A 
LEFT JOIN 
	(
		SELECT partition_date , COUNT(1) numdoc_null 
		FROM bi_corp_common.stk_cys_discrepanciasasdf 
		WHERE ds_per_numdoc IS NULL
		GROUP BY partition_date ) B
	ON A.partition_date = B.partition_date ;


SELECT *
FROM bi_corp_common.stk_cys_discrepanciasasdf 
WHERE ds_per_numdoc IS NULL LIMIT 10 
		
SELECT *
FROM bi_corp_staging.rio5_clientes 
WHERE partition_date = '2020-11-30'
AND nro_ejecucion = '2' AND CAST(nro_documento AS BIGINT) IS NULL ;		


SHOW PARTITIONS bi_corp_staging.rio5_clientes

SHOW PARTITIONS bi_corp_common.stk_cys_discrepanciasasdf 


SELECT * FROM bi_corp_common.bt_ga_canales 
WHERE partition_date = '2021-01-17' AND ds_ga_screen LIKE '%SuperClub%'


-- |_/|_/|_/|_/|_/|_/|_/|_/|_/|_/|_/|_/|_/|_/|_/|_/|_/|_/|_/|_/|_/|_/|_/| --
-- / _|/ _|/ _|/ _|/ _|/ _|/ _|/ _|/ _|/ _|/ _|/ _|/ _|/ _|/ _|/ _|/ _|/| --






ALTER TABLE  bi_corp_ic.tb_clientes_dia
DROP IF EXISTS PARTITION(partition_date!='2021-02-14')


SELECT * FROM bi_corp_ic.tb_clientes_dia WHERE as_of_date='2021-02-14' ;

DESCRIBE bi_corp_ic.tb_clientes_dia ;

SHOW PARTITIONS bi_corp_ic.tb_clientes_dia ;




SELECT * FROM bi_corp_staging.rio187_gestiones WHERE partition_date = '2021-02-14' ;

SHOW PARTITIONS bi_corp_staging.rio187_gestiones ;



SHOW PARTITIONS santander_business_risk_arda.nomina_empleados
SHOW PARTITIONS  bi_corp_staging.cloudera_logs_tables

SHOW PARTITIONS bi_corp_staging.rio157_ms0_dm_dwh_composicion_jerqs ;

SELECT * FROM bi_corp_staging.rio56_gestiones WHERE partition_date = '2021-02-18' AND TRIM(ide_gestion_nro) = '12314271'



SHOW PARTITIONS  bi_corp_common.bt_suc_interaccion

SHOW PARTITIONS  bi_corp_common.bt_suc_turnero

SELECT * FROM bi_corp_common.bt_suc_turnero WHERE partition_date = '2021-03-03'

SELECT * FROM bi_corp_staging.alha9601 ;


SELECT * FROM bi_corp_staging.rio154_enc_turno WHERE partition_date = '2019-01-01'

DESCRIBE bi_corp_staging.rio154_enc_turno ;

set hive.execution.engine=spark;
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;


set hive.execution.engine=spark;
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;
CREATE TEMPORARY TABLE turnos_h AS
	SELECT id_turno
		, id_estado
		, tipodoc
		, nrodoc
		, sector
		, id_suc
		, fecha_hora_desde
		, fecha_hora_hasta
		, fraccion
		, dia
		, nup
		, apellido
		, nombre
		, pesubseg
		, pesexper
		, id_motivo
		, mail
		, cuit
		, id_turno_orig
		, id_ejec_atencion
		, id_suc_atencion
		, id_puesto_citas
		, telefono
		, id_tipo_turno
		, fecha_desde
		, fecha_hasta
		, from_unixtime(unix_timestamp(SUBSTRING(fecha_hora_desde, 1, 8),'yyyyMMdd'), 'yyyy-MM-dd') partition_date 
		FROM bi_corp_staging.rio154_enc_turno WHERE partition_date='2019-hist' ;
	
	
set hive.execution.engine=spark;
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;
INSERT OVERWRITE TABLE bi_corp_staging.rio154_enc_turno
PARTITION(partition_date) 
SELECT * FROM turnos_h  WHERE SUBSTRING(partition_date, 1, 7) = '2019-01';

MSCK REPAIR TABLE bi_corp_staging.rio154_enc_turno ;

SHOW PARTITIONS bi_corp_staging.rio154_enc_turno ;

SELECT DISTINCT partition_date FROM turnos_h ORDER BY 1 ;

SELECT COUNT(1) FROM bi_corp_staging.rio154_enc_turno WHERE partition_date='2019-01-01' -- 8.273.171 -- 8.273.171

MSCK REPAIR TABLE bi_corp_staging.rio154_enc_turno ;

SHOW PARTITIONS bi_corp_staging.nesr_encolador_d ;

ALTER TABLE bi_corp_staging.rio154_enc_turno
DROP IF EXISTS PARTITION(partition_date='__HIVE_DEFAULT_PARTITION__')


SHOW PARTITIONS bi_corp_staging.watson_chat ;
SHOW PARTITIONS bi_corp_common.bt_wts_dialogoswatson ;
SHOW PARTITIONS bi_corp_common.bt_suc_interaccion ;

SELECT * FROM bi_corp_common.bt_wts_dialogoswatson WHERE partition_date='2019-10-30' ;

SELECT * FROM bi_corp_common.bt_suc_interaccion WHERE partition_date='2021-02-19'


--######################################################################################--
show partitions bi_corp_common.bt_suc_turnero ;

SHOW PARTITIONS bi_corp_ic.tb_clientes_dia ;

DROP TABLE bi_corp_ic.tb_clientes_dia ;

MSCK REPAIR TABLE bi_corp_ic.tb_clientes_dia ;

DROP TABLE bi_corp_ic.tb_clientes_dia ;

SHOW PARTITIONS bi_corp_ic.tb_clientes_dia ;

DESCRIBE bi_corp_ic.tb_clientes_dia

ALTER TABLE bi_corp_ic.tb_clientes_dia 
SET TBLPROPERTIES ('parquet.column.index.access'='true');

SELECT * FROM bi_corp_ic.tb_clientes_dia2 WHERE partition_date = '2021-02-17' ;

SELECT * FROM bi_corp_ic.tb_clientes_dia WHERE partition_date = '2021-02-17' ;



select cod_suc_gestion , cod_suc_resultado
from bi_corp_common.bt_suc_interaccionproducto
where partition_Date >= '2021-02-15';


select cod_suc_gestion , cod_suc_resultado, cod_suc_identificacionresultado
from bi_corp_common.bt_suc_interaccionalerta
where partition_Date >= '2021-02-15';

set hive.execution.engine=spark;
set spark.yarn.queue=root.dataeng;
SELECT COUNT(1) FROM bi_corp_common.bt_ror_input_activo
WHERE partition_date = '2020-12-31' AND cod_ren_areanegociocorp IS NOT NULL ; -- null 143.744.657


set hive.execution.engine=spark;
set spark.yarn.queue=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_ren_cobrabilidad
SELECT
	C.cod_pais cod_ren_pais,
	C.cod_cobrabilidad cod_ren_cobrabilidad,
	C.cod_cobrabilidad_niv_1 cod_ren_cobrabilidad_niv_1,
	C.cod_cobrabilidad_niv_2 cod_ren_cobrabilidad_niv_2,
	C.cod_cobrabilidad_niv_3 cod_ren_cobrabilidad_niv_3,
	C.cod_cobrabilidad_niv_4 cod_ren_cobrabilidad_niv_4,
	C.cod_cobrabilidad_niv_5 cod_ren_cobrabilidad_niv_5,
	C.cod_cobrabilidad_niv_6 cod_ren_cobrabilidad_niv_6,
	C.cod_cobrabilidad_niv_7 cod_ren_cobrabilidad_niv_7,
	C.cod_cobrabilidad_niv_8 cod_ren_cobrabilidad_niv_8,
	C.cod_cobrabilidad_niv_9 cod_ren_cobrabilidad_niv_9,
	C.cod_cobrabilidad_niv_10 cod_ren_cobrabilidad_niv_10,
	C.cod_cobrabilidad_niv_11 cod_ren_cobrabilidad_niv_11,
	C.cod_cobrabilidad_niv_12 cod_ren_cobrabilidad_niv_12,
	C.cod_cobrabilidad_niv_13 cod_ren_cobrabilidad_niv_13,
	C.cod_cobrabilidad_niv_14 cod_ren_cobrabilidad_niv_14,
	C.des_cobrabilidad_niv_1 ds_ren_cobrabilidad_niv_1,
	C.des_cobrabilidad_niv_2 ds_ren_cobrabilidad_niv_2,
	C.des_cobrabilidad_niv_3 ds_ren_cobrabilidad_niv_3,
	C.des_cobrabilidad_niv_4 ds_ren_cobrabilidad_niv_4,
	C.des_cobrabilidad_niv_5 ds_ren_cobrabilidad_niv_5,
	C.des_cobrabilidad_niv_6 ds_ren_cobrabilidad_niv_6,
	C.des_cobrabilidad_niv_7 ds_ren_cobrabilidad_niv_7,
	C.des_cobrabilidad_niv_8 ds_ren_cobrabilidad_niv_8,
	C.des_cobrabilidad_niv_9 ds_ren_cobrabilidad_niv_9,
	C.des_cobrabilidad_niv_10 ds_ren_cobrabilidad_niv_10,
	C.des_cobrabilidad_niv_11 ds_ren_cobrabilidad_niv_11,
	C.des_cobrabilidad_niv_12 ds_ren_cobrabilidad_niv_12,
	C.des_cobrabilidad_niv_13 ds_ren_cobrabilidad_niv_13,
	C.des_cobrabilidad_niv_14 ds_ren_cobrabilidad_niv_14,
	C.partition_date partition_date
FROM
	bi_corp_staging.rio157_ms0_dm_dwh_cobrabilidad_ C
WHERE
	C.partition_date in (SELECT max(partition_date) FROM bi_corp_staging.rio157_ms0_dm_dwh_cobrabilidad_);



set hive.execution.engine=spark;
set spark.yarn.queue=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_ren_tipoajuste
SELECT
	TA.cod_pais   cod_ren_pais,
	TA.cod_tip_ajuste   cod_ren_tipo_ajuste,
	IF(TA.cod_tip_ajuste_niv_1 ='null',NULL,TA.cod_tip_ajuste_niv_1 )  cod_ren_tipo_ajuste_niv_1,
	IF(TA.cod_tip_ajuste_niv_2 ='null',NULL,TA.cod_tip_ajuste_niv_2 )  cod_ren_tipo_ajuste_niv_2,
	IF(TA.cod_tip_ajuste_niv_3 ='null',NULL,TA.cod_tip_ajuste_niv_3 )  cod_ren_tipo_ajuste_niv_3,
	IF(TA.cod_tip_ajuste_niv_4 ='null',NULL,TA.cod_tip_ajuste_niv_4 )  cod_ren_tipo_ajuste_niv_4,
	IF(TA.cod_tip_ajuste_niv_5 ='null',NULL,TA.cod_tip_ajuste_niv_5 )  cod_ren_tipo_ajuste_niv_5,
	IF(TA.cod_tip_ajuste_niv_6 ='null',NULL,TA.cod_tip_ajuste_niv_6 )  cod_ren_tipo_ajuste_niv_6,
	IF(TA.cod_tip_ajuste_niv_7 ='null',NULL,TA.cod_tip_ajuste_niv_7 )  cod_ren_tipo_ajuste_niv_7,
	IF(TA.cod_tip_ajuste_niv_8 ='null',NULL,TA.cod_tip_ajuste_niv_8 )  cod_ren_tipo_ajuste_niv_8,
	IF(TA.cod_tip_ajuste_niv_9 ='null',NULL,TA.cod_tip_ajuste_niv_9 )  cod_ren_tipo_ajuste_niv_9,
	IF(TA.cod_tip_ajuste_niv_10 ='null',NULL,TA.cod_tip_ajuste_niv_10 )  cod_ren_tipo_ajuste_niv_10,
	IF(TA.cod_tip_ajuste_niv_11 ='null',NULL,TA.cod_tip_ajuste_niv_11 )  cod_ren_tipo_ajuste_niv_11,
	IF(TA.cod_tip_ajuste_niv_12 ='null',NULL,TA.cod_tip_ajuste_niv_12 )  cod_ren_tipo_ajuste_niv_12,
	IF(TA.cod_tip_ajuste_niv_13 ='null',NULL,TA.cod_tip_ajuste_niv_13 )  cod_ren_tipo_ajuste_niv_13,
	IF(TA.cod_tip_ajuste_niv_14 ='null',NULL,TA.cod_tip_ajuste_niv_14 )  cod_ren_tipo_ajuste_niv_14,
	IF(TA.des_tip_ajuste_niv_1 ='null',NULL,TA.des_tip_ajuste_niv_1 )  ds_ren_tipo_ajuste_niv_1,
	IF(TA.des_tip_ajuste_niv_2 ='null',NULL,TA.des_tip_ajuste_niv_2 )  ds_ren_tipo_ajuste_niv_2,
	IF(TA.des_tip_ajuste_niv_3 ='null',NULL,TA.des_tip_ajuste_niv_3 )  ds_ren_tipo_ajuste_niv_3,
	IF(TA.des_tip_ajuste_niv_4 ='null',NULL,TA.des_tip_ajuste_niv_4 )  ds_ren_tipo_ajuste_niv_4,
	IF(TA.des_tip_ajuste_niv_5 ='null',NULL,TA.des_tip_ajuste_niv_5 )  ds_ren_tipo_ajuste_niv_5,
	IF(TA.des_tip_ajuste_niv_6 ='null',NULL,TA.des_tip_ajuste_niv_6 )  ds_ren_tipo_ajuste_niv_6,
	IF(TA.des_tip_ajuste_niv_7 ='null',NULL,TA.des_tip_ajuste_niv_7 )  ds_ren_tipo_ajuste_niv_7,
	IF(TA.des_tip_ajuste_niv_8 ='null',NULL,TA.des_tip_ajuste_niv_8 )  ds_ren_tipo_ajuste_niv_8,
	IF(TA.des_tip_ajuste_niv_9 ='null',NULL,TA.des_tip_ajuste_niv_9 )  ds_ren_tipo_ajuste_niv_9,
	IF(TA.des_tip_ajuste_niv_10 ='null',NULL,TA.des_tip_ajuste_niv_10 )  ds_ren_tipo_ajuste_niv_10,
	IF(TA.des_tip_ajuste_niv_11 ='null',NULL,TA.des_tip_ajuste_niv_11 )  ds_ren_tipo_ajuste_niv_11,
	IF(TA.des_tip_ajuste_niv_12 ='null',NULL,TA.des_tip_ajuste_niv_12 )  ds_ren_tipo_ajuste_niv_12,
	IF(TA.des_tip_ajuste_niv_13 ='null',NULL,TA.des_tip_ajuste_niv_13 )  ds_ren_tipo_ajuste_niv_13,
	IF(TA.des_tip_ajuste_niv_14 ='null',NULL,TA.des_tip_ajuste_niv_14 )  ds_ren_tipo_ajuste_niv_14,
	TA.partition_date   partition_date
FROM
	bi_corp_staging.rio157_ms0_dm_dwh_tipo_ajuste_ TA
WHERE
	TA.partition_date  in (SELECT max(partition_date) FROM bi_corp_staging.rio157_ms0_dm_dwh_tipo_ajuste_);




SET hive.execution.engine = spark;
SET spark.yarn.queue = root.dataeng;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT overwrite TABLE bi_corp_common.bt_suc_direccionador PARTITION (partition_date)

SELECT TRIM(ED.id_tkt) cod_suc_ticket
	, CAST(ED.id_suc AS INT) cod_suc_sucural
	, UPPER(TRIM(ED.sector_espera)) ds_suc_sector
	, TRIM(ED.apellidocliente) ds_per_apellido
	, TRIM(ED.nombrecliente) ds_per_nombre
	, TRIM(ED.cod_producto) cod_suc_producto
	, TRIM(ED.cod_motivo) cod_suc_motivovisita
	, TRIM(ED.mejorproducto) ds_suc_mejorproducto
	, TRIM(ED.segmento) cod_suc_segmento
	, TRIM(ED.semafororentabilidad) cod_suc_semafororentabilidad
	, TRIM(ED.semafororenta) cod_suc_semafororenta
	, TRIM(ED.productosegmento) cod_suc_productosegmento
	, TRIM(ED.contacto) cod_suc_contacto
	, DS.ds_tipodoc ds_per_tipodoc
	, CAST(ED.nrodoc AS BIGINT) ds_per_numdoc
	, TRIM(ED.subsegmento) cod_suc_subsegmento
	, CAST(ED.nup AS BIGINT) cod_per_nup
	, from_unixtime(unix_timestamp(TRIM(ED.fechaticket), 'yyyyMMddHHmmss'),"yyyy-MM-dd HH:mm:ss") ts_suc_ticket
	, TRIM(ED.campana) cod_suc_campana
	, CAST(ED.numero_servicio AS INT) int_suc_servicio
	, TRIM(ED.tipoatencion) cod_suc_tipoatencion
	, TRIM(ED.legajo) cod_suc_legajo
	, TRIM(ED.canalcomp) cod_suc_canalcomp
	, from_unixtime(unix_timestamp(TRIM(ED.fecha_atencion), 'yyyyMMddHHmmss'),"yyyy-MM-dd HH:mm:ss") ts_suc_atencion
	, TRIM(ED.oficial_atencion) cod_suc_oficialatencion
	, TRIM(ED.id_gestion) cod_suc_gestion
	, from_unixtime(unix_timestamp(TRIM(ED.fecha_proceso), 'yyyyMMddHHmmss'),"yyyy-MM-dd HH:mm:ss") ts_suc_proceso
	, TRIM(ED.procesado) cod_suc_procesado
	, from_unixtime(unix_timestamp(TRIM(ED.fecha_fin_atencion), 'yyyyMMddHHmmss'),"yyyy-MM-dd HH:mm:ss") ts_suc_finatencion
	, TRIM(ED.nombre_servicio) ds_suc_servicio
	, TRIM(UPPER(ED.motivo_enc)) ds_suc_motivoenc 
	, TRIM(ED.oficial_nombre) ds_suc_oficialnombre
	, TRIM(ED.origen) cod_suc_origen
	, TRIM(ED.estado) ds_suc_estado
	, TRIM(UPPER(ED.motivo_abandono)) ds_suc_motivoabandono
	, CAST(ED.tecn AS BIGINT) int_suc_tecn
	, ED.partition_date 
FROM bi_corp_staging.nesr_encolador_d ED
LEFT JOIN 
	(SELECT DISTINCT TRIM(substr(gen_clave, 1, 1)) tipodoc, gen_datos ds_tipodoc
	 FROM bi_corp_staging.tcdtgen WHERE gentabla = '0113') DS
	ON TRIM(ED.tipodoc) = DS.tipodoc 

	
	
SET hive.execution.engine = spark;
SET spark.yarn.queue = root.dataeng;
SET hive.exec.dynamic.partition.mode = nonstrict;
--------------------------------------------------
INSERT overwrite TABLE bi_corp_common.bt_suc_detalleticket PARTITION (partition_date)
--------------------------------------------------
SELECT from_unixtime(unix_timestamp(TRIM(ET.fecha), 'yyyyMMdd'),"yyyy-MM-dd") dt_suc_atencion
	, TRIM(ET.sucursal) cod_suc_sucursal
	, TRIM(ET.zona) cod_suc_zona
	, TRIM(UPPER(ET.tipo_cola)) ds_suc_tipocola
	, TRIM(ET.ticket) cod_suc_ticket
	, TRIM(ET.numero_servicio) int_suc_servicio
	, TRIM(ET.nombre_servicio) ds_suc_servicio
	, TRIM(ET.numero_puesto) int_suc_puesto
	, IF(TRIM(ET.nombre_puesto) = '', NULL, TRIM(ET.nombre_puesto)) ds_suc_puesto
	, IF(SUBSTRING(TRIM(ET.usuario), 8, 1) = ' ', SUBSTRING(TRIM(ET.usuario), 1, 7), NULL) cod_suc_legajo
	, IF(SUBSTRING(TRIM(ET.usuario), 8, 1) = ' ', SUBSTRING(TRIM(ET.usuario), 9, 42), NULL) ds_suc_nombreusuario
	, TRIM(ET.t_obj_espera) ts_suc_objespera
	, TRIM(ET.t_max_espera) ts_suc_maxespera
	, TRIM(ET.fecha_ingreso) ts_suc_ingreso
	, TRIM(ET.fecha_llamado) ts_suc_llamado 
	, TRIM(ET.fecha_fin_atencion) ts_suc_finatencion
	, TRIM(ET.tiempo_espera) ts_suc_espera
	, TRIM(ET.tiempo_atencion) ts_suc_tiempoatencion
	, IF(TRIM(ET.auxiliar1) = '', NULL, TRIM(ET.auxiliar1)) ds_suc_auxiliar1
	, IF(TRIM(ET.auxiliar2) = '', NULL, TRIM(ET.auxiliar2)) ds_suc_auxiliar2
	, IF(TRIM(ET.cod_motivo) = '', NULL, TRIM(ET.cod_motivo)) cod_suc_motivovisita
	, IF(TRIM(ET.nom_motivo) = '', NULL, TRIM(UPPER(ET.nom_motivo))) ds_suc_motivovisita
	, TRIM(ET.estado) cod_suc_estado
	, IF(TRIM(ET.motivo_abandono) = '', NULL, TRIM(ET.motivo_abandono)) ds_suc_motivoabandono
	, TRIM(ET.tecn) int_suc_tecn 
	, ET.partition_date 
FROM bi_corp_staging.nesr_encolador_det_ticket ET 


SELECT * FROM bi_corp_common.dim_ren_jeareanegocioctr ; cod_ren_areanegociohijo







SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;
SELECT nro_inh , fec_inh , tpo_doc 
FROM bi_corp_staging.afir_in00
WHERE partition_date = '2021-03-08' 
GROUP BY nro_inh , fec_inh , tpo_doc
HAVING COUNT(*) > 1 ;



SELECT * FROM bi_corp_staging.afir_in00
WHERE partition_date = '2021-03-08' 
AND nro_inh = '02502226' AND fec_inh = '2017-11-10';



SELECT  --WACNL801-TIPO-CLIENTE     
		--WACNL801-SUCURSAL-CUENTA  
		--WACNL801-SUCURSAL-MAQUINA 
		--WACNL801-CANAL            
		--WACNL801-MAQUINA          
		--WACNL801-FECHA-AMD        
		--WACNL801-HORA             
		--WACNL801-TRANSACCION      
		--WACNL801-SERVICIO-ADOM    
		--WACNL801-ORI-REV          
		--WACNL801-FECHA-AMD-PROCESO
		--WACNL801-MARCA-PERSONA    
		--WACNL801-NUP              
		--WACNL801-USUARIO          
		--WACNL801-IMPORTE          
		--WACNL801-DIVISA           
		--WACNL801-CANT-CHEQUES     
		--WACNL801-PRODUCTO         
		--WACNL801-SUBPRODUCTO      
		--WACNL801-TIPO-OPERACION   
		--WACNL801-DATOS            
FROM bi_corp_staging.bgdtmae 
WHERE partition_date = '2021-03-08' ;


SELECT *
FROM bi_corp_staging.malug_movtosq 
WHERE partition_date = '2021-03-05' ;


ALTER TABLE sch_test.test_cascade ADD COLUMNS (cod_ren_idru string) CASCADE ;

SHOW PARTITIONS bi_corp_staging.risksql5_rel_contrato_integ_suc ;

DESCRIBE bi_corp_staging.risksql5_rel_contrato_integ_suc ;

--marca contrato cierre sucursales
--cod_centro_origen_cierre_sucursales
--num_cuenta_origen_cierre_sucursales
--cod_producto_origen_cierre_sucursales
--cod_subprodu_origen_cierre_sucursales


SELECT nup
	, num_grupo
	, entidad_or
	, centro_or
	, contrato_or
	, producto_or
	, subprod_or
	, divisa_or
	, entidad_de
	, centro_de
	, contrato_de
	, producto_de
	, subprod_de
	, divisa_de
	, fec_traspaso
	, centro_op_or
	, centro_op_de 
FROM bi_corp_staging.risksql5_rel_contrato_integ_suc WHERE partition_date = '2020-11-30' ;

SELECT count(1) FROM bi_corp_staging.risksql5_rel_contrato_integ_suc ;

SHOW PARTITIONS bi_corp_staging.garra_wagucdex ;

DESCRIBE bi_corp_staging.garra_wagucdex ;

SELECT waguxdex_cod_marca --marca contrato cierre sucursales
	, waguxdex_cod_centro --cod_centro_origen_cierre_sucursales
	, waguxdex_num_contrato --num_cuenta_origen_cierre_sucursales
	, waguxdex_cod_producto --cod_producto_origen_cierre_sucursales
	, waguxdex_cod_subprodu --cod_subprodu_origen_cierre_sucursales
	---------------------------------------------------------------
	, waguxdex_cod_destino
FROM bi_corp_staging.garra_wagucdex 
WHERE partition_date = '2021-03-05' ;

SELECT DISTINCT waguxdex_cod_marca
FROM bi_corp_staging.garra_wagucdex 
WHERE partition_date = '2021-03-05' ;

SHOW PARTITIONS bi_corp_staging.malpe_peec867c ;

SELECT * FROM bi_corp_staging.malpe_peec867c ;


SELECT * FROM bi_corp_staging.rio151_tbl_interaccion_producto LIMIT 10 ;


DESCRIBE bi_corp_staging.risksql5_rel_contrato_integ_suc

SELECT * FROM bi_corp_staging.param_baremo_local 


CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio151_tbl_combo_iatx_tmk (
	codigo string,
	grupo string,
	descripcion string,
	campo1 string,
	campo2 string,
	campo3 string,
	campo4 string,
	campo5 string,
	campo6 string,
	campo7 string,
	campo8 string,
	campo9 string,
	mc_activo string,
	campo10 string,
	campo11 string,
	campo12 string,
	campo13 string,
	campo14 string,
	campo15 string)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/santander/bi-corp/staging/rio151/dim/tbl_combo_iatx_tmk';



