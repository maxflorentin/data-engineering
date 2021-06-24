SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.size.per.task=4000000;
SET hive.merge.smallfiles.avgsize=16000000;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT INTO TABLE bi_corp_bdr.test_jm_posic_contr partition(partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}')
SELECT  '{{ ti.xcom_pull(task_ids='InputConfig',key='last_calendar_day',dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' AS E0621_FEOPERAC 
       ,'23100'                                                                                                    AS E0621_S1EMP 
       ,lpad(ncr.id_cto_bdr ,9,'0')                                                                                AS E0621_CONTRA1 --Normalización de Contrato 
       ,rpad(' ' ,40,' ')                                                                                          AS E0621_CTA_CONT 
       ,CASE WHEN cov.tipo ='EARLYCARDS' THEN '99997' 
             WHEN cov.tipo ='PLAN V' THEN '99998' 
             WHEN cov.tipo ='TASA 0' THEN '99999' END                                                              AS e0621_tip_impt 
       ,'{{ ti.xcom_pull(task_ids='InputConfig',key='last_calendar_day',dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' AS e0621_fec_mes 
       ,rpad(' ' ,40,' ')                                                                                          AS E0621_AGRCTACB 
       ,CASE WHEN cov.saldo_original >= 0 
             THEN concat("-",lpad(regexp_replace(regexp_replace(format_number(coalesce (cov.saldo_original,0),2),"\\,|\\.",""),"\\,|\\.","") ,16,"0") )  
             ELSE concat("+",lpad(regexp_replace(regexp_replace(format_number(coalesce (cov.saldo_original,0),2),"\\,|\\.",""),"\\,|\\.|\\-",""),16,"0") ) 
        END                                                                                                        AS E0621_IMPORTH 
       ,rpad(cbi.g4095_coddiv ,3,' ')                                                                              AS E0621_CODDIV 
       ,'0001-01-01'                                                                                               AS e0621_fecultmo 
       ,lpad(' ',40,' ')                                                                                           AS E0621_CENTCTBL 
       ,rpad(' ' ,40,' ')                                                                                          AS E0621_CTACGBAL
FROM 
(
	SELECT  cov.sucursal 
	       ,cov.nro_cuenta 
	       ,cov.codigo_producto 
	       ,cov.codigo_subproducto 
	       ,SUM(cov.saldo_original) AS saldo_original 
	       ,SUM(cov.saldo_hoy)      AS saldo_hoy 
	       ,cov.tipo 
	       ,fecha_alta_producto 
	       ,fecha_vencimiento_producto
	FROM 
	(
		SELECT  distinct cov.sucursal 
		       ,cov.nro_cuenta 
		       ,cov.codigo_producto 
		       ,cov.codigo_subproducto 
		       ,cast(cov.saldo_original             AS double) AS saldo_original 
		       ,cast(cov.saldo_hoy                  AS double) AS saldo_hoy 
		       ,cov.tipo 
		       ,SUBSTRING(cov.fecha_alta_producto,1,10)        AS fecha_alta_producto 
		       ,SUBSTRING(cov.fecha_vencimiento_producto,1,10) AS fecha_vencimiento_producto
		FROM bi_corp_bdr.saldos_tarjetas_covid cov
		WHERE cov.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_saldos_tarjetas_covid', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' 
		AND tipo IN ('EARLYCARDS','PLAN V','TASA 0')  
	) cov
	GROUP BY  cov.sucursal 
	         ,cov.nro_cuenta 
	         ,cov.codigo_producto 
	         ,cov.codigo_subproducto 
	         ,cov.tipo 
	         ,fecha_alta_producto 
	         ,fecha_vencimiento_producto 
) cov
INNER JOIN 
(
	SELECT  ncr.id_cto_bdr 
	       ,ncr.cred_cod_entidad 
	       ,ncr.cred_cod_centro 
	       ,ncr.cred_num_cuenta 
	       ,ncr.cred_cod_producto 
	       ,ncr.cred_cod_subprodu_altair
	FROM bi_corp_bdr.normalizacion_id_contratos ncr
	WHERE ncr.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'  
) ncr
ON lpad(cast(cov.sucursal AS string) ,4,'0') = ncr.cred_cod_centro AND lpad(cast(cov.nro_cuenta AS string),12,'0') = ncr.cred_num_cuenta AND lpad(cast(cov.codigo_producto AS string),2,'0') = ncr.cred_cod_producto AND lpad(cast(cov.codigo_subproducto AS string),4,'0') = ncr.cred_cod_subprodu_altair
INNER JOIN 
(
	SELECT  cbi.g4095_contra1 
	       ,cbi.g4095_fechaper 
	       ,cbi.g4095_coddiv 
	       ,cbi.g4095_feccance 
	       ,cbi.g4095_idpro_lc
	FROM bi_corp_bdr.jm_contr_bis cbi
	WHERE cbi.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'  
) cbi
ON cbi.g4095_contra1 = ncr.id_cto_bdr;