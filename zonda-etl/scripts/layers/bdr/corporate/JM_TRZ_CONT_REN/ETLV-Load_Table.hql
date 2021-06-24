set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

-- MORIA
CREATE TEMPORARY TABLE bi_corp_bdr.moria_contratos AS
SELECT  r.cod_entidadg
       ,r.cod_centrog
       ,r.num_contratg
       ,r.cod_productg
       ,trim(r.cod_subprodg) AS cod_subprodg
       ,r.cod_entidad
       ,r.cod_centro
       ,r.num_contrato
       ,r.cod_producto
       ,trim(r.cod_subprodu) AS cod_subprodu
       ,r.fec_altareg
       ,r.cod_divisa
       ,r.fec_baja
       ,c.imp_deuda
       ,row_number() over( partition by r.cod_entidadg,r.cod_centrog,r.num_contratg,r.cod_productg,r.cod_subprodg,r.cod_entidad,r.cod_centro,r.num_contrato,r.cod_producto,r.cod_subprodu ORDER BY r.cod_divisa ) AS rownum
       ,SUM(c.imp_deuda) over( partition by r.cod_entidadg,r.cod_centrog,r.num_contratg,r.cod_productg,r.cod_subprodg,r.cod_entidad,r.cod_centro,r.num_contrato,r.cod_producto,r.cod_subprodu) AS sum_imp_deuda
FROM bi_corp_staging.mddtccn r
LEFT JOIN santander_business_risk_arda.contratos c
ON c.cod_entidad = r.cod_entidad AND c.cod_centro = r.cod_centro AND c.num_cuenta = r.num_contrato AND c.cod_producto = r.cod_producto AND r.cod_subprodu = c.cod_subprodu_altair AND c.cod_divisa = r.cod_divisa AND c.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_contratos', dag_id='BDR_LOAD_Tables-Monthly') }}'
WHERE r.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_mddtccn', dag_id='BDR_LOAD_Tables-Monthly') }}' 
-- WHERE r.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}' 
AND c.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_contratos', dag_id='BDR_LOAD_Tables-Monthly') }}' -- '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Monthly') }}' 
AND r.fec_altareg BETWEEN '{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day', dag_id='BDR_LOAD_Tables-Monthly') }}' AND '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly') }}' ; 

CREATE TEMPORARY TABLE bi_corp_bdr.renum_moria AS
SELECT  z.cod_entidadg
       ,z.cod_centrog
       ,z.num_contratg
       ,z.cod_productg
       ,z.cod_subprodg
       ,z.cod_entidad
       ,z.cod_centro
       ,z.num_contrato
       ,z.cod_producto
       ,z.cod_subprodu
       ,z.fec_altareg
       ,z.fec_baja
       ,z.sum_imp_deuda
       ,case maxrownum WHEN 1 THEN z.cod_divisa else 'ARS' end AS cod_divisa
FROM 
(
	SELECT  x.cod_entidadg
	       ,x.cod_centrog
	       ,x.num_contratg
	       ,x.cod_productg
	       ,x.cod_subprodg
	       ,x.cod_entidad
	       ,x.cod_centro
	       ,x.num_contrato
	       ,x.cod_producto
	       ,x.cod_subprodu
	       ,x.fec_altareg
	       ,x.cod_divisa
	       ,x.fec_baja
	       ,x.rownum
	       ,x.sum_imp_deuda
	       ,MAX(rownum) over ( partition by x.cod_entidadg,x.cod_centrog,x.num_contratg,x.cod_productg,x.cod_subprodg,x.cod_entidad,x.cod_centro,x.num_contrato,x.cod_producto,x.cod_subprodu ) AS maxrownum
	FROM bi_corp_bdr.moria_contratos x 
) z
WHERE z.rownum = z.maxrownum ; 

INSERT OVERWRITE TABLE bi_corp_bdr.jm_trz_cont_ren partition(partition_date)
SELECT  DISTINCT '23100'                                                                                                AS G7025_S1EMP
       ,lpad(n_new.id_cto_bdr,9,'0')                                                                                    AS G7025_CONTRA1
       ,'23100'                                                                                                         AS G7025_EMP_ANT
       ,lpad(n_old.id_cto_bdr,9,'0')                                                                                    AS G7025_CONT_ANT
       ,lpad(bl.cod_baremo_local,5,'0')                                                                                 AS G7025_MOTRENU
       ,r.fec_altareg                                                                                                   AS G7025_FEALTREL
       ,'{{ ti.xcom_pull(task_ids='InputConfig',key='last_working_day',dag_id='BDR_LOAD_Tables-Monthly') }}'            AS G7025_FEC_MOD
       ,concat('-',lpad(regexp_replace(cast(cast(nvl(r.sum_imp_deuda,0) AS decimal(17,2)) AS string),'\\.',''),16,'0')) AS G7025_IMPRESTR
       ,r.cod_divisa                                                                                                    AS G7025_CODDIV
       ,lpad(mp.cod_baremo_global,5,'0')                                                                                AS G7025_MOTRENUG
       ,CASE WHEN r.fec_baja IS NOT NULL THEN r.fec_baja  ELSE '9999-12-31' END                                         AS G7025_FEC_BAJA
       ,'{{ ti.xcom_pull(task_ids='InputConfig',key='month_partition_bdr',dag_id='BDR_LOAD_Tables-Monthly') }}'         AS partition_date
FROM bi_corp_bdr.renum_moria r
INNER JOIN bi_corp_bdr.normalizacion_id_contratos n_old
ON n_old.cred_cod_entidad = r.cod_entidadg AND n_old.cred_cod_centro = r.cod_centrog AND n_old.cred_num_cuenta = r.num_contratg AND n_old.cred_cod_producto = r.cod_productg AND n_old.cred_cod_subprodu_altair = r.cod_subprodg AND n_old.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}'
INNER JOIN bi_corp_bdr.normalizacion_id_contratos n_new
ON n_new.cred_cod_entidad = r.cod_entidad AND n_new.cred_cod_centro = r.cod_centro AND n_new.cred_num_cuenta = r.num_contrato AND n_new.cred_cod_producto = r.cod_producto AND n_new.cred_cod_subprodu_altair = r.cod_subprodu AND n_new.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}'
LEFT JOIN bi_corp_bdr.v_baremos_local bl
ON bl.cod_negocio_local = '90' AND bl.cod_baremo_local = '4'
LEFT JOIN bi_corp_bdr.v_map_baremos_global_local mp
ON mp.cod_negocio_local = 90 AND mp.cod_baremo_local = bl.cod_baremo_local
WHERE n_old.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}' 
AND n_new.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}' ; 

-- SUCURSALES
CREATE TEMPORARY TABLE bi_corp_bdr.sucursales_contratos AS
SELECT  r.pecdgent
       ,r.datos_comunes_peofiori
       ,r.penumcon
       ,r.pecodpro
       ,trim(r.pecodsub)  AS pecodsub
       ,r.pecdgentd
       ,r.datos_comunes_peofides
       ,r.penumcond
       ,r.pecodprod
       ,trim(r.pecodsubd) AS pecodsubd
       ,r.pecodmond
       ,p.pefecest
       ,r.pesdoant
       ,row_number() over( partition by r.pecdgent,r.datos_comunes_peofiori,r.penumcon,r.pecodpro,r.pecodsub,r.pecdgentd,r.datos_comunes_peofides,r.penumcond,r.pecodprod,r.pecodsubd ORDER BY r.pecodmond ) AS rownum
FROM bi_corp_staging.malpe_peec867c r
LEFT JOIN bi_corp_staging.malpe_pedt042 p
ON r.pecdgent = p.pecdgent AND p.pecodofi = r.datos_comunes_peofiori AND substring(p.penumcon, 4) = substring(r.penumcon, 4) AND p.pecodpro = r.pecodpro AND p.pecodsub = r.pecodsub AND p.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt042', dag_id='BDR_LOAD_Tables-Monthly') }}'
WHERE r.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_peec867c', dag_id='BDR_LOAD_Tables-Monthly') }}' --'2019-12-13' valor fijo hasta que se cargue una novedad. no programado 
AND p.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt042', dag_id='BDR_LOAD_Tables-Monthly') }}' 
-- AND p.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}' 
AND p.pefecest BETWEEN '{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day', dag_id='BDR_LOAD_Tables-Monthly') }}' AND '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly') }}' 
AND p.peestope = 'C' ; 

CREATE TEMPORARY TABLE bi_corp_bdr.renum_sucursales AS
SELECT  z.pecdgent
       ,z.datos_comunes_peofiori
       ,z.penumcon
       ,z.pecodpro
       ,z.pecodsub
       ,z.pecdgentd
       ,z.datos_comunes_peofides
       ,z.penumcond
       ,z.pecodprod
       ,z.pecodsubd
       ,z.pefecest
       ,z.pesdoant 
       ,case maxrownum WHEN 1 THEN z.pecodmond else 'ARS' end AS pecodmond
FROM 
(
	SELECT  x.pecdgent
	       ,x.datos_comunes_peofiori
	       ,x.penumcon
	       ,x.pecodpro
	       ,x.pecodsub
	       ,x.pecdgentd
	       ,x.datos_comunes_peofides
	       ,x.penumcond
	       ,x.pecodprod
	       ,x.pecodsubd
	       ,x.pecodmond
	       ,x.pefecest
	       ,x.pesdoant
	       ,x.rownum
	       ,MAX(rownum) over ( partition by x.pecdgent,x.datos_comunes_peofiori,x.penumcon,x.pecodpro,x.pecodsub,x.pecdgentd,x.datos_comunes_peofides,x.penumcond,x.pecodprod,x.pecodsubd ) AS maxrownum
	FROM bi_corp_bdr.sucursales_contratos x 
) z
WHERE z.rownum = z.maxrownum ; 

INSERT INTO TABLE bi_corp_bdr.jm_trz_cont_ren partition(partition_date)
SELECT  DISTINCT '23100'                                                                                           AS G7025_S1EMP
       ,lpad(n_new.id_cto_bdr,9,'0')                                                                               AS G7025_CONTRA1
       ,'23100'                                                                                                    AS G7025_EMP_ANT
       ,lpad(n_old.id_cto_bdr,9,'0')                                                                               AS G7025_CONT_ANT
       ,lpad(bl.cod_baremo_local,5,'0')                                                                            AS G7025_MOTRENU
       ,r.pefecest                                                                                                 AS G7025_FEALTREL
       ,'{{ ti.xcom_pull(task_ids='InputConfig',key='last_working_day',dag_id='BDR_LOAD_Tables-Monthly') }}'       AS G7025_FEC_MOD
       ,concat('-',lpad(regexp_replace(cast(cast(nvl(r.pesdoant,0) AS decimal(17,2)) AS string),'\\.',''),16,'0')) AS G7025_IMPRESTR -- PENDIENTE DE DEFINICION
       ,r.pecodmond                                                                                                AS G7025_CODDIV
       ,lpad(mp.cod_baremo_global,5,'0')                                                                           AS G7025_MOTRENUG
       ,CASE WHEN r.pefecest IS NOT NULL THEN r.pefecest  ELSE '9999-12-31' END                                    AS G7025_FEC_BAJA
       ,'{{ ti.xcom_pull(task_ids='InputConfig',key='month_partition_bdr',dag_id='BDR_LOAD_Tables-Monthly') }}'    AS partition_date
FROM bi_corp_bdr.renum_sucursales r
INNER JOIN bi_corp_bdr.normalizacion_id_contratos n_old
ON n_old.cred_cod_entidad = r.pecdgent AND n_old.cred_cod_centro = r.datos_comunes_peofiori AND substring(n_old.cred_num_cuenta, 4) = substring(r.penumcon, 4) AND n_old.cred_cod_producto = r.pecodpro AND n_old.cred_cod_subprodu_altair = r.pecodsub AND n_old.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}'
INNER JOIN bi_corp_bdr.normalizacion_id_contratos n_new
ON n_new.cred_cod_entidad = r.pecdgentd AND n_new.cred_cod_centro = r.datos_comunes_peofides AND substring(n_new.cred_num_cuenta, 4) = substring(r.penumcond, 4) AND n_new.cred_cod_producto = r.pecodprod AND n_new.cred_cod_subprodu_altair = r.pecodsubd AND n_new.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}'
LEFT JOIN bi_corp_bdr.v_baremos_local bl
ON bl.cod_negocio_local = '90' AND bl.cod_baremo_local = '3'
LEFT JOIN bi_corp_bdr.v_map_baremos_global_local mp
ON mp.cod_negocio_local = 90 AND mp.cod_baremo_local = bl.cod_baremo_local
WHERE n_old.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}' 
AND n_new.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}' ; 



-- MIGRACION DE CUENTAS
CREATE TEMPORARY TABLE bi_corp_bdr.migracion_contratos AS
SELECT  r.mig_old_entidad
       ,r.mig_old_cent_alta
       ,r.mig_old_cuenta
       ,p_old.pecodpro       AS old_pecodpro
       ,trim(p_old.pecodsub) AS old_pecodsub
       ,r.mig_new_entidad
       ,r.mig_new_cent_alta
       ,r.mig_new_cuenta
       ,p_new.pecodpro       AS new_pecodpro
       ,trim(p_new.pecodsub) AS new_pecodsub
       ,r.mig_old_fech_baja
       ,r.mig_new_divisa
       ,row_number() over( partition by r.mig_old_entidad,r.mig_old_cent_alta,r.mig_old_cuenta,p_old.pecodpro,trim(p_old.pecodsub),r.mig_new_entidad,r.mig_new_cent_alta,r.mig_new_cuenta,p_new.pecodpro,trim(p_new.pecodsub) ORDER BY r.mig_new_divisa ) AS rownum
FROM bi_corp_staging.malbgc_zbdtmig r
LEFT JOIN bi_corp_staging.malpe_pedt008 p_old
ON p_old.pecodent = r.mig_old_entidad AND p_old.pecodofi = r.mig_old_cent_alta AND concat("0", p_old.pecodpro, substring(p_old.penumcon, 4) ) = r.mig_old_cuenta AND p_old.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt008', dag_id='BDR_LOAD_Tables-Monthly') }}'
LEFT JOIN bi_corp_staging.malpe_pedt008 p_new
ON p_new.pecodent = r.mig_new_entidad AND p_new.pecodofi = r.mig_new_cent_alta AND concat("0",p_new.pecodpro, substring(p_new.penumcon, 4) ) = r.mig_new_cuenta AND p_new.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt008', dag_id='BDR_LOAD_Tables-Monthly') }}'
WHERE r.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malbgc_zbdtmig', dag_id='BDR_LOAD_Tables-Monthly') }}' 
-- WHERE r.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}' 
AND p_old.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt008', dag_id='BDR_LOAD_Tables-Monthly') }}' 
AND p_new.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt008', dag_id='BDR_LOAD_Tables-Monthly') }}' 
-- AND p_old.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}' 
AND r.mig_old_fech_baja BETWEEN '{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day', dag_id='BDR_LOAD_Tables-Monthly') }}' AND '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly') }}' ; 

CREATE TEMPORARY TABLE bi_corp_bdr.renum_migracion AS
SELECT  z.mig_old_entidad
       ,z.mig_old_cent_alta
       ,z.mig_old_cuenta
       ,z.old_pecodpro
       ,z.old_pecodsub
       ,z.mig_new_entidad
       ,z.mig_new_cent_alta
       ,z.mig_new_cuenta
       ,z.new_pecodpro
       ,z.new_pecodsub
       ,z.mig_old_fech_baja
       ,case z.maxrownum WHEN 1 THEN z.mig_new_divisa else 'ARS' end AS mig_new_divisa
FROM 
(
	SELECT  x.mig_old_entidad
	       ,x.mig_old_cent_alta
	       ,x.mig_old_cuenta
	       ,x.old_pecodpro
	       ,x.old_pecodsub
	       ,x.mig_new_entidad
	       ,x.mig_new_cent_alta
	       ,x.mig_new_cuenta
	       ,x.new_pecodpro
	       ,x.new_pecodsub
	       ,x.mig_old_fech_baja
	       ,x.rownum
	       ,x.mig_new_divisa
	       ,MAX(rownum) over ( partition by x.mig_old_entidad,x.mig_old_cent_alta,x.mig_old_cuenta,x.old_pecodpro,x.old_pecodsub,x.mig_new_entidad,x.mig_new_cent_alta,x.mig_new_cuenta,x.new_pecodpro,x.new_pecodsub ) AS maxrownum
	FROM bi_corp_bdr.migracion_contratos x 
) z
WHERE z.rownum = z.maxrownum ; 

INSERT INTO TABLE bi_corp_bdr.jm_trz_cont_ren partition(partition_date)
SELECT  '23100'                                                                                                 AS G7025_S1EMP
       ,lpad(n_new.id_cto_bdr,9,'0')                                                                            AS G7025_CONTRA1
       ,'23100'                                                                                                 AS G7025_EMP_ANT
       ,lpad(n_old.id_cto_bdr,9,'0')                                                                            AS G7025_CONT_ANT
       ,lpad(bl.cod_baremo_local,5,'0')                                                                         AS G7025_MOTRENU
       ,r.mig_old_fech_baja                                                                                     AS G7025_FEALTREL
       ,'{{ ti.xcom_pull(task_ids='InputConfig',key='last_working_day',dag_id='BDR_LOAD_Tables-Monthly') }}'    AS G7025_FEC_MOD
       ,concat('-',lpad('0',16,'0'))                                                                            AS G7025_IMPRESTR
       ,r.mig_new_divisa                                                                                        AS G7025_CODDIV
       ,lpad(mp.cod_baremo_global,5,'0')                                                                        AS G7025_MOTRENUG
       ,CASE WHEN r.mig_old_fech_baja IS NOT NULL THEN r.mig_old_fech_baja  ELSE '9999-12-31' END               AS G7025_FEC_BAJA
       ,'{{ ti.xcom_pull(task_ids='InputConfig',key='month_partition_bdr',dag_id='BDR_LOAD_Tables-Monthly') }}' AS partition_date
FROM bi_corp_bdr.renum_migracion r
INNER JOIN bi_corp_bdr.normalizacion_id_contratos n_old
ON n_old.cred_cod_entidad = r.mig_old_entidad AND n_old.cred_cod_centro = r.mig_old_cent_alta AND substring(n_old.cred_num_cuenta, 4) = substring(r.mig_old_cuenta, 4) AND n_old.cred_cod_producto = r.old_pecodpro AND n_old.cred_cod_subprodu_altair = r.old_pecodsub AND n_old.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}'
INNER JOIN bi_corp_bdr.normalizacion_id_contratos n_new
ON n_new.cred_cod_entidad = r.mig_new_entidad AND n_new.cred_cod_centro = r.mig_new_cent_alta AND substring(n_new.cred_num_cuenta, 4) = substring(r.mig_new_cuenta, 4) AND n_new.cred_cod_producto = r.new_pecodpro AND n_new.cred_cod_subprodu_altair = r.new_pecodsub AND n_new.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}'
LEFT JOIN bi_corp_bdr.v_baremos_local bl
ON bl.cod_negocio_local = '90' AND bl.cod_baremo_local = '1'
LEFT JOIN bi_corp_bdr.v_map_baremos_global_local mp
ON mp.cod_negocio_local = 90 AND mp.cod_baremo_local = bl.cod_baremo_local
WHERE n_old.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}' 
AND n_new.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}' ; 

-- UPGRADE DE TARJETAS
INSERT INTO TABLE bi_corp_bdr.jm_trz_cont_ren partition(partition_date)
SELECT  DISTINCT '23100'                                                                                            AS G7025_S1EMP
       ,lpad(n_new.id_cto_bdr,9,'0')                                                                                AS G7025_CONTRA1
       ,'23100'                                                                                                     AS G7025_EMP_ANT
       ,lpad(n_old.id_cto_bdr,9,'0')                                                                                AS G7025_CONT_ANT
       ,lpad(bl.cod_baremo_local,5,'0')                                                                             AS G7025_MOTRENU
       ,r.fec_renum                                                                                                 AS G7025_FEALTREL
       ,'{{ ti.xcom_pull(task_ids='InputConfig',key='last_working_day',dag_id='BDR_LOAD_Tables-Monthly') }}'        AS G7025_FEC_MOD
       ,concat('-',lpad(regexp_replace(cast(cast(nvl(r.imp_reest,0) AS decimal(17,2)) AS string),'\\.',''),16,'0')) AS G7025_IMPRESTR
       ,r.divisa                                                                                                    AS G7025_CODDIV
       ,lpad(mp.cod_baremo_global,5,'0')                                                                            AS G7025_MOTRENUG
       ,CASE WHEN r.fec_renum IS NOT NULL THEN r.fec_renum  ELSE '9999-12-31' END                                   AS G7025_FEC_BAJA
       ,'{{ ti.xcom_pull(task_ids='InputConfig',key='month_partition_bdr',dag_id='BDR_LOAD_Tables-Monthly') }}'     AS partition_date
FROM bi_corp_bdr.upgrade_tarjetas r
INNER JOIN bi_corp_bdr.normalizacion_id_contratos n_old
ON n_old.cred_cod_entidad = r.old_entidad AND n_old.cred_cod_centro = r.old_centro AND substring(n_old.cred_num_cuenta, 2) = substring(r.old_cuenta, 2) AND n_old.cred_cod_producto = r.old_producto AND n_old.cred_cod_subprodu_altair = trim(r.old_subprodu) AND n_old.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}'
INNER JOIN bi_corp_bdr.normalizacion_id_contratos n_new
ON n_new.cred_cod_entidad = r.new_entidad AND n_new.cred_cod_centro = r.new_centro AND substring(n_new.cred_num_cuenta, 2) = substring(r.new_cuenta, 2) AND n_new.cred_cod_producto = r.new_producto AND n_new.cred_cod_subprodu_altair = trim(r.new_subprodu) AND n_new.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}'
LEFT JOIN bi_corp_bdr.v_baremos_local bl
ON bl.cod_negocio_local = '90' AND bl.cod_baremo_local = '6'
LEFT JOIN bi_corp_bdr.v_map_baremos_global_local mp
ON mp.cod_negocio_local = 90 AND mp.cod_baremo_local = bl.cod_baremo_local
WHERE n_old.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}' 
AND n_new.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}' 
AND r.partition_date BETWEEN '{{ ti.xcom_pull(task_ids='InputConfig', key='first_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}' AND '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}' ; 

--Renumeracion contratos corresponsales
INSERT INTO TABLE bi_corp_bdr.jm_trz_cont_ren partition(partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}')
SELECT  '23100'                                                                                                                           AS G7025_S1EMP 
       ,lpad(nct.id_cto_bdr,9,'0')                                                                                                        AS G7025_CONTRA1 --Normalización de Contratos 
       ,'23100'                                                                                                                           AS G7025_EMP_ANT 
       ,lpad(nct2.id_cto_bdr,9,'0')                                                                                                       AS G7025_CONT_ANT --Normalización de Contratos 
       ,lpad(20,5,'0')                                                                                                                    AS G7025_MOTRENU -- Baremos 
       ,concat(substr('{{ ti.xcom_pull(task_ids='InputConfig',key='next_month_first_day',dag_id='BDR_LOAD_Tables-Monthly') }}',0,8),'06') AS G7025_FEALTREL --Día siguiente a la fecha de vencimiento actual vencen el 05 de cada mes 
       ,'{{ ti.xcom_pull(task_ids='InputConfig',key='last_working_day',dag_id='BDR_LOAD_Tables-Monthly') }}'                              AS G7025_FEC_MOD --last_working_day 
       ,CASE WHEN cast(nvl(regexp_replace(trim(alh.importe),"\\,","."),0) AS double) >= 0 
             THEN concat("-",nvl(lpad(regexp_replace(format_number(cast(nvl(regexp_replace(trim(alh.importe),"\\,","."),0) AS double),2),"\\,|\\.",""),16,"0"),0))  
             ELSE concat("+",nvl(lpad(regexp_replace(format_number(cast(nvl(regexp_replace(trim(alh.importe),"\\,","."),0) AS double),2),"\\,|\\.|\\-",""),16,"0"),0)) 
        END                                                                                                                               AS G7025_IMPRESTR --Valor por defecto: Valor actualizado del balance en el nuevo mes 
       ,crp.moneda                                                                                                                        AS G7025_CODDIV 
       ,lpad(20,5,'0')                                                                                                                    AS G7025_MOTRENUG -- Baremos 
       ,'9999-12-31'                                                                                                                      AS G7025_FEC_BAJA
FROM 
(
	SELECT  *
	FROM bi_corp_staging.corresponsales crp
	WHERE crp.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_corresponsales', dag_id='BDR_LOAD_Tables-Monthly') }}'  
) crp
LEFT JOIN 
(
	SELECT  *
	FROM bi_corp_bdr.normalizacion_id_contratos
	WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}' 
	AND source = 'corresponsales-tactico'  
) nct
ON concat_ws('_', trim(crp.nup), trim(crp.moneda), trim(crp.rubro_altair)) = trim(nct.id_cto_source)
LEFT JOIN 
(
	SELECT  *
	FROM bi_corp_bdr.normalizacion_id_contratos
	WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}' 
	AND source = 'corresponsales-tactico'  
) nct2
ON concat_ws('_', trim(crp.nup), trim(crp.moneda), trim(crp.rubro_altair_ant)) = trim(nct2.id_cto_source)
LEFT JOIN 
(
	SELECT  alh.rubro_altair 
	       ,trim(group_map[cast(month('{{ ti.xcom_pull(task_ids='InputConfig',key='last_calendar_day',dag_id='BDR_LOAD_Tables-Monthly') }}') AS string)]) AS importe
	FROM 
	(
		SELECT  rubro_altair
		       ,map('1',enero ,'2',febrero ,'3',marzo ,'4',abril ,'5',mayo ,'6',junio ,'7',julio ,'8',agosto ,'9',septiembre ,'10',octubre ,'11',noviembre ,'12',diciembre) AS group_map
		FROM bi_corp_staging.alha9600
		WHERE partition_date = '{{ var.json.jm_trz_cont_ren.alha9600 }}'  
	) alh 
) alh
ON trim(alh.rubro_altair) = trim(crp.rubro_altair);