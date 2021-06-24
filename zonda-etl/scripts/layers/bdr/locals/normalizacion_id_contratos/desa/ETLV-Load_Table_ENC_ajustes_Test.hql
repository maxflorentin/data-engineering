set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


CREATE TEMPORARY TABLE perimetro_contratos AS
SELECT DISTINCT contratos.* FROM (

SELECT 'ENC_ajustes' source
, concat_ws('_', trim(blc.sociedad), trim(blc.cargabal)) id_cto_source
, '' cred_cod_entidad
, '' cred_cod_centro
, '' cred_num_cuenta
, '' cred_cod_producto
, '' cred_cod_subprodu_altair
, '' mmff_cod_especie
, '' mmff_cod_disponibilidad
, '' mmff_cod_sis_origen
, '' mmff_cod_cartera
, '' mmff_cod_operacion
, '' mmff_cod_pata
, trim(blc.sociedad) as  blc_sociedad
, trim(blc.cargabal) as  blc_cargabal
, '' blc_sociedad_eliminacion
from 
    (
        select  blc.sociedad
                ,blc.cargabal
                , sum(cast(blc.importe as double) ) as importe
        from bi_corp_bdr.balances_ajustes blc 
        where blc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
        and blc.sociedad = '00083'
         group by blc.sociedad, blc.cargabal
    ) blc
inner join 
    (select 
            jpc.e0621_ctacgbal
            , sum(cast(jpc.e0621_importh as double)*-1  / 100) as e0621_importh
        from  bi_corp_bdr.jm_posic_contr jpc 
        where jpc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
        group by jpc.e0621_ctacgbal
    ) jpc
    on trim(jpc.e0621_ctacgbal) = trim(blc.cargabal)
where (COALESCE(blc.importe,0) - COALESCE(jpc.e0621_importh,0))  <> 0

UNION ALL

SELECT 'ENC_ajustes_prov' source
, trim(prv.ctacgbal) id_cto_source
, '' cred_cod_entidad
, '' cred_cod_centro
, '' cred_num_cuenta
, '' cred_cod_producto
, '' cred_cod_subprodu_altair
, '' mmff_cod_especie
, '' mmff_cod_disponibilidad
, '' mmff_cod_sis_origen
, '' mmff_cod_cartera
, '' mmff_cod_operacion
, '' mmff_cod_pata
, '' blc_sociedad
, trim(prv.ctacgbal) as  blc_cargabal
, '' blc_sociedad_eliminacion
from bi_corp_staging.ifrs_provisiones prv
where prv.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'

) contratos;


CREATE TEMPORARY TABLE max_id_cto AS
SELECT COALESCE(MAX(cast(id_cto_bdr as BIGINT)), 0) as max_id_cto
FROM bi_corp_bdr.test_normalizacion_id_contratos_history;


INSERT INTO TABLE bi_corp_bdr.test_normalizacion_id_contratos_history
SELECT lpad(CAST(y.max_id_cto AS BIGINT) + z.rownum, 9, '0') as id_cto_bdr,
z.source,
z.id_cto_source,
z.cred_cod_entidad,
z.cred_cod_centro,
z.cred_num_cuenta,
z.cred_cod_producto,
z.cred_cod_subprodu_altair,
z.mmff_cod_especie,
z.mmff_cod_disponibilidad,
z.mmff_cod_sis_origen,
z.mmff_cod_cartera,
z.mmff_cod_operacion,
z.mmff_cod_pata,
z.blc_sociedad,
z.blc_cargabal,
z.blc_sociedad_eliminacion,
current_date as inserted_date
FROM (
SELECT x.*, ROW_NUMBER() OVER() AS rownum
FROM
(
SELECT p.*
FROM perimetro_contratos p
LEFT JOIN bi_corp_bdr.test_normalizacion_id_contratos_history h ON h.id_cto_source = p.id_cto_source
WHERE h.id_cto_source is null
) x
) z, max_id_cto y;


INSERT INTO TABLE bi_corp_bdr.test_normalizacion_id_contratos
PARTITION ( partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' )
SELECT
hs.id_cto_bdr,
hs.source,
hs.id_cto_source,
hs.cred_cod_entidad,
hs.cred_cod_centro,
hs.cred_num_cuenta,
hs.cred_cod_producto,
hs.cred_cod_subprodu_altair,
hs.mmff_cod_especie,
hs.mmff_cod_disponibilidad,
hs.mmff_cod_sis_origen,
hs.mmff_cod_cartera,
hs.mmff_cod_operacion,
hs.mmff_cod_pata,
hs.blc_sociedad,
hs.blc_cargabal,
hs.blc_sociedad_eliminacion
FROM (
SELECT DISTINCT pc.id_cto_source FROM perimetro_contratos pc
) ds
LEFT JOIN bi_corp_bdr.test_normalizacion_id_contratos_history hs ON hs.id_cto_source = ds.id_cto_source;