set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

-- max partitions for personas
DROP TABLE IF EXISTS tmp_maxpart_personas;
CREATE TEMPORARY TABLE tmp_maxpart_personas AS
select
    max(partition_date) AS partition_date
from bi_corp_common.stk_per_personas
where partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}',7)
  and partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'
  and cod_per_segmentoduro is not null;

DROP TABLE IF EXISTS personas_segmentoduro;
CREATE TEMPORARY TABLE personas_segmentoduro AS
select
    p.cod_per_nup,
    p.cod_per_segmentoduro,
    p.ds_per_segmento,
    p.ds_per_subsegmento
from bi_corp_common.stk_per_personas p
INNER JOIN tmp_maxpart_personas maxp on (p.partition_date=maxp.partition_date);


-- temporal table to calculate limites lineas
CREATE TEMPORARY TABLE limites_lineas AS
select
    l.cod_adm_nro_prop,
    sum(if(l.int_adm_secuencia = 1, l.fc_adm_monto_linea, 0)) as fc_adm_limite_solicitado,
    sum(if(l.int_adm_secuencia = 2 and l.ds_adm_estado_linea = 'A', l.fc_adm_monto_linea, 0)) as fc_adm_limite_aprobado,
    sum(if(l.int_adm_secuencia = 1 and l.cod_adm_operacion not in ('59','70','71','72','91','92') and l.ds_adm_estado_linea = 'A', l.fc_adm_monto_linea, 0)) as fc_adm_limite_solicitado_sin_linea_individuos,
    sum(if(l.int_adm_secuencia = 2 and l.cod_adm_operacion not in ('59','70','71','72','91','92') and l.ds_adm_estado_linea = 'A', l.fc_adm_monto_linea, 0)) as fc_adm_limite_aprobado_sin_linea_individuos
from bi_corp_common.stk_adm_pyme_lineas l
where l.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'
group by l.cod_adm_nro_prop;

-- temporal table to calculate the max date for estados propuestas for each propuesta by sector
DROP TABLE IF EXISTS estados_maxfechabysector;
CREATE TEMPORARY TABLE estados_maxfechabysector AS
select
    e.cod_adm_nro_prop as cod_adm_nro_prop,
    e.ds_adm_sector_estado,
    max(e.dt_adm_fec_log) as max_fecha
from bi_corp_common.stk_adm_estadospropuesta e
where e.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'
group by e.cod_adm_nro_prop, e.ds_adm_sector_estado;

-- temporal table to calculate the total max date for estados propuestas for each propuesta, no matter the sector
DROP TABLE IF EXISTS estados_maxfecha;
CREATE TEMPORARY TABLE estados_maxfecha AS
select
    e.cod_adm_nro_prop as cod_adm_nro_prop,
    max(e.dt_adm_fec_log) as max_fecha
from bi_corp_common.stk_adm_estadospropuesta e
where e.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'
group by e.cod_adm_nro_prop;


-- temporal table to get data related to a propuesta with the max date by sector
-- if there is more than one result with the max date, we take the first one
DROP TABLE IF EXISTS estadospropuestasimplificadobysector;
CREATE TEMPORARY TABLE estadospropuestasimplificadobysector AS
select * from (
    select
        e.cod_adm_nro_prop,
        e.ds_adm_log,
        e.ds_adm_obs_log,
        e.cod_adm_usuario,
        e.ds_adm_estado_prop,
        e.cod_adm_estado_accion,
        e.cod_adm_estado_origen,
        e.ds_adm_sector_estado,
        e.ds_adm_tipo_estado,
        e.ds_adm_origen,
        e.cod_adm_penumper,
        e.int_adm_cuit_cliente,
        e.cod_adm_campania,
        emaxf.max_fecha as dt_adm_fec_log,
        row_number() over (partition by e.cod_adm_nro_prop, emaxf.max_fecha order by e.cod_adm_usuario desc) rn
    from bi_corp_common.stk_adm_estadospropuesta e
    inner join estados_maxfechabysector emaxf on emaxf.cod_adm_nro_prop = e.cod_adm_nro_prop and emaxf.max_fecha = e.dt_adm_fec_log and emaxf.ds_adm_sector_estado = e.ds_adm_sector_estado
    where e.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'
) temp where rn = 1;

-- temporal table to get data related to a propuesta with the total max date
-- if there is more than one result with the max date, we take the first one
DROP TABLE IF EXISTS estadospropuestasimplificado;
CREATE TEMPORARY TABLE estadospropuestasimplificado AS
select * from (
    select
        e.cod_adm_nro_prop,
        e.ds_adm_log,
        e.ds_adm_obs_log,
        e.cod_adm_usuario,
        e.ds_adm_estado_prop,
        e.cod_adm_estado_accion,
        e.cod_adm_estado_origen,
        e.ds_adm_sector_estado,
        e.ds_adm_tipo_estado,
        e.ds_adm_origen,
        e.cod_adm_penumper,
        e.int_adm_cuit_cliente,
        e.cod_adm_campania,
        emaxf.max_fecha as dt_adm_fec_log,
        row_number() over (partition by e.cod_adm_nro_prop, emaxf.max_fecha order by e.cod_adm_usuario desc) rn
    from bi_corp_common.stk_adm_estadospropuesta e
    inner join estados_maxfecha emaxf on emaxf.cod_adm_nro_prop = e.cod_adm_nro_prop and emaxf.max_fecha = e.dt_adm_fec_log
    where e.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'
) temp where rn = 1;


-- temporal table to calculate the fields for resolucion scoring + zona gris
CREATE TEMPORARY TABLE resol_scoring_zonagris AS
select
    cast(p.nro_prop as int) as cod_adm_nro_prop,
    cast(CASE
        when ezg.ds_adm_tipo_estado is NOT null
            then ezg.ds_adm_tipo_estado
        when escoring.ds_adm_tipo_estado is not null and ezg.ds_adm_tipo_estado is null
            then escoring.ds_adm_tipo_estado
            else null
    END as string) as ds_adm_estado_resolucion_scoring_zona_gris,

    cast(CASE
        when ezg.dt_adm_fec_log is NOT null
            then ezg.dt_adm_fec_log
        when escoring.dt_adm_fec_log is not null and ezg.dt_adm_fec_log is null
            then escoring.dt_adm_fec_log
            else null
    END as string )as dt_adm_fec_resol_scoring_zona_gris,

    cast(CASE
        when ezg.cod_adm_estado_accion is NOT null
            then ezg.cod_adm_estado_accion
        when escoring.cod_adm_estado_accion is not null and ezg.cod_adm_estado_accion is null
            then escoring.cod_adm_estado_accion
        ELSE null
    END as string) as cod_adm_estado_scoring_zona_gris,

    cast(CASE
        when ezg.ds_adm_sector_estado is NOT null
            then ezg.ds_adm_sector_estado
        when escoring.ds_adm_sector_estado is not null and ezg.ds_adm_sector_estado is null
            then escoring.ds_adm_sector_estado
        ELSE null
    END as string )as cod_adm_sector_scoring_zona_gris
from bi_corp_staging.sge_propuesta p
left join estadospropuestasimplificadobysector escoring on escoring.cod_adm_nro_prop = cast(p.nro_prop as int) and escoring.ds_adm_sector_estado = 'SCORING'
left join estadospropuestasimplificadobysector ezg on ezg.cod_adm_nro_prop = cast(p.nro_prop as int) and ezg.ds_adm_sector_estado = 'ZONA GRIS'
where p.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}';


-- temporal table to calculate cod_adm_estado_ultima_resolucion_riesgos
CREATE TEMPORARY TABLE cod_ultimaresolucionriesgos AS
SELECT
    cast(p.nro_prop AS INT) AS cod_adm_nro_prop,
    cast(CASE
    WHEN ecvc.dt_adm_fec_log IS NOT NULL
        AND esre.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
        AND ecvc.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
        AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
        AND ecvc.dt_adm_fec_log > esre.dt_adm_fec_log
    THEN ecvc.cod_adm_estado_accion
    WHEN ecvc.dt_adm_fec_log IS NOT NULL   --WHEN modificado
        AND esre.dt_adm_fec_log IS NOT NULL
        AND esre.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
        AND esre.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
        AND ecvc.dt_adm_fec_log < esre.dt_adm_fec_log
    THEN esre.cod_adm_estado_accion
    WHEN ecvc.dt_adm_fec_log IS NOT NULL   --WHEN modificado
        AND esre.dt_adm_fec_log IS NOT NULL
        AND ecvc.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
        AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
        AND ecvc.dt_adm_fec_log > esre.dt_adm_fec_log
    THEN ecvc.cod_adm_estado_accion
    WHEN ecvc.dt_adm_fec_log IS NOT NULL
        AND esre.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
        AND ecvc.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
        AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
        AND ecvc.dt_adm_fec_log < esre.dt_adm_fec_log
    THEN ecvc.cod_adm_estado_accion
    WHEN ecvc.dt_adm_fec_log IS NOT NULL
        AND esre.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
        AND ecvc.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
        AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
        AND ecvc.dt_adm_fec_log < esre.dt_adm_fec_log
        THEN szg.cod_adm_estado_scoring_zona_gris
    WHEN ecvc.dt_adm_fec_log IS NOT NULL
        AND esre.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
        AND ecvc.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
        AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
        AND ecvc.dt_adm_fec_log > esre.dt_adm_fec_log
        THEN szg.cod_adm_estado_scoring_zona_gris
    WHEN ecvc.dt_adm_fec_log IS NOT NULL
        AND ecvc.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
        AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
        AND esre.dt_adm_fec_log IS NULL
        THEN szg.cod_adm_estado_scoring_zona_gris
    WHEN ecvc.dt_adm_fec_log IS NOT NULL
        AND ecvc.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
        AND ecvc.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
        AND esre.dt_adm_fec_log IS NULL
        THEN szg.cod_adm_estado_scoring_zona_gris
	WHEN esre.dt_adm_fec_log IS NOT NULL
        AND esre.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
        AND esre.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
        AND ecvc.dt_adm_fec_log IS NULL
        THEN szg.cod_adm_estado_scoring_zona_gris
	WHEN esre.dt_adm_fec_log IS NOT NULL
        AND esre.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
        AND esre.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
        AND ecvc.dt_adm_fec_log IS NULL
        THEN szg.cod_adm_estado_scoring_zona_gris
	WHEN esre.dt_adm_fec_log IS NOT NULL
        AND esre.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
        AND esre.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
        AND ecvc.dt_adm_fec_log < esre.dt_adm_fec_log
        THEN esre.cod_adm_estado_accion
    WHEN esre.dt_adm_fec_log IS NOT NULL
        AND esre.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
        AND esre.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
        AND ecvc.dt_adm_fec_log IS NULL
    THEN esre.cod_adm_estado_accion
    when ecvc.dt_adm_fec_log IS NOT NULL
        and esre.dt_adm_fec_log IS NOT NULL
        AND esre.ds_adm_tipo_estado not IN ('ACEPTADA','DECLINADA')
        AND ecvc.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
        AND szg.dt_adm_fec_resol_scoring_zona_gris < esre.dt_adm_fec_log
        AND szg.dt_adm_fec_resol_scoring_zona_gris is NOT null
    THEN szg.cod_adm_estado_scoring_zona_gris
    when ecvc.dt_adm_fec_log IS NOT NULL
        and esre.dt_adm_fec_log IS NOT NULL
        AND ecvc.ds_adm_tipo_estado not IN ('ACEPTADA','DECLINADA')
        AND esre.ds_adm_tipo_estado not IN ('ACEPTADA','DECLINADA')
        AND ecvc.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
        AND szg.dt_adm_fec_resol_scoring_zona_gris < esre.dt_adm_fec_log
        AND szg.dt_adm_fec_resol_scoring_zona_gris < ecvc.dt_adm_fec_log
        AND szg.dt_adm_fec_resol_scoring_zona_gris is NOT null
    THEN szg.cod_adm_estado_scoring_zona_gris
    when ecvc.dt_adm_fec_log IS NOT NULL
        and esre.dt_adm_fec_log IS NOT NULL
        AND ecvc.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
        AND esre.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
        AND szg.dt_adm_fec_resol_scoring_zona_gris is NOT null
    THEN szg.cod_adm_estado_scoring_zona_gris
    when ecvc.dt_adm_fec_log IS NOT NULL
        and esre.dt_adm_fec_log IS NULL
        AND ecvc.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
        AND szg.dt_adm_fec_resol_scoring_zona_gris is NOT null
    THEN szg.cod_adm_estado_scoring_zona_gris
    when esre.dt_adm_fec_log IS NOT NULL
        and ecvc.dt_adm_fec_log IS NULL
        AND esre.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
        AND szg.dt_adm_fec_resol_scoring_zona_gris is NOT null
    THEN szg.cod_adm_estado_scoring_zona_gris
    when esre.dt_adm_fec_log IS NOT NULL
        and ecvc.dt_adm_fec_log IS NULL
        AND esre.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
        AND szg.dt_adm_fec_resol_scoring_zona_gris is NOT null
    THEN szg.cod_adm_estado_scoring_zona_gris
    when esre.dt_adm_fec_log IS NOT NULL
        and ecvc.dt_adm_fec_log IS NULL
        AND esre.ds_adm_tipo_estado not IN ('ACEPTADA','DECLINADA')
        AND esre.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
        AND szg.dt_adm_fec_resol_scoring_zona_gris is NOT null
    THEN szg.cod_adm_estado_scoring_zona_gris
    when    esre.dt_adm_fec_log IS NULL
        and ecvc.dt_adm_fec_log IS not NULL
        AND ecvc.ds_adm_tipo_estado not IN ('ACEPTADA','DECLINADA')
        AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
        AND szg.dt_adm_fec_resol_scoring_zona_gris is NOT null
    THEN szg.cod_adm_estado_scoring_zona_gris
	WHEN ecvc.dt_adm_fec_log IS NOT NULL
        AND ecvc.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
        AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
        AND ecvc.dt_adm_fec_log > esre.dt_adm_fec_log
        THEN ecvc.cod_adm_estado_accion
    WHEN ecvc.dt_adm_fec_log IS NOT NULL
        AND ecvc.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
        AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
        AND esre.dt_adm_fec_log IS NULL
        THEN ecvc.cod_adm_estado_accion
    WHEN ecvc.dt_adm_fec_log IS NULL
        AND esre.dt_adm_fec_log IS NULL
        AND szg.dt_adm_fec_resol_scoring_zona_gris IS NOT NULL
    THEN szg.cod_adm_estado_scoring_zona_gris
    WHEN ecvc.dt_adm_fec_log IS NOT NULL  --when agregado please work
        and esre.dt_adm_fec_log IS NOT NULL
        AND esre.ds_adm_tipo_estado not IN ('ACEPTADA','DECLINADA')
        AND ecvc.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
        AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
        AND ecvc.dt_adm_fec_log < esre.dt_adm_fec_log
    THEN ecvc.cod_adm_estado_accion
    WHEN ecvc.dt_adm_fec_log IS NOT NULL --when agregado please work
        and esre.dt_adm_fec_log IS NOT NULL
        AND esre.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
        AND ecvc.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
    THEN szg.cod_adm_estado_scoring_zona_gris
    WHEN ecvc.dt_adm_fec_log IS NOT NULL --when agregado please work
        and esre.dt_adm_fec_log IS NOT NULL
        AND ecvc.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
        AND esre.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
    THEN szg.cod_adm_estado_scoring_zona_gris
    ELSE ''
    END as string) AS cod_adm_estado_ultima_resolucion_riesgos
from bi_corp_staging.sge_propuesta p
left join estadospropuestasimplificadobysector ecvc on ecvc.cod_adm_nro_prop = cast(p.nro_prop as int) and ecvc.ds_adm_sector_estado = 'CVC'
left join estadospropuestasimplificadobysector esre on esre.cod_adm_nro_prop = cast(p.nro_prop as int) and esre.ds_adm_sector_estado = 'SUPERVISION'
left join resol_scoring_zonagris szg on szg.cod_adm_nro_prop = cast(p.nro_prop as int)
where p.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}';


-- temporal table to calculate cod_adm_motivo_sup_no_forzaje
CREATE TEMPORARY TABLE motivos_sup_no_forzaje AS
select
    d.nro_prop as nro_prop,
    CASE
        when UPPER(d.cod_motivo1_sup) in ('A01', 'A02', 'A03', 'A06', 'A07', 'A08', 'A09', 'A11', 'A12', 'A14', 'A17', 'A19', 'A29', 'A20', 'A21', 'A22', 'A23', 'A24', 'A25', 'A26', 'A27', 'A28', 'A30', 'A04', 'A10', 'A13', 'A16')
            then 1
        when UPPER(d.cod_motivo2_sup) in ('A01', 'A02', 'A03', 'A06', 'A07', 'A08', 'A09', 'A11', 'A12','A14',	'A17', 'A19', 'A29', 'A20', 'A21', 'A22', 'A23', 'A24', 'A25', 'A26', 'A27', 'A28', 'A30', 'A04', 'A10', 'A13', 'A16')
            then 1
        when UPPER(d.cod_motivo3_sup) in ('A01', 'A02', 'A03', 'A06', 'A07', 'A08', 'A09', 'A11', 'A12', 'A14', 'A17', 'A19', 'A29', 'A20', 'A21', 'A22', 'A23', 'A24', 'A25', 'A26', 'A27', 'A28', 'A30', 'A04', 'A10', 'A13', 'A16')
            then 1
        when UPPER(d.cod_motivo4_sup) in ('A01', 'A02', 'A03', 'A06', 'A07', 'A08', 'A09', 'A11', 'A12', 'A14', 'A17', 'A19', 'A29', 'A20', 'A21', 'A22', 'A23', 'A24', 'A25', 'A26', 'A27', 'A28', 'A30','A04', 'A10', 'A13', 'A16')
            then 1
        ELSE 0
        END as flag_cod_adm_motivo_sup_no_forzaje
from bi_corp_staging.sge_propuesta p
left join bi_corp_staging.sge_stnd_datos_adicionales_prop d on d.nro_prop = p.nro_prop and d.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'
where p.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}';


INSERT OVERWRITE TABLE bi_corp_common.stk_adm_propuestas
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}')
select distinct
    -- campos de propuesta
    concat('F485', lpad(p.nro_prop, 16 , "0")) as cod_adm_tramite,
    cast(p.nro_prop as int) as cod_adm_nro_prop,
    p.fec_prop as ts_adm_fec_prop,

    lim_lineas.fc_adm_limite_solicitado as fc_adm_limite_solicitado,
    lim_lineas.fc_adm_limite_aprobado as fc_adm_limite_aprobado,
    lim_lineas.fc_adm_limite_solicitado_sin_linea_individuos as fc_adm_limite_solicitado_sin_linea_individuos,
    lim_lineas.fc_adm_limite_aprobado_sin_linea_individuos as fc_adm_limite_aprobado_sin_linea_individuos,

    p.nro_grupo as cod_adm_nro_grupo,
    p.fec_vto_prop as ts_adm_fec_vto_prop,
    cast(p.penumper as int) as cod_adm_penumper,
    p.tpo_prop as cod_adm_tpo_prop,
    p.obs_fondos as ds_adm_obs_fondos,
    p.fec_paso_ries as ts_adm_fec_paso_ries,
    p.gerente_suc as cod_adm_gerente_suc,
    cast(p.cuit_cliente as bigint) as int_adm_cuit_cliente,
    p.pecdgent as cod_adm_pecdgent,
    p.resp_cta as cod_adm_resp_cta,
    p.obs_estado as ds_adm_obs_estado,
    p.origen as cod_adm_origen,
    cast(pe.resp_comercial as int) as cod_adm_resp_comercial,
    cast(p.resp_riesgos as int) as cod_adm_resp_riesgos,
    p.resp_comite as cod_adm_resp_comite,
    p.fec_comite as ts_adm_fec_comite,
    cast(p.pos_acre as double) as dec_adm_pos_acre,
    p.fec_pos_acre as ts_adm_fec_pos_acre,
    p.peusualt as cod_adm_peusualt,
    p.pefecalt as ts_adm_pefecalt,
    p.peusumod as cod_adm_peusumod,
    p.estado_prop as cod_adm_estado_prop,
    p.fec_est_prop as ts_adm_fec_est_prop,
    cast(p.rorac_cc as double) as dec_adm_rorac_cc,
    cast(p.rorac_sc as double) as dec_adm_rorac_sc,
    p.pefecmod as ts_adm_pefecmod,
    cast(p.fecha_deuda_id_1 as int) as id_adm_fecha_deuda_1,
    cast(p.fecha_deuda_id_2 as int) as id_adm_fecha_deuda_2,
    cast(p.eoaf_id as int) as id_adm_eoaf,
    cast(p.id_cashflow as int) as id_adm_cashflow,
    p.fecha_ventas_hist_desde as ts_adm_fecha_ventas_hist_desde,
    p.fecha_ventas_hist_hasta as ts_adm_fecha_ventas_hist_hasta,
    p.fecha_ventas_est_1 as ts_adm_fecha_ventas_est_1,
    p.fecha_ventas_est_2 as ts_adm_fecha_ventas_est_2,
    cast(p.cantidad_meses_est as int) as int_adm_cantidad_meses_est,
    p.resp_actual as cod_adm_resp_actual,
    cast(p.pegfeve as int) as cod_adm_pegfeve,
    cast(p.id_rentabilidad as int) as id_adm_rentabilidad,
    cast(p.id_acta as int) as id_adm_acta,
    p.modif_vo as cod_adm_modif_vo,
    p.excluir_vo as flag_adm_excluir_vo,
    cast(p.id_proyeccion as int) as id_adm_proyeccion,
    p.comite_esp as flag_adm_comite_esp,
    p.fecha_env_esp as ts_adm_fecha_env_esp,
    p.fecha_recep_esp as ts_adm_fecha_recep_esp,
    p.mar_refinanciacion as flag_adm_mar_refinanciacion,
    p.mar_cesionado as flag_adm_mar_cesionado,
    p.cod_campania as cod_adm_campania,
    p.cod_estado_accion as cod_adm_estado_accion,
    p.cod_estado_origen as cod_adm_estado_origen,
    p.obs_baja as ds_adm_obs_baja,
    p.obs_ultimo_estado_stnd as ds_adm_obs_ultimo_estado_stnd,
    p.cod_usuario_net as cod_adm_usuario_net,
    p.tpo_supervision as flag_adm_tpo_supervision,
    p.fec_vigencia_campania as ts_adm_fec_vigencia_campania,
    p.mar_migrada as flag_adm_mar_migrada,
    cast(p.nro_pglobal as int) as cod_adm_nro_pglobal,
    p.tipo_promocion as ds_adm_tipo_promocion,
    p.campania_desc as ds_adm_campania,
    p.sub_origen as cod_adm_sub_origen,
    p.cuadrante as cod_adm_cuadrante,

    -- campos de personas
    pe.petipdoc as cod_adm_petipdoc,
    cast(pe.penumdoc as bigint) as cod_adm_penumdoc,
    pe.penomfan as cod_adm_penomfan,
    pe.pepriape as cod_adm_pepriape,
    pe.pesegape as cod_adm_pesegape,
    pe.penomper as cod_adm_penomper,
    pe.pefecing as ts_adm_pefecing,
    pe.peestciv as flag_adm_peestciv,
    pe.pesexper as flag_adm_pesexper,
    pe.petipper as cod_adm_petipper,
    pe.pefecnac as ts_adm_pefecnac,
    pe.pefecini as ts_adm_pefecini,
    pe.pecancap as cod_adm_pecancap,
    pe.pepaiori as cod_adm_pepaiori,
    pe.penacper as cod_adm_penacper,
    pe.pepaires as cod_adm_pepaires,
    pe.petipocu as cod_adm_petipocu,
    pe.pecodact as cod_adm_pecodact,
    cast(pe.petieres as int) as cod_adm_petieres,
    pe.peforjur as cod_adm_peforjur,
    pe.penatjur as cod_adm_penatjur,
    pe.peestper as cod_adm_peestper,
    pe.peconper as cod_adm_peconper,
    pe.pecodemp as cod_adm_pecodemp,
    pe.peindno2 as flag_adm_peindno2,
    pe.peactriu as cod_adm_peactriu,
    pe.pefecrev as ts_adm_pefecrev,
    pe.peidioma as flag_adm_peidioma,
    pe.petermod as cod_adm_petermod,
    pe.psucmod as cod_adm_psucmod,
    pe.pehstamp as cod_adm_pehstamp,
    pe.penumgru as cod_adm_penumgru,
    pe.peuserid as cod_adm_peuserid,
    pe.petipemp as cod_adm_petipemp,
    pe.petieblc as flag_adm_petieblc,
    pe.segmento as cod_adm_segmento,
    pe.numgruant as cod_adm_numgruant,
    pe.perescom as cod_adm_perescom,
    pe.peobserv as cod_adm_peobserv,
    pe.perubro as cod_adm_rubrosectoreconomico,
    cast(pe.nup as int) as cod_per_nup,
    cast(pe.situacion as int) as cod_adm_situacion,
    pe.cod_iva as cod_adm_iva,
    pe.cod_iibb as cod_adm_iibb,
    pe.actualizacion_host as flag_adm_actualizacion_host,
    pe.mar_global as flag_adm_mar_global,
    cast(pe.val_nivelger_item1 as int) as cod_adm_val_nivelger_item1,
    cast(pe.val_nivelger_item2 as int) as cod_adm_val_nivelger_item2,
    cast(pe.val_nivelger_item3 as int) as cod_adm_val_nivelger_item3,
    cast(pe.val_nivelger_item4 as int) as cod_adm_val_nivelger_item4,
    cast(pe.val_nivelger_item5 as int) as cod_adm_val_nivelger_item5,
    cast(pe.val_nivelger_item6 as int) as cod_adm_val_nivelger_item6,
    cast(pe.can_pers_ocupado as int) as int_adm_can_pers_ocupado,
    pe.des_censo as ds_adm_censo,
    pe.fec_carga_censo as ts_adm_fec_carga_censo,
    pe.fec_venc_censo as ts_adm_fec_venc_censo,
    pe.cod_actecon as cod_adm_actecon,
    pe.pecodpai_nac as cod_adm_pecodpai_nac,
    cast(pe.nro_solicitud_atril as int) as cod_adm_nro_solicitud_atril,
    pe.fec_inactivacion as ts_adm_fec_inactivacion,
    pe.cod_usu_inactivacion as cod_adm_usu_inactivacion,
    pe.mar_uge as flag_adm_mar_uge,
    pe.mar_afip_ok as flag_adm_mar_afip_ok,
    pe.fecha_afip_ok as ts_adm_fecha_afip_ok,
    pe.mar_contrato_marco as flag_adm_mar_contrato_marco,
    pe.nivel_estudio as cod_adm_nivel_estudio,
    pe.tipo_vivienda as flag_adm_tipo_vivienda,
    pe.desc_actividad_bma as ds_adm_actividad_bma,
    cast(pe.cod_actividad_santander as int) as cod_adm_actividad_santander,
    pe.marca_resolucion_3337 as flag_adm_marca_resolucion_3337,
    pe.marca_impuesto_endeudamiento as flag_adm_marca_impuesto_endeudamiento,
    cast(pe.sucursal_cta_cte_bma as int) as cod_adm_sucursal_cta_cte_bma,
    cast(pe.tipo_cta_cte_bma as int) as cod_adm_tipo_cta_cte_bma,
    cast(pe.numero_cta_cte_bma as int) as cod_adm_numero_cta_cte_bma,
    cast(pe.monto_facturacion as double) as fc_adm_monto_facturacion,
    pe.com5603 as cod_adm_com5603,
    pe.cod_act_bma as cod_adm_act_bma,
    pe.resp_riesgos_por_omision as cod_adm_resp_riesgos_por_omision,
    pe.segmento_por_omision as cod_adm_segmento_por_omision,
    pe.segmento_riesgo as cod_adm_segmento_riesgo,

    -- campos de datos adicionales
    d.cod_resol_cvc as cod_adm_resol_cvc,
    d.cod_resol_sup as cod_adm_resol_sup,
    d.obs_resol_cvc as ds_adm_obs_resol_cvc,
    d.obs_resol_sup as ds_adm_obs_resol_sup,
    d.usu_codigo as cod_adm_usu,
    cast(d.lote_num as int) as cod_adm_lote_num,
    d.cod_motivo_cvc as cod_adm_motivo_cvc,
    d.lote_fecha as dt_adm_lote_fecha,
    cast(d.mon_valoracion_final as double) as fc_adm_mon_valoracion_final,
    cast(d.mon_sw_final as double) as fc_adm_mon_sw_final,
    cast(d.porc_exceso as int) as int_adm_porc_exceso,
    cast(d.mon_valoracion_financiera as double) as fc_adm_mon_valoracion_financiera,
    d.cod_canal_origen as cod_adm_canal_origen,
    d.cod_analista_srs as cod_adm_analista_srs,
    d.cod_supervisor_srs as cod_adm_supervisor_srs,
    d.cod_analista_cvc as cod_adm_analista_cvc,
    d.cod_supervisor_cvc as cod_adm_supervisor_cvc,
    d.cod_analista_sup as cod_adm_analista_sup,
    d.cod_supervisor_sup as cod_adm_supervisor_sup,
    d.cod_canal_destino as cod_adm_canal_destino,
    d.cod_resol_srs as cod_adm_resol_srs,
    d.obs_resol_srs as ds_adm_obs_resol_srs,
    d.mar_reutiliza_veraz as flag_adm_mar_reutiliza_veraz,
    d.fec_carga as ts_adm_fec_carga,
    d.fec_cierre as ts_adm_fec_cierre,
    d.fec_envio_sw as ts_adm_fec_envio_sw,
    d.fec_respuesta_sw as ts_adm_fec_respuesta_sw,
    d.fec_ini_resol_zg as ts_adm_fec_ini_resol_zg,
    d.fec_fin_resol_zg as ts_adm_fec_fin_resol_zg,
    d.fec_ini_resol_cvc as ts_adm_fec_ini_resol_cvc,
    d.fec_fin_resol_cvc as ts_adm_fec_fin_resol_cvc,
    d.fec_ini_resol_sup as ts_adm_fec_ini_resol_sup,
    d.fec_fin_resol_sup as ts_adm_fec_fin_resol_sup,
    d.fec_envio_veraz as ts_adm_fec_envio_veraz,
    d.fec_respuesta_veraz as ts_adm_fec_respuesta_veraz,
    d.mar_inf_calificacion as flag_adm_mar_inf_calificacion,
    d.mar_alta_por_pyme as flag_adm_mar_alta_por_pyme,
    d.val_mapa_riesgo as cod_adm_val_mapa_riesgo,
    d.val_fecha_mapa as dt_adm_val_fecha_mapa,
    d.origen_suc as cod_adm_origen_suc,
    d.mar_reca_obligatoria as flag_adm_mar_reca_obligatoria,
    d.indicador_riesgo as cod_adm_indicador_riesgo,
    cast(d.mrat as int) as int_adm_mrat,
    d.mar_baja_cent_ant as flag_adm_mar_baja_cent_ant,
    d.mar_baja_centralizada as flag_adm_mar_baja_centralizada,
    d.segmento_f485 as cod_adm_segmento_f485,
    d.mar_paquetizacion as flag_adm_mar_paquetizacion,
    d.cod_motivo1_sup as cod_adm_motivo1_sup,
    d.cod_motivo2_sup as cod_adm_motivo2_sup,
    d.cod_motivo3_sup as cod_adm_motivo3_sup,
    d.cod_motivo4_sup as cod_adm_motivo4_sup,
    d.cod_motivo1_cvc as cod_adm_motivo1_cvc,
    d.cod_motivo2_cvc as cod_adm_motivo2_cvc,
    d.cod_motivo3_cvc as cod_adm_motivo3_cvc,
    d.cod_motivo4_cvc as cod_adm_motivo4_cvc,
    d.canal_om as flag_adm_canal_om,

    -- campos de estados propuesta
    e.ds_adm_estado_prop as ds_adm_estado,
    e.ds_adm_sector_estado as ds_adm_sector_estado,
    e.ds_adm_tipo_estado as ds_adm_tipo_estado,

    -- resolucion scoring
    escoring.ds_adm_tipo_estado as ds_adm_estado_resolucion_scoring,
    escoring.dt_adm_fec_log as ts_adm_fec_resol_scoring,
    escoring.cod_adm_estado_accion as cod_adm_estado_scoring,
    escoring.ds_adm_sector_estado as cod_adm_sector_scoring,

    -- resolucion zona gris
    ezg.ds_adm_tipo_estado as ds_adm_estado_resolucion_zona_gris,
    ezg.dt_adm_fec_log as ts_adm_fec_resol_zona_gris,
    ezg.cod_adm_estado_accion as cod_adm_estado_zona_gris,
    ezg.ds_adm_sector_estado as cod_adm_sector_zona_gris,

    -- resolucion scoring + zona gris from temporal table resol_scoring_zonagris szg
    szg.ds_adm_estado_resolucion_scoring_zona_gris as ds_adm_estado_resolucion_scoring_zona_gris,
    szg.dt_adm_fec_resol_scoring_zona_gris as ts_adm_fec_resol_scoring_zona_gris,
    szg.cod_adm_estado_scoring_zona_gris as cod_adm_estado_scoring_zona_gris,
    szg.cod_adm_sector_scoring_zona_gris as cod_adm_sector_scoring_zona_gris,

    -- resolucion cvc
    ecvc.ds_adm_tipo_estado as ds_adm_estado_resolucion_ecvc,
    ecvc.dt_adm_fec_log as ts_adm_fec_resol_ecvc,
    ecvc.cod_adm_estado_accion as cod_adm_estado_ecvc,
    ecvc.ds_adm_sector_estado as cod_adm_sector_ecvc,

    -- resolucion supervision
    esre.ds_adm_tipo_estado as ds_adm_estado_resolucion_sre,
    esre.dt_adm_fec_log as ts_adm_fec_resol_sre,
    esre.cod_adm_estado_accion as cod_adm_estado_sre,
    esre.ds_adm_sector_estado as cod_adm_sector_sre,

    -- ultima resolución entre Scoring, zona gris, cvc y Supervisión
    cast(CASE
        WHEN ecvc.dt_adm_fec_log IS NOT NULL   --WHEN modificado
            AND esre.dt_adm_fec_log IS NOT NULL
            AND esre.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND ecvc.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND esre.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log < esre.dt_adm_fec_log
        THEN esre.ds_adm_tipo_estado
        WHEN ecvc.dt_adm_fec_log IS NOT NULL   --WHEN modificado
            AND esre.dt_adm_fec_log IS NOT NULL
            AND esre.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND esre.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log < esre.dt_adm_fec_log
        THEN esre.ds_adm_tipo_estado
		WHEN ecvc.dt_adm_fec_log IS NOT NULL
            AND esre.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND ecvc.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log > esre.dt_adm_fec_log
        THEN ecvc.ds_adm_tipo_estado
		WHEN ecvc.dt_adm_fec_log IS NOT NULL
            and esre.dt_adm_fec_log IS NOT NULL
			AND ecvc.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log > esre.dt_adm_fec_log
        THEN ecvc.ds_adm_tipo_estado
        WHEN esre.dt_adm_fec_log IS NOT NULL  --WHEN modificado
            AND esre.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND ecvc.ds_adm_tipo_estado not IN ('ACEPTADA','DECLINADA')
            AND esre.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log < esre.dt_adm_fec_log
        THEN esre.ds_adm_tipo_estado
        WHEN esre.dt_adm_fec_log IS NOT NULL  --WHEN modificado
            AND esre.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND ecvc.ds_adm_tipo_estado not IN ('ACEPTADA','DECLINADA')
            AND esre.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log > esre.dt_adm_fec_log
        THEN esre.ds_adm_tipo_estado
        WHEN esre.dt_adm_fec_log IS NOT NULL  --WHEN agregado
            AND esre.ds_adm_tipo_estado not IN ('ACEPTADA','DECLINADA')
            AND ecvc.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log > esre.dt_adm_fec_log
        THEN ecvc.ds_adm_tipo_estado
        WHEN ecvc.dt_adm_fec_log IS NOT NULL
            AND esre.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND ecvc.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log < esre.dt_adm_fec_log
        THEN szg.ds_adm_estado_resolucion_scoring_zona_gris
        WHEN ecvc.dt_adm_fec_log IS NOT NULL
            AND ecvc.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND esre.dt_adm_fec_log is NULL
        THEN szg.ds_adm_estado_resolucion_scoring_zona_gris
        WHEN ecvc.dt_adm_fec_log IS NOT NULL
            AND ecvc.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
            AND esre.dt_adm_fec_log is NULL
        THEN szg.ds_adm_estado_resolucion_scoring_zona_gris
        WHEN esre.dt_adm_fec_log IS NOT NULL
            AND esre.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND esre.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log is NULL
        THEN szg.ds_adm_estado_resolucion_scoring_zona_gris
        WHEN esre.dt_adm_fec_log IS NOT NULL
            AND esre.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND esre.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log is NULL
		THEN szg.ds_adm_estado_resolucion_scoring_zona_gris
		WHEN ecvc.dt_adm_fec_log IS NOT NULL --when agregado
            AND ecvc.dt_adm_fec_log is not null
            AND ecvc.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
            AND esre.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
        THEN szg.ds_adm_estado_resolucion_scoring_zona_gris
        WHEN ecvc.dt_adm_fec_log IS  NULL --when agregado
            AND esre.dt_adm_fec_log is not null
            AND esre.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
        THEN szg.ds_adm_estado_resolucion_scoring_zona_gris
        WHEN esre.dt_adm_fec_log IS  NULL --when agregado
            AND ecvc.dt_adm_fec_log is not null
            AND ecvc.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
        THEN szg.ds_adm_estado_resolucion_scoring_zona_gris
        WHEN ecvc.dt_adm_fec_log IS NOT NULL
            AND esre.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND ecvc.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log > esre.dt_adm_fec_log
        THEN szg.ds_adm_estado_resolucion_scoring_zona_gris
        WHEN esre.dt_adm_fec_log IS NOT NULL
            AND esre.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND esre.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log is NULL
        THEN esre.ds_adm_tipo_estado
        WHEN ecvc.dt_adm_fec_log IS NOT NULL
            AND ecvc.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND esre.dt_adm_fec_log is NULL
        THEN ecvc.ds_adm_tipo_estado
        WHEN ecvc.dt_adm_fec_log IS NOT NULL
            AND ecvc.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log > esre.dt_adm_fec_log
            AND esre.dt_adm_fec_log is NOT NULL
        THEN ecvc.ds_adm_tipo_estado
		WHEN ecvc.dt_adm_fec_log IS NULL
            AND esre.dt_adm_fec_log IS NULL
            AND szg.dt_adm_fec_resol_scoring_zona_gris IS NOT NULL
        THEN szg.ds_adm_estado_resolucion_scoring_zona_gris
		WHEN ecvc.dt_adm_fec_log IS NOT NULL  --when agregado please work
            and esre.dt_adm_fec_log IS NOT NULL
            AND esre.ds_adm_tipo_estado not IN ('ACEPTADA','DECLINADA')
			AND ecvc.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log < esre.dt_adm_fec_log
        THEN ecvc.ds_adm_tipo_estado
        WHEN ecvc.dt_adm_fec_log IS NOT NULL --when agregado please work
            and esre.dt_adm_fec_log IS NOT NULL
			AND esre.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
        THEN szg.ds_adm_estado_resolucion_scoring_zona_gris
        WHEN ecvc.dt_adm_fec_log IS NOT NULL --when agregado please work
            and esre.dt_adm_fec_log IS NOT NULL
			AND ecvc.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND esre.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
        THEN szg.ds_adm_estado_resolucion_scoring_zona_gris

        ELSE ''
    END as string) AS ds_adm_estado_ultima_resolucion_riesgos,

	cast(CASE
        WHEN ecvc.dt_adm_fec_log IS NOT NULL   --WHEN modificado
		   AND esre.dt_adm_fec_log IS NOT NULL
		   AND esre.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND ecvc.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND esre.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log < esre.dt_adm_fec_log
        THEN esre.dt_adm_fec_log
        WHEN ecvc.dt_adm_fec_log IS NOT NULL   --WHEN modificado
		   AND esre.dt_adm_fec_log IS NOT NULL
		   AND esre.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND esre.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log < esre.dt_adm_fec_log
        THEN esre.dt_adm_fec_log
        WHEN ecvc.dt_adm_fec_log IS NOT NULL
            AND esre.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND ecvc.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log > esre.dt_adm_fec_log
        THEN ecvc.dt_adm_fec_log
        WHEN ecvc.dt_adm_fec_log IS NOT NULL
			and esre.dt_adm_fec_log IS NOT NULL
			AND ecvc.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log > esre.dt_adm_fec_log
        THEN ecvc.dt_adm_fec_log
        WHEN esre.dt_adm_fec_log IS NOT NULL
            AND esre.ds_adm_tipo_estado  IN ('ACEPTADA','DECLINADA')
            AND ecvc.ds_adm_tipo_estado not IN ('ACEPTADA','DECLINADA')
            AND esre.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log < esre.dt_adm_fec_log
        THEN esre.dt_adm_fec_log
        WHEN esre.dt_adm_fec_log IS NOT NULL  --WHEN agregado
            AND esre.ds_adm_tipo_estado not IN ('ACEPTADA','DECLINADA')
            AND ecvc.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log > esre.dt_adm_fec_log
        THEN ecvc.dt_adm_fec_log
		WHEN ecvc.dt_adm_fec_log IS NOT NULL
            AND esre.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND ecvc.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log < esre.dt_adm_fec_log
        THEN szg.dt_adm_fec_resol_scoring_zona_gris
        WHEN ecvc.dt_adm_fec_log IS NOT NULL
            AND ecvc.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND esre.dt_adm_fec_log is NULL
        THEN szg.dt_adm_fec_resol_scoring_zona_gris
        WHEN ecvc.dt_adm_fec_log IS NOT NULL --when agregado
            AND ecvc.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
            AND esre.dt_adm_fec_log is NULL
        THEN szg.dt_adm_fec_resol_scoring_zona_gris
        WHEN ecvc.dt_adm_fec_log IS NOT NULL --when agregado
            AND ecvc.dt_adm_fec_log is not null
            AND ecvc.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
            AND esre.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
        THEN szg.dt_adm_fec_resol_scoring_zona_gris
        WHEN ecvc.dt_adm_fec_log IS  NULL --when agregado
            AND esre.dt_adm_fec_log is not null
            AND esre.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
        THEN szg.dt_adm_fec_resol_scoring_zona_gris
        WHEN esre.dt_adm_fec_log IS  NULL --when agregado
            AND ecvc.dt_adm_fec_log is not null
            AND ecvc.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
        THEN szg.dt_adm_fec_resol_scoring_zona_gris
        WHEN esre.dt_adm_fec_log IS NOT NULL
            AND esre.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND esre.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log is NULL
        THEN szg.dt_adm_fec_resol_scoring_zona_gris
        WHEN esre.dt_adm_fec_log IS NOT NULL --when agregado
            AND esre.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND esre.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log is NULL
        THEN szg.dt_adm_fec_resol_scoring_zona_gris
        WHEN ecvc.dt_adm_fec_log IS NOT NULL
            AND esre.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND ecvc.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log > esre.dt_adm_fec_log
        THEN szg.dt_adm_fec_resol_scoring_zona_gris
        WHEN esre.dt_adm_fec_log IS NOT NULL
            AND esre.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND esre.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log is NULL
        THEN esre.dt_adm_fec_log
        WHEN ecvc.dt_adm_fec_log IS NOT NULL
            AND ecvc.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND esre.dt_adm_fec_log is NULL
        THEN ecvc.dt_adm_fec_log
        WHEN ecvc.dt_adm_fec_log IS NOT NULL --WHEN AGREGADO
            AND ecvc.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log > esre.dt_adm_fec_log
            AND esre.dt_adm_fec_log is NOT NULL
        THEN ecvc.dt_adm_fec_log
        WHEN ecvc.dt_adm_fec_log IS NULL
            AND esre.dt_adm_fec_log IS NULL
            AND szg.dt_adm_fec_resol_scoring_zona_gris IS NOT NULL
        THEN szg.dt_adm_fec_resol_scoring_zona_gris
		WHEN ecvc.dt_adm_fec_log IS NOT NULL  --when agregado please work
            and esre.dt_adm_fec_log IS NOT NULL
            AND esre.ds_adm_tipo_estado not IN ('ACEPTADA','DECLINADA')
			AND ecvc.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log < esre.dt_adm_fec_log
        THEN ecvc.dt_adm_fec_log
        WHEN ecvc.dt_adm_fec_log IS NOT NULL --when agregado please work
            and esre.dt_adm_fec_log IS NOT NULL
			AND esre.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
        THEN szg.dt_adm_fec_resol_scoring_zona_gris
        WHEN ecvc.dt_adm_fec_log IS NOT NULL --when agregado please work
            and esre.dt_adm_fec_log IS NOT NULL
			AND ecvc.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND esre.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
        THEN szg.dt_adm_fec_resol_scoring_zona_gris

        ELSE ''
    END as string) AS ts_adm_fec_resol_ultima_resolucion_riesgos,

    -- from temporal table cod_ultimaresolucionriesgos
    ultresolriesgos.cod_adm_estado_ultima_resolucion_riesgos AS cod_adm_estado_ultima_resolucion_riesgos,

    cast( CASE
        when ecvc.dt_adm_fec_log IS  NULL  --WHEN AGREGADO
            AND esre.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND esre.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
        THEN 'SRE'
        when esre.dt_adm_fec_log IS  NULL --WHEN AGREGADO
            AND ecvc.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
        THEN 'CVC'
        when ecvc.dt_adm_fec_log IS NOT NULL
            and esre.dt_adm_fec_log IS NOT NULL
            AND esre.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND ecvc.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log > esre.dt_adm_fec_log
        THEN 'CVC'
        when ecvc.dt_adm_fec_log IS NOT NULL --WHEN AGREGADO PLEASE WORK
            AND esre.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND ecvc.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log < esre.dt_adm_fec_log
        THEN 'CVC'
        when ecvc.dt_adm_fec_log IS NOT NULL
            AND esre.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND ecvc.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log < esre.dt_adm_fec_log
            AND szg.dt_adm_fec_resol_scoring_zona_gris is NOT null
        THEN szg.cod_adm_sector_scoring_zona_gris
        when ecvc.dt_adm_fec_log IS  NULL
            AND esre.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND esre.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND szg.dt_adm_fec_resol_scoring_zona_gris is NOT null
        THEN szg.cod_adm_sector_scoring_zona_gris
        when ecvc.dt_adm_fec_log IS  NULL
            AND esre.ds_adm_tipo_estado  IN ('ACEPTADA','DECLINADA')
            AND esre.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
            AND szg.dt_adm_fec_resol_scoring_zona_gris is NOT null
        THEN szg.cod_adm_sector_scoring_zona_gris
        when ecvc.dt_adm_fec_log IS  NULL
            AND esre.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND esre.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
            AND szg.dt_adm_fec_resol_scoring_zona_gris is NOT null
        THEN szg.cod_adm_sector_scoring_zona_gris
        when esre.dt_adm_fec_log IS NULL
            AND ecvc.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND szg.dt_adm_fec_resol_scoring_zona_gris is NOT null
        THEN szg.cod_adm_sector_scoring_zona_gris
        when esre.dt_adm_fec_log IS NULL
            AND ecvc.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
            AND szg.dt_adm_fec_resol_scoring_zona_gris is NOT null
        THEN szg.cod_adm_sector_scoring_zona_gris
        when esre.dt_adm_fec_log IS NULL
            AND ecvc.ds_adm_tipo_estado  IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
            AND szg.dt_adm_fec_resol_scoring_zona_gris is NOT null
        THEN szg.cod_adm_sector_scoring_zona_gris
        when ecvc.dt_adm_fec_log IS NOT NULL
            AND esre.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND ecvc.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log < esre.dt_adm_fec_log
            AND szg.dt_adm_fec_resol_scoring_zona_gris is null AND escoring.dt_adm_fec_log IS NOT NULL
        THEN 'SCORING'
        when ecvc.dt_adm_fec_log IS NOT NULL
            AND esre.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND ecvc.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log > esre.dt_adm_fec_log
            AND ezg.dt_adm_fec_log is NOT null
        THEN szg.cod_adm_sector_scoring_zona_gris
        when ecvc.dt_adm_fec_log IS NOT NULL
            AND esre.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND ecvc.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log > esre.dt_adm_fec_log
            AND ezg.dt_adm_fec_log is null AND escoring.dt_adm_fec_log IS NOT NULL
        THEN szg.cod_adm_sector_scoring_zona_gris
        when esre.dt_adm_fec_log IS NOT NULL
            AND esre.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND esre.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log < esre.dt_adm_fec_log
        THEN 'SRE'
        when ecvc.dt_adm_fec_log IS NOT NULL
            AND ecvc.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.cod_adm_estado_accion > esre.dt_adm_fec_log
        THEN 'CVC'
        when ecvc.dt_adm_fec_log IS NOT NULL
            and esre.dt_adm_fec_log IS NOT NULL
            AND esre.ds_adm_tipo_estado not IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
            AND szg.dt_adm_fec_resol_scoring_zona_gris < esre.dt_adm_fec_log
            AND szg.dt_adm_fec_resol_scoring_zona_gris is NOT null
        THEN szg.cod_adm_sector_scoring_zona_gris
        when ecvc.dt_adm_fec_log IS NOT NULL
            and esre.dt_adm_fec_log IS NOT NULL
            AND ecvc.ds_adm_tipo_estado not IN ('ACEPTADA','DECLINADA')
            AND esre.ds_adm_tipo_estado not IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
            AND szg.dt_adm_fec_resol_scoring_zona_gris < esre.dt_adm_fec_log
            AND szg.dt_adm_fec_resol_scoring_zona_gris < ecvc.dt_adm_fec_log
            AND szg.dt_adm_fec_resol_scoring_zona_gris is NOT null
        THEN szg.cod_adm_sector_scoring_zona_gris
        when ecvc.dt_adm_fec_log IS NULL
            AND esre.dt_adm_fec_log IS NULL
            AND szg.dt_adm_fec_resol_scoring_zona_gris IS NOT NULL
            AND ezg.dt_adm_fec_log is NOT null
        THEN 'ZONA GRIS'
        when ecvc.dt_adm_fec_log IS NULL
            AND esre.dt_adm_fec_log IS NULL
            AND ezg.dt_adm_fec_log is null
            AND escoring.dt_adm_fec_log IS NOT NULL
        THEN 'SCORING'
        WHEN ecvc.dt_adm_fec_log IS NOT NULL  --when agregado please work
            and esre.dt_adm_fec_log IS NOT NULL
            AND esre.ds_adm_tipo_estado not IN ('ACEPTADA','DECLINADA')
            AND ecvc.ds_adm_tipo_estado IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log > szg.dt_adm_fec_resol_scoring_zona_gris
            AND ecvc.dt_adm_fec_log < esre.dt_adm_fec_log
        THEN 'CVC'
        WHEN ecvc.dt_adm_fec_log IS NOT NULL --when agregado please work
            and esre.dt_adm_fec_log IS NOT NULL
            AND esre.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND ecvc.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
        THEN szg.cod_adm_sector_scoring_zona_gris
        WHEN ecvc.dt_adm_fec_log IS NOT NULL --when agregado please work
            and esre.dt_adm_fec_log IS NOT NULL
            AND ecvc.ds_adm_tipo_estado NOT IN ('ACEPTADA','DECLINADA')
            AND esre.dt_adm_fec_log < szg.dt_adm_fec_resol_scoring_zona_gris
        THEN szg.cod_adm_sector_scoring_zona_gris
        ELSE ''
    END as string) AS cod_adm_sector_ultima_resolucion_riesgos,


    -- field cod_adm_motivo_sup_no_forzaje from temporal table motivos_sup_no_forzaje msnf
    msnf.flag_cod_adm_motivo_sup_no_forzaje as flag_cod_adm_motivo_sup_no_forzaje,

    if(cast(p.cuit_cliente as int) >= 30000000000, 'P1', 'NEGOCIOS')
        as cod_adm_segmento_inferido,

    CASE
    WHEN p.origen = 'ENOR'
        THEN 'TRAMITE NORMAL'
	WHEN p.origen = 'PREA'
        THEN 'PREAPROBADOS'
    WHEN p.origen = 'EPRE' AND camp.codigo_clasificacion = 'PRO'
        THEN 'PROGRAMA DE VENTA'
    WHEN ORIGEN='EPRE' AND camp.codigo_clasificacion <> 'PRO'
        THEN 'PREAPROBADOS'
    ELSE ''
    END as ds_adm_tramite_f485,

    CASE
    WHEN p.origen = 'ENOR'
	    THEN 'TRAMITE NORMAL'
	WHEN p.origen = 'PREA'
        THEN camp.nombre_com
    WHEN p.origen = 'EPRE' AND camp.codigo_clasificacion = 'PRO'
        THEN camp.nombre_com
    WHEN p.origen = 'EPRE' AND camp.codigo_clasificacion <> 'PRO'
        THEN camp.nombre_com
    ELSE ''
    END as ds_adm_descripcion_campania_f485,

    CASE
    WHEN p.origen = 'ENOR'
        THEN 'TRAMITE NORMAL'
	WHEN p.origen = 'PREA'
        THEN camp.esquema
    WHEN p.origen = 'EPRE' AND camp.codigo_clasificacion = 'PRO'
        THEN camp.esquema
    WHEN p.origen = 'EPRE' AND camp.codigo_clasificacion <> 'PRO'
        THEN camp.esquema
    ELSE ''
    END as ds_adm_esquema_tramite,

    CASE
    WHEN p.origen = 'ENOR'
        THEN 'TRAMITE NORMAL'
	WHEN p.origen ='PREA'
        THEN camp.clasificacion
    WHEN p.origen ='EPRE' AND camp.codigo_clasificacion = 'PRO'
        THEN camp.clasificacion
    WHEN p.origen ='EPRE' AND camp.codigo_clasificacion <> 'PRO'
        THEN camp.clasificacion
    ELSE ''
    END as ds_adm_clasificacion_tramite,

    pp.cod_per_segmentoduro as cod_adm_segmentoduro,
    pp.ds_per_segmento as ds_adm_segmento,
    pp.ds_per_subsegmento as ds_adm_subsegmento

from bi_corp_staging.sge_propuesta p

left join limites_lineas lim_lineas on lim_lineas.cod_adm_nro_prop = cast(p.nro_prop as int)
left join bi_corp_staging.sge_personas pe on pe.penumper = p.penumper and pe.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'
left join bi_corp_staging.sge_stnd_datos_adicionales_prop d on d.nro_prop = p.nro_prop and d.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'

left join estadospropuestasimplificado e on
    e.cod_adm_nro_prop = cast(p.nro_prop as int)

left join estadospropuestasimplificadobysector escoring on
    escoring.cod_adm_nro_prop = cast(p.nro_prop as int) and
    escoring.ds_adm_sector_estado = 'SCORING'

left join estadospropuestasimplificadobysector ezg on
    ezg.cod_adm_nro_prop = cast(p.nro_prop as int) and
    ezg.ds_adm_sector_estado = 'ZONA GRIS'

left join estadospropuestasimplificadobysector ecvc on
    ecvc.cod_adm_nro_prop = cast(p.nro_prop as int) and
    ecvc.ds_adm_sector_estado = 'CVC'

left join estadospropuestasimplificadobysector esre on
    esre.cod_adm_nro_prop = cast(p.nro_prop as int) and
    esre.ds_adm_sector_estado = 'SUPERVISION'

left join resol_scoring_zonagris szg on szg.cod_adm_nro_prop = cast(p.nro_prop as int)
left join cod_ultimaresolucionriesgos ultresolriesgos on ultresolriesgos.cod_adm_nro_prop = cast(p.nro_prop as int)

left join personas_segmentoduro pp on pp.cod_per_nup =  cast(pe.nup as int)

left join motivos_sup_no_forzaje msnf on msnf.nro_prop = p.nro_prop

left join bi_corp_staging.risksql5_campanias camp on camp.codigo = p.cod_campania and camp.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'

where p.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}';