SET mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;


DROP TABLE IF EXISTS pedt008_use;
CREATE TEMPORARY TABLE pedt008_use AS
select distinct penumper,pecalpar,peordpar,penumcon,pecodpro,pecodsub
from
(select
first_value(penumper) over(partition by pecalpar, peordpar, penumcon, pecodpro, pecodsub  order by peestrel asc, pefecbrb desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) penumper,
pecalpar,
peordpar,
penumcon,
pecodpro,
pecodsub
from bi_corp_staging.malpe_pedt008 p08
WHERE p08.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt008', dag_id='LOAD_CMN_Tarjetas_Credito_Movimientos_Fact-Daily') }}'
AND p08.pecodpro in ('40','41','42','43')) t;

DROP TABLE IF EXISTS pedt008_use_titular;
CREATE TEMPORARY TABLE pedt008_use_titular AS
select distinct penumper,penumcon,pecodpro,pecodsub
from
(select
first_value(penumper) over(partition by penumcon, pecodpro, pecodsub  order by peestrel asc, pefecbrb desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) penumper,
pecalpar,
peordpar,
penumcon,
pecodpro,
pecodsub
from bi_corp_staging.malpe_pedt008 p08
where p08.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt008', dag_id='LOAD_CMN_Tarjetas_Credito_Movimientos_Fact-Daily') }}'
AND p08.pecodpro in ('40','41','42','43')
AND p08.pecalpar = 'TI') t;

DROP TABLE IF EXISTS pedt008_intermedia;
CREATE TEMPORARY TABLE pedt008_intermedia AS
select p.penumper,p.pecalpar,p.peordpar,p.penumcon,p.pecodpro,p.pecodsub, tit.penumper penumper_titular
from pedt008_use p
left join pedt008_use_titular tit on tit.penumcon = p.penumcon and tit.pecodpro = p.pecodpro and tit.pecodsub = p.pecodsub;

DROP TABLE IF EXISTS pedt008_final;
CREATE TEMPORARY TABLE pedt008_final AS
select distinct penumper_tarjeta,penumper_titular,pecodpro,pecodsub,penumcon,peordpar
from (select
first_value(penumper) over(partition by pecodpro, penumcon, peordpar order by pecalpar asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) penumper_tarjeta,
first_value(penumper_titular) over(partition by pecodpro, penumcon, peordpar order by pecalpar asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) penumper_titular,
pecodpro,
pecodsub,
penumcon,
peordpar
from pedt008_intermedia) t;

DROP TABLE IF EXISTS wamas02_temp;
CREATE TEMPORARY TABLE wamas02_temp AS
select *
from bi_corp_staging.amas_wamas02
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_amas_wamas02', dag_id='LOAD_CMN_Tarjetas_Credito_Movimientos_Fact-Daily') }}';


INSERT OVERWRITE TABLE bi_corp_common.bt_tcr_movimientos_facturados
    PARTITION (partition_date)
SELECT
    w600.600_codop                  AS cod_tcr_operacion,
    s02.s02_cuenta_tc               AS cod_tcr_cuenta,
    s02_rel_nro_cta                 AS cod_nro_cuenta,
    trim(w600.600_tarjeta)                AS cod_tcr_tarjeta,
    w600.600_evento_origen          AS cod_tcr_evento_origen,
    CASE WHEN trim(w600.600_codaut) IN ('','null','NULL') THEN null ELSE  trim(w600.600_codaut) END               AS cod_tcr_autorizacion,
    CASE WHEN trim(w600.600_moneda) IN ('','null','NULL') THEN null ELSE trim(w600.600_moneda) END                 AS cod_div_divisa,
    date_format(
            from_unixtime(unix_timestamp(cast(w600.600_fpres AS string), 'yyyyMMdd')), 'yyyy-MM-dd'
        )                           AS dt_tcr_fecha_presentacion,
    date_format(
            from_unixtime(unix_timestamp(cast(w600.600_fpago AS string), 'yyyyMMdd')), 'yyyy-MM-dd'
        )                           AS dt_tcr_fecha_pago,
    s02.s02_tipo_cuenta             AS cod_tcr_tipo_cuenta,
    cast(p08.penumper_tarjeta as int)                   AS cod_per_nup_tarjeta,
    CASE w600.600_bcodest
        WHEN "472" THEN "AMEX" --42
        WHEN "172" THEN "MASTER" --41
        WHEN "072" THEN "VISA" --40
        WHEN "72" THEN "VISA" --40
        END                         AS cod_tcr_marca_tarjeta,
    cast(p08.penumper_titular as int)                AS cod_per_nup_cuenta,
    CASE WHEN trim(w600.600_numdoc) IN ('','null','NULL') THEN null ELSE trim(w600.600_numdoc) END                 AS cod_tcr_documento,
    CASE WHEN trim(w600.600_tipdoc) IN ('','null','NULL') THEN null ELSE trim(w600.600_tipdoc) END                 AS ds_tcr_tipo_documento,
    w600.600_evento                 AS cod_tcr_evento,
    CASE WHEN trim(w600.600_signo_evento) IN ('','null','NULL') THEN null ELSE trim(w600.600_signo_evento) END           AS ds_tcr_signo_evento,
    w600.600_evento_grupo           AS cod_tcr_evento_grupo,
    w600.600_orden                  AS cod_tcr_orden,
    "0072"                     AS cod_tcr_entidad,
    w600.600_casadest               AS cod_suc_sucursal,
    CASE WHEN trim(w600.600_marce_rlimpio) IN ('','null','NULL') THEN null ELSE trim(w600.600_marce_rlimpio) END          AS cod_tcr_marca_limpio,
    w600.600_bcodest_e_i            AS cod_tcr_entidad_destino_ei,
    w600.600_casadest_e_i           AS cod_suc_sucursal_destino_ei,
    w600.600_cartera_e_i            AS cod_tcr_cartera_ei,
    date_format(
            from_unixtime(unix_timestamp(cast(w600.600_fcierre AS string), 'yyyyMMdd')), 'yyyy-MM-dd'
        )                            AS dt_tcr_fecha_cierre,
    w600.600_plancuot               AS ds_tcr_plan_cuotas,
    w600.600_nro_cuota              AS int_tcr_nro_cuota,
    CASE WHEN trim(w600.600_producto) IN ('','null','NULL') THEN null ELSE trim(w600.600_producto) END               AS cod_tcr_producto,
    CASE WHEN trim(w600.600_producto_conc) IN ('','null','NULL') THEN null ELSE trim(w600.600_producto_conc) END          AS cod_tcr_producto_conc,
    w600.600_codop_orig             AS cod_tcr_operacion_origen,
    date_format(
            from_unixtime(unix_timestamp(cast(w600.600_fevento AS string), 'yyyyMMdd')), 'yyyy-MM-dd'
        )                           AS dt_tcr_fecha_evento,
    date_format(
            from_unixtime(unix_timestamp(cast(w600.600_fpres_orig  AS string), 'yyyyMMdd')), 'yyyy-MM-dd'
        )                           AS dt_tcr_fecha_presentacion_origen,
    date_format(
            from_unixtime(unix_timestamp(cast(w600.600_fpago_orig  AS string), 'yyyyMMdd')), 'yyyy-MM-dd'
        )                           AS dt_tcr_fecha_pago_origen,
    CASE WHEN trim(w600.600_numcomp) IN ('','null','NULL') THEN null ELSE trim(w600.600_numcomp) END               AS cod_tcr_comprobante,
    case when trim(w600.600_db_automat)='1' then 1 else 0 end             AS flag_tcr_debito_automatico,
    CASE WHEN trim(w600.600_term_captura) IN ('','null','NULL') THEN null ELSE trim(w600.600_term_captura) END          AS cod_tcr_term_captura,
    CASE WHEN trim(w600.600_atm) IN ('','null','NULL') THEN null ELSE trim(w600.600_atm) END                    AS cod_tcr_atm,
    w600.600_numest                 AS cod_tcr_establecimiento,
    w600.600_numcom                 AS cod_tcr_comercio,
    w600.600_bco_pagador            AS cod_tcr_entidad_pagadora,
    w600.600_bcoest                 AS cod_tcr_entidad_est,
    w600.600_sasaest                AS cod_suc_sucursal_est,
    w600.600_rubro                  AS cod_tcr_rubro,
    w600.600_codgeo_est             AS ds_tcr_geoestablecimiento,
    CASE WHEN trim(w600.600_denest) IN ('','null','NULL') THEN null ELSE trim(w600.600_denest) END                 AS ds_tcr_denominacion_establecimiento,
    CASE WHEN trim(w600.600_nomcdad) IN ('','null','NULL') THEN null ELSE trim(w600.600_nomcdad) END              AS ds_tcr_ciudad,
    CASE WHEN trim(w600.600_codpais) IN ('','null','NULL') THEN null ELSE trim(w600.600_codpais) END               AS ds_tcr_pais,
    CASE WHEN trim(w600.600_monorig) IN ('','null','NULL') THEN null ELSE trim(w600.600_monorig) END                AS cod_div_divisa_origen,
    CASE WHEN trim(w600.600_cinta) IN ('','null','NULL') THEN null ELSE trim(w600.600_cinta) END                  AS ds_tcr_cinta,
    CASE WHEN trim(w600.600_nro_recl_bco) IN ('','null','NULL') THEN null ELSE trim(w600.600_nro_recl_bco) END           AS ds_tcr_nro_recl_bco,
    w600.600_tna_emisor             AS ds_tna_emisor,
    CASE WHEN trim(regexp_replace(w600.600_cuit_estab, "^0+", '')) IN ('','null','NULL') THEN null ELSE trim(w600.600_cuit_estab) END            AS ds_tcr_cuit_establecimiento,
    CASE WHEN trim(w600.600_cod_campania_bco) IN ('','null','NULL') THEN null ELSE trim(w600.600_cod_campania_bco) END       AS cod_tcr_campania_bco,
    w600.600_cartera                AS cod_tcr_cartera,
    w600.600_importe_1              AS fc_tcr_importe1,
    w600.600_importe_2              AS fc_tcr_importe2,
    w600.600_importe_3              AS fc_tcr_importe3,
    w600.600_importe_4              AS fc_tcr_importe4,
    w600.600_imporig                AS fc_tcr_importe_origen,
    w600.600_imp_dto_usu            AS fc_tcr_importe_dto_usu,
    w600.600_imp_a_cargo_pag        AS fc_tcr_importe_a_cargo_pag,
    date_format(
            from_unixtime(unix_timestamp(cast(w600.600_forig AS string), 'yyyyMMdd')), 'yyyy-MM-dd'
        )                           AS dt_tcr_fecha_origen,
    w600.partition_date             AS partition_date
FROM bi_corp_staging.aftc_waftc600_facturados w600
LEFT JOIN wamas02_temp s02 ON s02.s02_nro_tarjeta = substr(w600.600_tarjeta,1,16)
LEFT JOIN pedt008_final p08
    ON
    p08.penumcon = s02.s02_rel_nro_cta
    AND p08.peordpar = s02.s02_rel_nro_firm
    AND (CASE w600.600_bcodest
        WHEN "472" THEN "42"
        WHEN "172" THEN "41"
        WHEN "072" THEN "40"
        WHEN "72" THEN "40"
        END) = p08.pecodpro
WHERE w600.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Tarjetas_Credito_Movimientos_Fact-Daily') }}';
