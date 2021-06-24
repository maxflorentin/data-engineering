set mapred.job.queue.name=root.dataeng;
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
WHERE p08.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt008', dag_id='LOAD_CMN_Tarjetas_Credito_Movimientos_Liq-Daily') }}'
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
WHERE p08.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt008', dag_id='LOAD_CMN_Tarjetas_Credito_Movimientos_Liq-Daily') }}'
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
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_amas_wamas02', dag_id='LOAD_CMN_Tarjetas_Credito_Movimientos_Liq-Daily') }}';


INSERT OVERWRITE TABLE bi_corp_common.bt_tcr_movimientos_liquidados
PARTITION(partition_date)
SELECT
    w310.310_cod_ope           AS cod_tcr_operacion,
    w310.310_nro_cta           AS cod_tcr_cuenta,
    s02.s02_rel_nro_cta        AS cod_nro_cuenta,
    w310.310_nro_tarj          AS cod_tcr_tarjeta,
    s02.s02_tipo_cuenta        AS cod_tcr_tipo_cuenta,
    cast(p08.penumper_tarjeta as int)              AS cod_per_nup_tarjeta,
    cast(p08.penumper_titular as int)            AS cod_per_nup_cuenta,
    "0072"                AS cod_tcr_entidad,
    w310.310_cod_sucu          AS cod_suc_sucursal,
    CASE w310.310_entidad
        WHEN 472 THEN "AMEX"
        WHEN 172 THEN "MASTER"
        WHEN 72 THEN "VISA"
        END                    AS cod_tcr_marca_tarjeta,
    w310.310_nro_comp          AS cod_tcr_comprobante,
    CASE WHEN trim(w310.310_moneda) IN ('','null','NULL') THEN null ELSE trim(w310.310_moneda) END            AS cod_div_divisa,
    w310.310_grp_afi           AS cod_tcr_grupo_afip,
    w310.310_cartera           AS cod_tcr_cartera,
    w310.310_nro_est           AS cod_tcr_establecimiento,
    CASE WHEN trim(w310.310_origen) IN ('','null','NULL') THEN null ELSE trim(w310.310_origen) END             AS cod_tcr_origen,
    CASE WHEN trim(w310.310_mk_agro) IN ('','null','NULL') THEN null ELSE trim(w310.310_mk_agro) END           AS ds_tcr_mk_agro,
    CASE WHEN trim(w310.310_mk_final) IN ('','null','NULL') THEN null ELSE trim(w310.310_mk_final) END          AS ds_tcr_mk_final,
    CASE
        WHEN w310.310_signo = "-" THEN (-1 * w310.310_importe)
        ELSE w310.310_importe
        END                    AS fc_tcr_importe,
    CASE
        WHEN w310.310_signo_arp = '-' THEN (-1 * w310.310_importe_arp)
        ELSE w310.310_importe_arp
        END                    AS fc_tcr_importe_arp,
    CASE
        WHEN w310.310_signo_usd = '-' THEN (-1 * w310.310_importe_usd)
        ELSE w310.310_importe_usd
        END                    AS fc_tcr_importe_usd,
    case when date_format(from_unixtime(unix_timestamp(SUBSTRING(CONCAT('000000', cast(w310.310_fec_pres_aammdd as string)), -6),'ddMMyy')),'yyyy-MM-dd') = '1999-11-30' then null else date_format(from_unixtime(unix_timestamp(SUBSTRING(CONCAT('000000', cast(w310.310_fec_pres_aammdd as string)), -6),'ddMMyy')),'yyyy-MM-dd') end AS dt_tcr_fecha_presentacion,
    date_format(from_unixtime(unix_timestamp(SUBSTRING(CONCAT('000000', cast(w310.310_fec_origen_aammdd as string)), -6),'ddMMyy')),'yyyy-MM-dd') AS dt_tcr_fecha_origen,
    w310.partition_date
FROM bi_corp_staging.aftc_waftc310 w310
LEFT JOIN wamas02_temp s02 ON s02.s02_nro_tarjeta = w310.310_nro_tarj
LEFT JOIN pedt008_final p08
    ON
    p08.penumcon = s02.s02_rel_nro_cta
    AND p08.peordpar = s02.s02_rel_nro_firm
    AND (CASE w310.310_entidad
        WHEN 472 THEN "42"
        WHEN 172 THEN "41"
        WHEN 72 THEN "40"
        END) = p08.pecodpro
WHERE w310.310_tpo_reg = 30
  AND w310.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Tarjetas_Credito_Movimientos_Liq-Daily') }}';

