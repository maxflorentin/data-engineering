SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS max008;
CREATE TEMPORARY TABLE max008 AS
SELECT max(partition_date) AS partition_date
FROM bi_corp_staging.malpe_pedt008 p08
WHERE partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Tarjetas_Credito_Cuentas-Daily') }}',7)
AND partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Tarjetas_Credito_Cuentas-Daily') }}';

DROP TABLE IF EXISTS pedt008_use;
CREATE TEMPORARY TABLE pedt008_use AS
select distinct penumper,pecalpar,peordpar,penumcon,pecodpro,pecodsub
from
(select
first_value(penumper) over(partition by pecalpar, peordpar, penumcon, pecodpro, pecodsub  order by peestrel asc, pefecbrb desc, pefecalt asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) penumper,
pecalpar,
peordpar,
penumcon,
pecodpro,
pecodsub
from bi_corp_staging.malpe_pedt008 p08
INNER JOIN max008 mx8 ON p08.partition_date = mx8.partition_date
WHERE p08.partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Tarjetas_Credito_Cuentas-Daily') }}',7)
AND p08.partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Tarjetas_Credito_Cuentas-Daily') }}'
AND p08.pecodpro in ('40','41','42','43')) t;

DROP TABLE IF EXISTS pedt008_use_titular;
CREATE TEMPORARY TABLE pedt008_use_titular AS
select distinct penumper,penumcon,pecodpro,pecodsub
from
(select
first_value(penumper) over(partition by penumcon, pecodpro, pecodsub  order by peestrel asc, pefecbrb desc, peordpar asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) penumper,
pecalpar,
peordpar,
penumcon,
pecodpro,
pecodsub
from bi_corp_staging.malpe_pedt008 p08
INNER JOIN max008 mx8 ON p08.partition_date = mx8.partition_date
WHERE p08.partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Tarjetas_Credito_Cuentas-Daily') }}',7)
AND p08.partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Tarjetas_Credito_Cuentas-Daily') }}'
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


INSERT OVERWRITE TABLE bi_corp_common.stk_tcr_tarjetas
    PARTITION (partition_date)
SELECT
    CASE WHEN trim(upper(s02.s02_rel_aplicacion))  IN ('','null','NULL') THEN null ELSE trim(upper(s02.s02_rel_aplicacion)) END    AS cod_tcr_tipo_cuenta,
    s02.s02_cuenta_tc                AS cod_tcr_cuenta,
    s02.s02_nro_tarjeta              AS cod_tcr_tarjeta,
    CASE WHEN trim(s02.s02_marca_tc) IN ('','null','NULL') THEN null ELSE trim(s02.s02_marca_tc) END                AS cod_tcr_marca,
    cast(trim(p08.penumper_tarjeta) as int)          AS cod_per_nup_tarjeta,
    cast(trim(p08.penumper_titular) as int)          AS cod_per_nup_titular,
    CASE WHEN trim(s02.s02_apel_nomb_emb) IN ('','null','NULL') THEN null ELSE trim(s02.s02_apel_nomb_emb) END           AS ds_tcr_apellido_nombre_embozado,
    s02.s02_cod_sucursal AS cod_tcr_sucursal,
     CASE
       WHEN upper(s02.s02_rel_aplicacion) = 'AVIS' THEN 'VISA'
       WHEN upper(s02.s02_rel_aplicacion) = 'AEXP' THEN 'AMEX'
       WHEN upper(s02.s02_rel_aplicacion) = 'AMAS' THEN 'MASTER'
       WHEN upper(s02.s02_rel_aplicacion) = 'AMON' THEN 'MONEDERO'
       ELSE upper(s02.s02_rel_aplicacion) END AS ds_tcr_aplicacion_relacionada,
    s02_rel_cod_suc AS cod_suc_sucursal,
    coalesce(s01_rel_tipo_cta,s02.s02_rel_tipo_cta)             AS cod_tcr_tipo_cuenta_relacionada,
    coalesce(s01.s01_rel_nro_cta,s02.s02_rel_nro_cta)              AS cod_nro_cuenta,
    s02.s02_rel_nro_firm             AS int_tcr_nro_firmas_relacionada,
    s02.s02_cod_categoria            AS cod_tcr_categoria,
    s01.s01_estado_cuenta            AS cod_tcr_estado_cuenta,
    s02.s02_estado_tarjeta           AS cod_tcr_estado_tarjeta,
    COALESCE (CASE TRIM(p08.pecodsub)
                  WHEN 'INT' THEN '1'
                  WHEN 'ORO' THEN '2'
                  WHEN 'NAC' THEN '3'
                  WHEN 'BUS' THEN '4'
                  WHEN 'AGRO' THEN '6'
                  WHEN 'CORP' THEN '7'
                  WHEN 'BLAC' THEN '8'
                  WHEN 'PREN' THEN 'A'
                  WHEN 'COCC' THEN '9'
                  WHEN 'DIST' THEN 'D'
                  WHEN 'MONE' THEN 'F'
                  WHEN 'PLAT' THEN 'L'
                  WHEN 'BURE' THEN 'M'
                  WHEN 'CORE' THEN 'N'
                  WHEN 'PUIR' THEN 'Q'
                  END, CASE WHEN trim(s01.s01_cod_producto) IN ('','null','NULL') THEN null ELSE trim(s01.s01_cod_producto) END) AS cod_tcr_producto,
    CASE WHEN trim(p08.pecodpro) IN ('','null','NULL') THEN null ELSE trim(p08.pecodpro) END                     AS cod_prod_producto,
    CASE WHEN trim(p08.pecodsub) IN ('','null','NULL') THEN null ELSE trim(p08.pecodsub) END                     AS cod_prod_subproducto,
    COALESCE(CASE
                 WHEN TRIM(p08.pecodsub) = 'INT' THEN 'INTERNATIONAL'
                 WHEN TRIM(p08.pecodsub) = 'ORO' THEN 'GOLD'
                 WHEN TRIM(p08.pecodsub) = 'NAC' THEN 'NACIONAL'
                 WHEN TRIM(p08.pecodsub) = 'BUS' THEN 'BUSINESS'
                 WHEN TRIM(p08.pecodsub) = 'AGRO' THEN 'AGRO'
                 WHEN TRIM(p08.pecodsub) = 'CORP' THEN 'CORPORATE GOLD'
                 WHEN TRIM(p08.pecodsub) = 'BLAC' THEN 'BLACK'
                 WHEN TRIM(p08.pecodsub) = 'COCC' THEN 'CORPORATE CTA CENTRAL'
                 WHEN TRIM(p08.pecodsub) = 'PREN' THEN 'PREPAGA NOMINADA RECARGABLE'
                 WHEN TRIM(p08.pecodsub) = 'DIST' THEN 'DISTRIBUTION VISA'
                 WHEN TRIM(p08.pecodsub) = 'MONE' THEN 'MONEDERO'
                 WHEN TRIM(p08.pecodsub) = 'PLAT' THEN 'PLATINUM'
                 WHEN TRIM(p08.pecodsub) = 'BURE' THEN 'BUSINESS RECARGABLE'
                 WHEN TRIM(p08.pecodsub) = 'CORE' THEN 'CORPORATE RECARGABLE'
                 WHEN TRIM(p08.pecodsub) = 'PUIR' THEN 'PURCHASING RECARGABLE'
                 END, CASE WHEN trim(dp.ds_tcr_producto) IN ('','null','NULL') THEN null ELSE trim(dp.ds_tcr_producto) END)       AS ds_tcr_producto_tipo,
    COALESCE(CASE
                 WHEN TRIM(p08.pecodsub) = 'INT' THEN 'CREDITO'
                 WHEN TRIM(p08.pecodsub) = 'ORO' THEN 'CREDITO'
                 WHEN TRIM(p08.pecodsub) = 'NAC' THEN 'CREDITO'
                 WHEN TRIM(p08.pecodsub) = 'BUS' THEN 'CREDITO'
                 WHEN TRIM(p08.pecodsub) = 'AGRO' THEN 'CREDITO'
                 WHEN TRIM(p08.pecodsub) = 'CORP' THEN 'CREDITO'
                 WHEN TRIM(p08.pecodsub) = 'BLAC' THEN 'CREDITO'
                 WHEN TRIM(p08.pecodsub) = 'COCC' THEN 'CREDITO'
                 WHEN TRIM(p08.pecodsub) = 'PREN' THEN 'RECARGABLE'
                 WHEN TRIM(p08.pecodsub) = 'DIST' THEN 'CREDITO'
                 WHEN TRIM(p08.pecodsub) = 'MONE' THEN 'MONEDERO'
                 WHEN TRIM(p08.pecodsub) = 'PLAT' THEN 'CREDITO'
                 WHEN TRIM(p08.pecodsub) = 'BURE' THEN 'RECARGABLE'
                 WHEN TRIM(p08.pecodsub) = 'CORE' THEN 'RECARGABLE'
                 WHEN TRIM(p08.pecodsub) = 'PUIR' THEN 'RECARGABLE'
                 END, CASE WHEN trim(dp.ds_tcr_producto_agrup) IN ('','null','NULL') THEN null ELSE trim(dp.ds_tcr_producto_agrup) END) AS ds_tcr_producto_agrup,
    date_format(from_unixtime(unix_timestamp(cast(s02.s02_fec_incl_boletin as string),'yyyyMMdd')),'yyyy-MM-dd') AS dt_tcr_fecha_incluye_boletin,
    s02.s02_motivo_incl_boletin     AS cod_tcr_motivo_incluye_boletin,
    date_format(from_unixtime(unix_timestamp(cast(s02.s02_fecha_alta as string),'yyyyMMdd')),'yyyy-MM-dd')       AS dt_tcr_fecha_alta,
    s02.s02_motivo_alta              AS cod_tcr_motivo_alta,
    CASE WHEN trim(s02.s02_userid_alta) IN ('','null','NULL') THEN null ELSE trim(s02.s02_userid_alta) END              AS cod_tcr_usuario_alta,
    CASE
       WHEN nvl(s01.s01_ult_modif_fec, '0') IN ('0', '20000000', '20111200') THEN null
       ELSE concat(substr(s01.s01_ult_modif_fec, 0, 4), '-', substr(s01.s01_ult_modif_fec, 5, 2), '-',
                   substr(s01.s01_ult_modif_fec, 7, 2)) END                                    dt_tcr_ult_modif_fec,
    case when date_format(from_unixtime(unix_timestamp(cast(s02.s02_fec_baja as string),'yyyyMMdd')),'yyyy-MM-dd') in ('0001-01-01','9999-12-31') then null else  date_format(from_unixtime(unix_timestamp(cast(s02.s02_fec_baja as string),'yyyyMMdd')),'yyyy-MM-dd') end AS dt_tcr_fecha_baja,
    s02.s02_motivo_baja              AS cod_tcr_motivo_baja,
    s02.s02_nro_solicitud            AS cod_tcr_solicitud,
    CASE WHEN trim(s01.s01_campania_id) IN ('','null','NULL') THEN null ELSE trim(s01.s01_campania_id) END  AS cod_tcr_campania,
    s01.s01_campania_nro_shot                                                                            AS cod_tcr_campania_corto,
    CASE WHEN s02.s02_userid_alta = 'CONVCITI' THEN 1 ELSE 0 END                                         AS flag_tcr_marca_contrato_citi,
    CASE
        WHEN s01.s01_campania_id <> 'OPNMKT' AND s01.s01_campania_id NOT LIKE 'ALPT%' THEN 1
        ELSE 0 END                                                                                       AS flag_tcr_marca_preembozada,
    CASE
        WHEN s01.s01_campania_id = 'OPNMKT' THEN 'OPEN MARKET'
        WHEN s01.s01_campania_id LIKE 'ALPT%' THEN 'PUNTO DE VENTA'
        ELSE 'PREEMBOZADO'
        END                                                                                              AS ds_tcr_campania_tipo,
    SUBSTRING(CONCAT('00', cast(s02.s02_vig_des_mm as string)), -2) AS cod_tcr_vigencia_mes_desde,
    SUBSTRING(CONCAT('00', cast(s02.s02_vig_has_mm as string)), -2) AS cod_tcr_vigencia_mes_hasta,
    SUBSTRING(CONCAT('0000', cast(s02.s02_vig_des_aa as string)), -4) AS cod_tcr_vigencia_anio_desde,
    SUBSTRING(CONCAT('0000', cast(s02.s02_vig_has_aa as string)), -4) AS cod_tcr_vigencia_anio_hasta,
    s02.s02_no_renov_motivo          AS cod_tcr_motivo_no_renovacion,
    CASE
        WHEN s02.s02_no_renov_motivo = '1' THEN 'DECISION ENTIDAD'
        WHEN s02.s02_no_renov_motivo = '2' THEN 'DECISION CLIENTE'
        WHEN s02.s02_no_renov_motivo = '3' THEN 'FALLECIMIENTO'
        WHEN s02.s02_no_renov_motivo = '4' THEN 'ORDEN JUDICIAL'
        WHEN s02.s02_no_renov_motivo = '5' THEN 'INHABILITACION BCRA'
        WHEN s02.s02_no_renov_motivo = '6' THEN 'INCOBRABLE'
        WHEN s02.s02_no_renov_motivo = '7' THEN 'TEMPORAL'
        WHEN s02.s02_no_renov_motivo = '8' THEN 'REIMPRESION'
        END                            AS ds_tcr_motivo_no_renovacion,
    date_format(from_unixtime(unix_timestamp(cast(s02.s02_no_renov_fecha as string),'yyyyMMdd')),'yyyy-MM-dd')   AS dt_tcr_fecha_no_renovacion,
    s02.s02_tipo_tarjeta             AS cod_tcr_tipo_tarjeta,
    CASE
        WHEN s02.s02_tipo_tarjeta = '1' THEN 'NORMAL'
        WHEN s02.s02_tipo_tarjeta = '2' THEN 'TEMPORARIA'
        WHEN s02.s02_tipo_tarjeta = '3' THEN 'MINI'
        WHEN s02.s02_tipo_tarjeta = '4' THEN 'VIRTUAL'
        WHEN s02.s02_tipo_tarjeta = '5' THEN 'TAG'
        WHEN s02.s02_tipo_tarjeta = '6' THEN 'CONTACTO'
        WHEN s02.s02_tipo_tarjeta = '7' THEN 'DUAL'
        END AS ds_tcr_tipo_tarjeta,
    s02.s02_canal_vta                AS cod_tcr_canal_venta,
    CASE
        WHEN s02.s02_canal_vta = '1' THEN 'SUCURSAL'
        WHEN s02.s02_canal_vta = '2' THEN 'FUERZA DE VENTAS'
        WHEN s02.s02_canal_vta = '3' THEN 'TELEMARKETING'
        WHEN s02.s02_canal_vta = '5' THEN 'COMERCIALIZADORA'
        END                              AS ds_tcr_canal_venta,
    CASE WHEN trim(s02.s02_marca_tc) IN ('','null','NULL') THEN null ELSE trim(s02.s02_marca_tc) END                 AS cod_tcr_marca_tarjeta,
    CASE WHEN trim(s02.s02_autorizado_tc) IN ('','null','NULL') THEN null ELSE trim(s02.s02_autorizado_tc) END            AS ds_tcr_autorizado_tc,
    CASE
        WHEN s02.s02_rel_nro_firm = 1 or s02.s02_autorizado_tc = 0 THEN 1 else 0
        END                              AS flag_tcr_titular,
    s01.s01_grupo_afinidad           AS cod_tcr_grupo_afinidad,
   CASE
        WHEN (dp.ds_tcr_producto_agrup = 'CREDITO' AND dp.cod_tcr_producto IN ('7','9','D','5','B')) or trim(s01.s01_rel_aplicacion) not in ('AVIS','AEXP','AMAS') THEN 'SIN AFINIDAD'
        WHEN ((s01.s01_estado_cuenta = 10 and s02.partition_date  >= '2020-06-01') or (s01.s01_estado_cuenta <> 10 and concat(substr(s02.s02_fec_baja, 0, 4), '-', substr(s02.s02_fec_baja, 5, 2), '-', substr(s02.s02_fec_baja, 7, 2)) >= '2020-06-01')) and dp.ds_tcr_producto_agrup = 'CREDITO' and trim(s01.s01_rel_aplicacion) in ('AVIS','AEXP','AMAS') then 'SUPERCLUB'
        WHEN ((s01.s01_estado_cuenta = 10 and s02.partition_date  >= '2020-04-29') or (s01.s01_estado_cuenta <> 10 and concat(substr(s02.s02_fec_baja, 0, 4), '-', substr(s02.s02_fec_baja, 5, 2), '-', substr(s02.s02_fec_baja, 7, 2)) >= '2020-04-29')) and dp.ds_tcr_producto_agrup = 'CREDITO' and trim(s01.s01_rel_aplicacion) = 'AMAS' then nvl(da.ds_tcr_afinidad, 'SUPERCLUB')
        WHEN ((s01.s01_estado_cuenta = 10 and s02.partition_date  <  '2020-04-29') or (s01.s01_estado_cuenta <> 10 and concat(substr(s02.s02_fec_baja, 0, 4), '-', substr(s02.s02_fec_baja, 5, 2), '-', substr(s02.s02_fec_baja, 7, 2))  < '2020-04-29')) and dp.ds_tcr_producto_agrup = 'CREDITO' and trim(s01.s01_rel_aplicacion) = 'AMAS' then nvl(da.ds_tcr_afinidad, 'SIN AFINIDAD')
        ELSE (CASE WHEN dp.ds_tcr_producto_agrup = 'CREDITO' THEN nvl(da.ds_tcr_afinidad, 'SUPERCLUB') ELSE nvl(da.ds_tcr_afinidad, 'SIN AFINIDAD') END) END ds_tcr_grupo_afinidad,
    CASE WHEN trim(s02.s02_marca_women) IN ('','null','NULL') THEN null ELSE trim(s02.s02_marca_women) END               AS cod_tcr_tipo_promo,
    date_format(from_unixtime(unix_timestamp(cast(s02.s02_fecha_actual_women as string),'yyyyMMdd')),'yyyy-MM-dd') AS dt_tcr_fecha_promo,
    s02.s02_lim_compras              AS fc_tcr_porcentaje_limite_compra,
    s02.s02_lim_cuotas               AS fc_tcr_porcentaje_limite_cuotas,
    s02.s02_lim_adelantos            AS fc_tcr_porcentaje_limite_adelantos,
    s02.partition_date               AS partition_date
FROM bi_corp_staging.amas_wamas02 s02
LEFT JOIN bi_corp_staging.amas_wamas01 s01
    ON
        s01.s01_cuenta_tc = s02.s02_cuenta_tc AND
        s01.s01_marca_tc = s02.s02_marca_tc AND
        s01.partition_date = s02.partition_date
LEFT JOIN bi_corp_common.dim_tcr_afinidad da
   ON (s01.s01_grupo_afinidad = da.cod_tcr_afinidad
       AND s01.s01_rel_aplicacion = CASE
                                        WHEN da.rel_aplicacion = 'AMEX' THEN 'AEXP'
                                        WHEN da.rel_aplicacion = 'VISA' THEN 'AVIS'
                                        ELSE da.rel_aplicacion
                                    END
       )
LEFT JOIN pedt008_final p08
   ON
       p08.penumcon = s02.s02_rel_nro_cta AND
       CASE
           WHEN p08.pecodpro = '40' THEN 'AVIS'
           WHEN p08.pecodpro = '41' THEN 'AMAS'
           WHEN p08.pecodpro = '42' THEN 'AEXP'
           WHEN p08.pecodpro = '43' THEN 'AMON'
           END = upper(s02.s02_rel_aplicacion) AND
       p08.peordpar = s02.s02_rel_nro_firm
LEFT JOIN bi_corp_common.dim_tcr_producto dp
   ON s01.s01_cod_producto = dp.cod_tcr_producto
WHERE s02.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Tarjetas_Credito_Cuentas-Daily') }}';
