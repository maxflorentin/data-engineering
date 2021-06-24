set mapred.job.queue.name=root.dataeng;

CREATE TEMPORARY TABLE max008 AS
SELECT max(partition_date) AS partition_date
FROM bi_corp_staging.malpe_pedt008 p08
WHERE partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_Tarjetas_Common-Daily') }}'
;

CREATE TEMPORARY TABLE pedt008_use
AS
SELECT p08.penumper,
       p08.penumcon,
       p08.pecodofi,
       p08.peordpar,
       p08.partition_date,
       p08.pecodpro,
       p08.pecalpar,
       p08.pecodsub
FROM bi_corp_staging.malpe_pedt008 p08
         JOIN max008 mx8 ON p08.partition_date = mx8.partition_date
WHERE
        p08.pecodpro IN ('40', '41', '42', '43')
;
CREATE TEMPORARY TABLE pedt008
AS
SELECT p08.penumper,
       p08.penumcon,
       p08.pecodofi,
       p08.peordpar,
       p08.partition_date,
       p08.pecodpro,
       p08.pecodsub,
       ptit.penumper AS nup_titular
FROM pedt008_use p08
         JOIN pedt008_use ptit
              ON
                          ptit.penumcon = p08.penumcon AND
                          ptit.pecodofi = p08.pecodofi AND
                          ptit.pecalpar = 'TI'
;
CREATE TEMPORARY TABLE maxs02 AS
SELECT max(partition_date) AS partition_date
FROM bi_corp_staging.amas_wamas02 s02
WHERE partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_Tarjetas_Common-Daily') }}'
;
CREATE TEMPORARY TABLE maxpra AS
SELECT max(partition_date) AS partition_date
FROM bi_corp_staging.ops_promociones_ra pra
WHERE partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_Tarjetas_Common-Daily') }}'
;
INSERT INTO TABLE bi_corp_common.bt_pro_movimientos_promocionados
    PARTITION (partition_date)

    select
    p08.penumper                             as cod_pro_nupconsumo,
    null                                     as cod_pro_nuptitular,
    s02.s02_cuenta_tc                        as cod_pro_cuenta,
    s02.s02_tipo_cuenta                      as cod_pro_tipo_cuenta,
    w600.600_tarjeta                         as cod_pro_tarjeta,
    lpad (w600.600_casadest,4,0)             as cod_pro_sucursal,
    cast (w600.600_forig as string)          as dt_pro_fechamovimiento,
    null                                     as ts_pro_movimiento,
    cast(w600.600_numest as string)          as cod_pro_establecimiento,
    w600.600_denest                          as ds_pro_establecimiento,
    w600.600_numcom                          as cod_pro_comercio,
    null                                     as ds_pro_comercio,
    w600.600_cod_campania_bco                as cod_pro_camp,
    proa.desc_campana                        as ds_pro_campania
    cast (w600.600_rubro as string)          as cod_pro_rubro,
    w600.600_imp_dto_usu                     as fc_pro_importebonificado,
    w600.600_imporig                         as fc_pro_importeconsumo,
    'credito'                                as medio_pago,
    w600.partition_date
    from bi_corp_staging.aftc_waftc600 w600
    LEFT JOIN
         (
             SELECT
                 s02.s02_cuenta_tc,
                 s02.s02_tipo_cuenta,
                 s02.s02_nro_tarjeta,
                 s02.s02_rel_nro_cta,
                 s02.s02_cod_sucursal,
                 s02.s02_rel_nro_firm
             FROM bi_corp_staging.amas_wamas02 s02
                      JOIN maxs02
                           ON maxs02.partition_date = s02.partition_date
         ) s02
         ON s02.s02_nro_tarjeta = w600.600_tarjeta
    LEFT JOIN pedt008 p08
        ON
        p08.penumcon = s02.s02_rel_nro_cta AND
        p08.pecodofi = s02.s02_cod_sucursal AND
        p08.peordpar = s02.s02_rel_nro_firm
    LEFT JOIN
       (
         SELECT
         distinct cod_banco,
         desc_campana
         bi_corp_staging.ops_promociones_ra ra
                  JOIN maxpra
                          ON maxpra.partition_date = ra.partition_date
        ) proa
        ON proa.cod_banco = w600.600_cod_campania_bco
    where
    w600.600_cod_registro='30'
    and w600.600_id_archivo='P'
    and w600.600_cod_campania_bco <> ''
    and w600.partition_date  = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_Tarjetas_Common-Daily') }}'
