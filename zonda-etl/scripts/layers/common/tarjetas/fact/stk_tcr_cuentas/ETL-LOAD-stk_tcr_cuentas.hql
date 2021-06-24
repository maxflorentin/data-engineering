SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS max008;
CREATE TEMPORARY TABLE max008 AS
SELECT max(partition_date) AS partition_date
FROM bi_corp_staging.malpe_pedt008 p08
WHERE partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Tarjetas_Credito_Cuentas-Daily') }}',7)
and partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Tarjetas_Credito_Cuentas-Daily') }}'
;

DROP TABLE IF EXISTS pedt008_use;
CREATE TEMPORARY TABLE pedt008_use
AS
SELECT p08.penumper,
       p08.penumcon,
       p08.pecodofi,
       p08.peordpar,
       p08.partition_date,
       p08.pecodpro,
       p08.pecalpar,
       p08.pecodsub,
       p08.pefecalt,
       p08.pefecbrb,
       CASE
           WHEN p08.pecodpro = '70' THEN
               CASE substr(p08.pecodsub, 3, 2)
                   WHEN '40' THEN 'AVIS'
                   WHEN '41' THEN 'AMAS'
                   WHEN '42' THEN 'AEXP'
                   WHEN '43' THEN 'AMON'
                   END
           WHEN p08.pecodpro = '40' THEN 'AVIS'
           WHEN p08.pecodpro = '41' THEN 'AMAS'
           WHEN p08.pecodpro = '42' THEN 'AEXP'
           WHEN p08.pecodpro = '43' THEN 'AMON'
       END           AS       rel_aplicacion,
       CASE p08.pecodpro
            WHEN '70' THEN TRUE
            ELSE FALSE
        END AS flag_70
FROM bi_corp_staging.malpe_pedt008 p08
INNER JOIN max008 mx8 ON p08.partition_date = mx8.partition_date
WHERE p08.partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Tarjetas_Credito_Cuentas-Daily') }}',7)
AND p08.partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Tarjetas_Credito_Cuentas-Daily') }}'
AND     ( p08.pecodpro IN ('40', '41', '42', '43') OR
            (
                p08.pecodpro = '70' AND
                substr(p08.pecodsub, 3, 2) IN ('40', '41', '42', '43')
            )
        ) AND p08.pecalpar = 'TI'
;

DROP TABLE IF EXISTS max042;
CREATE TEMPORARY TABLE max042 AS
SELECT max(partition_date) AS partition_date
FROM bi_corp_staging.malpe_pedt042 p42
WHERE partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Tarjetas_Credito_Cuentas-Daily') }}',7)
AND partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Tarjetas_Credito_Cuentas-Daily') }}'
;

DROP TABLE IF EXISTS pedt042_rel;
CREATE TEMPORARY TABLE pedt042_rel
AS
SELECT p42.pecodofi,
       p42.penumcon,
       p42.partition_date,
       substr(p42.pepaqper, 5, 4) cod_tcr_sucursal_paquete,
       substr(p42.pepaqper, 9)    cod_tcr_numero_paquete,
       CASE
           WHEN p42.pecodpro = '70' THEN
               CASE substr(p42.pecodsub, 3, 2)
                   WHEN '40' THEN 'AVIS'
                   WHEN '41' THEN 'AMAS'
                   WHEN '42' THEN 'AEXP'
                   WHEN '43' THEN 'AMON'
                   END
           WHEN p42.pecodpro = '40' THEN 'AVIS'
           WHEN p42.pecodpro = '41' THEN 'AMAS'
           WHEN p42.pecodpro = '42' THEN 'AEXP'
           WHEN p42.pecodpro = '43' THEN 'AMON'
           END           AS       p42_rel_aplicacion,
       p42.pecodmon,
       p42.pesdoant_sgn,
       trim(p42.pesucope) as pesucope,
       coalesce(cast(trim(concat(substr(regexp_replace(regexp_replace(p42.pesdoant,' ',''), "^0+", ''),1,length(regexp_replace(regexp_replace(p42.pesdoant,' ',''), "^0+", ''))-2),'.',substr(regexp_replace(p42.pesdoant, "^0+", ''),-2)))as decimal(15, 2)),0) as pesdoant
FROM bi_corp_staging.malpe_pedt042 p42
INNER JOIN max042 m42 ON p42.partition_date = m42.partition_date
WHERE p42.partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Tarjetas_Credito_Cuentas-Daily') }}',7)
AND p42.partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Tarjetas_Credito_Cuentas-Daily') }}'
AND (p42.pecodpro IN ('40', '41', '42', '43')
   OR (p42.pecodpro = '70' AND substr(p42.pecodsub, 3, 2) IN ('40', '41', '42', '43')))
;


DROP TABLE IF EXISTS pedt042_use;
CREATE TEMPORARY TABLE pedt042_use
AS
SELECT p42.pecodofi,
       p42.penumcon,
       p42.partition_date,
       max(cod_tcr_sucursal_paquete) AS cod_tcr_sucursal_paquete,
       max(cod_tcr_numero_paquete) AS cod_tcr_numero_paquete,
       p42_rel_aplicacion,
       max(p42.pesucope) as pesucope,
       sum(
               CASE
                   WHEN p42.pecodmon = 'USD'
                       THEN 0
                   ELSE
                        CASE WHEN p42.pesdoant_sgn = '-' THEN -1 ELSE 1 END * p42.pesdoant
                   END
           ) AS saldo_pesos,
       sum(
               CASE
                   WHEN p42.pecodmon = 'USD'
                       THEN CASE WHEN p42.pesdoant_sgn = '-' THEN -1 ELSE 1 END * p42.pesdoant
                   ELSE
                       0
                   END
           ) AS saldo_dolares
FROM pedt042_rel p42
GROUP BY p42.pecodofi,
         p42.penumcon,
         p42.partition_date,
         p42_rel_aplicacion
;

DROP TABLE IF EXISTS limites_disponibles;
CREATE TEMPORARY TABLE limites_disponibles AS
select case when cod_banco='72' then 'AVIS'
             when cod_banco='472' then 'AEXP'
             when cod_banco='172' then 'AMAS' end AS marca,
case when (cod_banco='472' or cod_banco='172') then substr(cast(nro_cuenta as string), 1, length(cast(nro_cuenta as string)) - 1) else cast(nro_cuenta as string) end as nro_cuenta,
cast(marca_limite_unificado as int) as marca_limite_unificado,
limite_actual_compra_cuotas,
limite_compra,
round(Limite_Actual_Compra_Cuotas/Limite_Compra,1) factor_limite_compra_implicito,
case when round(Limite_Actual_Compra_Cuotas/Limite_Compra,1)=99.9 then 1 else round(Limite_Actual_Compra_Cuotas/Limite_Compra,1) end as factor_limite_compra_calculado,
case when marca_limite_unificado='0' then limite_compra*(case when round(Limite_Actual_Compra_Cuotas/Limite_Compra,1)=99.9 then 1 else round(Limite_Actual_Compra_Cuotas/Limite_Compra,1) end)
     when marca_limite_unificado='1' then limite_compra end as importe_limite_compra_cuotas,
case when marca_limite_unificado='0' then limite_compra*(1+(case when round(Limite_Actual_Compra_Cuotas/Limite_Compra,1)=99.9 then 1 else round(Limite_Actual_Compra_Cuotas/Limite_Compra,1) end))
     when marca_limite_unificado='1' then limite_compra end as limite_compras_total,
case when cod_grupo_afinidad = '1974' then 0.2 else 1 end as factor_compra_planes,
case when marca_limite_unificado='0' then (case when marca_limite_unificado='0' then limite_compra*(case when round(Limite_Actual_Compra_Cuotas/Limite_Compra,1)=99.9 then 1 else round(Limite_Actual_Compra_Cuotas/Limite_Compra,1) end)
     when marca_limite_unificado='1' then limite_compra end)*(case when cod_grupo_afinidad = '1974' then 0.2 else 1 end)
     when marca_limite_unificado='1' then limite_compra*(case when cod_grupo_afinidad = '1974' then 0.2 else 1 end) end limite_compra_planes
from bi_corp_staging.prisma_limites_disponibles
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_prisma_limites_disponibles', dag_id='LOAD_CMN_Tarjetas_Credito_Cuentas-Daily') }}';


INSERT OVERWRITE TABLE bi_corp_common.stk_tcr_cuentas
    PARTITION (partition_date)
SELECT
    CASE WHEN trim(w1.s01_rel_aplicacion) IN ('','null','NULL') THEN null ELSE trim(w1.s01_rel_aplicacion) END cod_tcr_tipo_cuenta, -- Se usa REL_APLICACION porque TIPO_CUENTA es 3 tanto para AMEX como AMAS
    w1.s01_cuenta_tc                                                                           cod_tcr_cuenta,
    cast(trim(p8.penumper) as int)                                                             cod_per_nup_titular,
    w1.s01_estado_cuenta                                                                       cod_tcr_estado_cuenta,
    CASE WHEN trim(w1.s01_cod_producto) IN ('','null','NULL') THEN null ELSE trim(w1.s01_cod_producto) END cod_tcr_producto,
    CASE WHEN trim(p.ds_tcr_producto_banco) IN ('','null','NULL') THEN null ELSE trim(p.ds_tcr_producto_banco)  END ds_tcr_producto_tipo,
    CASE WHEN trim(p.ds_tcr_producto_agrup) IN ('','null','NULL') THEN null ELSE trim(p.ds_tcr_producto_agrup)  END ds_tcr_producto_agrup,
    CASE WHEN trim(COALESCE(p8_70.pecodpro,p8.pecodpro)) IN ('','null','NULL') THEN null ELSE trim(COALESCE(p8_70.pecodpro,p8.pecodpro))  END AS cod_prod_producto,
    CASE WHEN trim(COALESCE(p8_70.pecodsub, p8.pecodsub)) IN ('','null','NULL') THEN null ELSE trim(COALESCE(p8_70.pecodsub, p8.pecodsub))  END AS cod_prod_subproducto,
    s01_cod_sucursal                                                           cod_tcr_sucursal,
    cast(p42.pesucope as int) as cod_tcr_sucursal_operativa,
    w1.s01_cta_aut_mc_dv as cod_tcr_digito_verificador,
    CASE
       WHEN w1.s01_rel_aplicacion = 'AVIS' THEN 'VISA'
       WHEN w1.s01_rel_aplicacion = 'AEXP' THEN 'AMEX'
       WHEN w1.s01_rel_aplicacion = 'AMAS' THEN 'MASTER'
       WHEN w1.s01_rel_aplicacion = 'AMON' THEN 'MONEDERO'
       ELSE s01_rel_aplicacion END ds_tcr_aplicacion_relacionada,
    s01_rel_cod_suc                                                        cod_suc_sucursal,
    w1.s01_rel_nro_cta                                                                         cod_nro_cuenta,
    w1.s01_rel_nro_firm                                                                        cod_tcr_nro_firm,
    w1.s01_cartera                                                                             cod_tcr_cartera,
    w1.s01_canal_vta                                                                           cod_tcr_canal_venta,
    CASE
       WHEN w1.s01_canal_vta = '1' THEN 'SUCURSAL'
       WHEN w1.s01_canal_vta = '2' THEN 'FUERZA DE VENTAS'
       WHEN w1.s01_canal_vta = '3' THEN 'TELEMARKETING'
       WHEN w1.s01_canal_vta = '5' THEN 'COMERCIALIZADORA' END                                ds_tcr_canal_venta,
    COALESCE(p8_70.pefecalt,
        CASE
           WHEN nvl(w1.s01_fecha_alta, '0') = '0' THEN null
           ELSE concat(substr(w1.s01_fecha_alta, 0, 4), '-', substr(w1.s01_fecha_alta, 5, 2), '-',
                       substr(w1.s01_fecha_alta, 7, 2))
        END)                                       dt_tcr_fecha_alta,
    CASE
       WHEN nvl(w1.s01_ult_modif_fec, '0') IN ('0', '20000000', '20111200') THEN null
       ELSE concat(substr(w1.s01_ult_modif_fec, 0, 4), '-', substr(w1.s01_ult_modif_fec, 5, 2), '-',
                   substr(w1.s01_ult_modif_fec, 7, 2)) END                                    dt_tcr_ult_modif_fec,
    case when COALESCE(p8_70.pefecbrb,
        CASE
           WHEN nvl(w1.s01_fec_baja, '0') = '0' THEN null
           WHEN w1.s01_fec_baja = '20111200' AND w1.s01_cuenta_tc = '5555811' THEN '2008-08-05'
           ELSE concat(substr(w1.s01_fec_baja, 0, 4), '-', substr(w1.s01_fec_baja, 5, 2), '-',
                       substr(w1.s01_fec_baja, 7, 2))
        END) in ('0001-01-01','9999-12-31') then null else COALESCE(p8_70.pefecbrb,
        CASE
           WHEN nvl(w1.s01_fec_baja, '0') = '0' THEN null
           WHEN w1.s01_fec_baja = '20111200' AND w1.s01_cuenta_tc = '5555811' THEN '2008-08-05'
           ELSE concat(substr(w1.s01_fec_baja, 0, 4), '-', substr(w1.s01_fec_baja, 5, 2), '-',
                       substr(w1.s01_fec_baja, 7, 2))
        END) end as dt_tcr_fecha_baja,
    w1.s01_motivo_baja                                                                         cod_tcr_motivo_baja_cta,
    CASE
       WHEN w1.s01_motivo_baja = '1' THEN 'DECISION CLIENTE CON TJ'
       WHEN w1.s01_motivo_baja = '2' THEN 'DECISION CLIENTE DESIST'
       WHEN w1.s01_motivo_baja = '3' THEN 'DECISION CLIENTE SIN TJ'
       WHEN w1.s01_motivo_baja = '4' THEN 'NO RENOVACION'
       WHEN w1.s01_motivo_baja = '5' THEN 'PASE A MORA'
       WHEN w1.s01_motivo_baja = '6' THEN 'DECISION BANCO'
       WHEN w1.s01_motivo_baja = '8' THEN 'CIERRE DE CAMPANA'
       WHEN w1.s01_motivo_baja = '9'
           THEN 'CIERRE CONTROLADO' END                                                       ds_tcr_motivo_baja_cta,
    cast(trim(p42.cod_tcr_sucursal_paquete) as int)                                           cod_tcr_sucursal_paquete,
    CASE WHEN trim(p42.cod_tcr_numero_paquete) IN ('','null','NULL') THEN null ELSE trim(p42.cod_tcr_numero_paquete) END cod_tcr_numero_paquete,
    a.o20_model_liq                                                                            cod_tcr_modelo_liquidacion,
    CASE
       WHEN nvl(a.o20_fecha_ult_pago, '00000000') IN ('00000000', '00990705', '19000000','0') THEN null
       ELSE concat(substr(a.o20_fecha_ult_pago, 0, 4), '-', substr(a.o20_fecha_ult_pago, 5, 2), '-',
                   substr(a.o20_fecha_ult_pago, 7, 2)) END                                    dt_tcr_fecha_ultimo_pago,
    CASE
       WHEN nvl(a.o20_fecha_liquidacion, '00000000') IN ('00000000', '19000000','0') THEN null
       ELSE concat(substr(a.o20_fecha_liquidacion, 0, 4), '-', substr(a.o20_fecha_liquidacion, 5, 2), '-',
                   substr(a.o20_fecha_liquidacion, 7, 2)) END                                 dt_tcr_fecha_cierre,
    CASE
       WHEN nvl(a.o20_fecha_prox_liq, '00000000') in ('00000000','0') THEN null
       ELSE concat(substr(a.o20_fecha_prox_liq, 0, 4), '-', substr(a.o20_fecha_prox_liq, 5, 2), '-',
                   substr(a.o20_fecha_prox_liq, 7, 2)) END                                    dt_tcr_fecha_proximo_cierre,
    CASE
       WHEN nvl(a.o20_fec_ult_compra, '00000000') in ('00000000','0') THEN null
       ELSE concat(substr(a.o20_fec_ult_compra, 0, 4), '-', substr(a.o20_fec_ult_compra, 5, 2), '-',
                   substr(a.o20_fec_ult_compra, 7, 2)) END                                    dt_tcr_fecha_ultima_compra,
    CASE
       WHEN nvl(a.o20_fecha_vto_liq, '00000000') IN ('00000000', '19000000','0') THEN null
       ELSE concat(substr(a.o20_fecha_vto_liq, 0, 4), '-', substr(a.o20_fecha_vto_liq, 5, 2), '-',
                   substr(a.o20_fecha_vto_liq, 7, 2)) END                                     dt_tcr_fecha_ultimo_vto,
    CASE
       WHEN nvl(a.o20_fec_vto_ori, '00000000') in ('00000000','0') THEN null
       ELSE concat(substr(a.o20_fec_vto_ori, 0, 4), '-', substr(a.o20_fec_vto_ori, 5, 2), '-',
                   substr(a.o20_fec_vto_ori, 7, 2)) END                                       dt_tcr_fecha_ultimo_vto_orig,
    CASE
       WHEN nvl(a.o20_fecha_prox_vto_liq, '00000000') in ('00000000','0') THEN null
       ELSE concat(substr(a.o20_fecha_prox_vto_liq, 0, 4), '-', substr(a.o20_fecha_prox_vto_liq, 5, 2), '-',
                   substr(a.o20_fecha_prox_vto_liq, 7, 2)) END                                dt_tcr_fecha_proximo_vto,
    CASE
       WHEN nvl(a.o20_inicio_mora_nm, '00000000') in ('00000000','0') THEN null
       ELSE concat(substr(a.o20_inicio_mora_nm, 0, 4), '-', substr(a.o20_inicio_mora_nm, 5, 2), '-',
                   substr(a.o20_inicio_mora_nm, 7, 2)) END                                    dt_tcr_fecha_irregularidad,
    a.o20_ult_mes_mora_nm                                                                      cod_tcr_ult_mes_irregularidad,
    CASE WHEN trim(w1.s01_campania_id) IN ('','null','NULL') THEN null ELSE trim(w1.s01_campania_id) END                                                                         cod_tcr_campania,
    w1.s01_campania_nro_shot                                                                   cod_tcr_campania_corto,
    CASE WHEN w1.s01_userid_alta = 'CONVCITI' THEN 1 ELSE 0 END                                flag_tcr_marca_contrato_citi,
    CASE
       WHEN w1.s01_campania_id <> 'OPNMKT' AND w1.s01_campania_id NOT LIKE 'ALPT%' THEN 1
       ELSE 0 END                                                                             flag_tcr_marca_preembozada,
    CASE
       WHEN w1.s01_campania_id = 'OPNMKT' THEN 'OPEN MARKET'
       WHEN w1.s01_campania_id LIKE 'ALPT%' THEN 'PUNTO DE VENTA'
       ELSE 'PREEMBOZADO'
       END                                                                                    ds_tcr_campania_tipo,
    a.o20_forma_pago                                                                           cod_tcr_forma_pago,
    CASE
       WHEN a.o20_forma_pago = '1' THEN 'SIN DEBITO AUTOMATICO'
       WHEN a.o20_forma_pago = '2' THEN 'DEBITO CTA. CTE. PAGO MINIMO'
       WHEN a.o20_forma_pago = '3' THEN 'DEBITO CTA. CTE. PAGO TOTAL'
       WHEN a.o20_forma_pago = '4' THEN 'DEBITO CAJA AHORRO PAGO MINIMO'
       WHEN a.o20_forma_pago = '5' THEN 'DEBITO CAJA AHORRO PAGO TOTAL' END                   ds_tcr_forma_pago,

    ld.marca_limite_unificado as flag_tcr_marca_limite_unificado,
    ld.limite_actual_compra_cuotas as fc_tcr_importe_limite_actual_compra_cuotas,
    ld.limite_compra as fc_tcr_importe_limite_compra_actual,
    ld.factor_limite_compra_implicito as fc_tcr_factor_limite_compra_implicito,
    ld.factor_limite_compra_calculado as fc_tcr_factor_limite_compra_calculado,


    CASE
       WHEN w1.s01_cod_producto IN ('H', '8') THEN ld.importe_limite_compra_cuotas *
                                                   CASE WHEN a.o20_dolar_visa = 0 THEN 1 ELSE (a.o20_dolar_visa / 100) END
       ELSE ld.importe_limite_compra_cuotas END                                                          fc_tcr_importe_limite_compra_cuotas_segun_unificado_pesos,
    CASE
       WHEN w1.s01_cod_producto NOT IN ('H', '8') THEN ld.importe_limite_compra_cuotas /
                                                       CASE WHEN a.o20_dolar_visa = 0 THEN 1 ELSE (a.o20_dolar_visa / 100) END
       ELSE ld.importe_limite_compra_cuotas END                                                          fc_tcr_importe_limite_compra_cuotas_segun_unificado_dolares,
    ld.importe_limite_compra_cuotas as fc_tcr_importe_limite_compra_cuotas_segun_unificado,

    CASE
       WHEN w1.s01_cod_producto IN ('H', '8') THEN ld.limite_compras_total *
                                                   CASE WHEN a.o20_dolar_visa = 0 THEN 1 ELSE (a.o20_dolar_visa / 100) END
       ELSE ld.limite_compras_total END                                                          fc_tcr_importe_limite_compras_total_segun_unificado_pesos,
    CASE
       WHEN w1.s01_cod_producto NOT IN ('H', '8') THEN ld.limite_compras_total /
                                                       CASE WHEN a.o20_dolar_visa = 0 THEN 1 ELSE (a.o20_dolar_visa / 100) END
       ELSE ld.limite_compras_total END                                                          fc_tcr_importe_limite_compras_total_segun_unificado_dolares,
    ld.limite_compras_total as fc_tcr_importe_limite_compras_total_segun_unificado,

    ld.factor_compra_planes as fc_tcr_factor_compra_planes,

    CASE
       WHEN w1.s01_cod_producto IN ('H', '8') THEN ld.limite_compra_planes *
                                                   CASE WHEN a.o20_dolar_visa = 0 THEN 1 ELSE (a.o20_dolar_visa / 100) END
       ELSE ld.limite_compra_planes END                                                          fc_tcr_importe_limite_compra_planes_segun_unificado_pesos,
    CASE
       WHEN w1.s01_cod_producto NOT IN ('H', '8') THEN ld.limite_compra_planes /
                                                       CASE WHEN a.o20_dolar_visa = 0 THEN 1 ELSE (a.o20_dolar_visa / 100) END
       ELSE ld.limite_compra_planes END                                                          fc_tcr_importe_limite_compra_planes_segun_unificado_dolares,

    ld.limite_compra_planes as fc_tcr_importe_limite_compra_planes_segun_unificado,

    CASE
       WHEN a.o20_inicio_mora_nm <> '00000000' THEN datediff(w1.partition_date, date_format(
               from_unixtime(unix_timestamp(cast(a.o20_inicio_mora_nm AS string), 'yyyymmdd')), 'yyyy-mm-dd'))
       ELSE 0 END                                                                             fc_tcr_dias_mora,
    a.o20_imp_tot_liq_aus                                                                      fc_tcr_importe_saldo_total_pesos,
    a.o20_imp_tot_liq_dol                                                                      fc_tcr_importe_saldo_total_dolares,
    a.o20_saldo_ur_total                                                                       fc_tcr_importe_saldo_total,
    CASE
       WHEN w1.s01_cod_producto IN ('H', '8') THEN a.o20_limite *
                                                   CASE WHEN a.o20_dolar_visa = 0 THEN 1 ELSE (a.o20_dolar_visa / 100) END
       ELSE a.o20_limite END                                                                  fc_tcr_importe_limite_compra_pesos,
    CASE
       WHEN w1.s01_cod_producto NOT IN ('H', '8') THEN a.o20_limite /
                                                       CASE WHEN a.o20_dolar_visa = 0 THEN 1 ELSE (a.o20_dolar_visa / 100) END
       ELSE a.o20_limite END                                                                  fc_tcr_importe_limite_compra_dolares,
    a.o20_limite                                                                               fc_tcr_importe_limite_compra,
    a.o20_saldo_actual_aus                                                                     fc_tcr_importe_saldo_tot_peso_ciclo,
    a.o20_saldo_actual_dol                                                                     fc_tcr_importe_saldo_tot_dolares_ciclo,
    a.o20_saldo_actual_aus + (a.o20_saldo_actual_dol *
                             CASE WHEN a.o20_dolar_visa = 0 THEN 1 ELSE (a.o20_dolar_visa / 100) END) fc_tcr_importe_saldo_tot_ciclo,
    p42.saldo_pesos         AS fc_tcr_importe_saldo_pesos,
    p42.saldo_dolares       AS fc_tcr_importe_saldo_dolares,
    CASE
       WHEN w1.s01_cod_producto IN ('H', '8') THEN a.o20_limite_credito *
                                                   CASE WHEN a.o20_dolar_visa = 0 THEN 1 ELSE (a.o20_dolar_visa / 100) END
       ELSE a.o20_limite_credito END                                                          fc_tcr_importe_limite_credito_pesos,
    CASE
       WHEN w1.s01_cod_producto NOT IN ('H', '8') THEN a.o20_limite_credito /
                                                       CASE WHEN a.o20_dolar_visa = 0 THEN 1 ELSE (a.o20_dolar_visa / 100) END
       ELSE a.o20_limite_credito END                                                          fc_tcr_importe_limite_credito_dolares,
    a.o20_limite_credito                                                                       fc_tcr_importe_limite_credito,
    a.o20_pago_min_liq                                                                         fc_tcr_pago_minimo,
    a.o20_adelantos_dol                                                                        fc_tcr_monto_adelanto_dolar_ciclo,
    a.o20_adelantos_aus                                                                        fc_tcr_monto_adelanto_pesos_ciclo,
    a.o20_consumos_dol                                                                         fc_tcr_monto_compra_dolar_ciclo,
    a.o20_consumos_aus                                                                         fc_tcr_monto_compra_pesos_ciclo,
    w1.s01_porc_bonif_atm                                                                      fc_tcr_porcentaje_bonificacion_atm,
    w1.s01_no_renov_motivo                                                                     cod_tcr_no_renov_motivo,
    CASE
       WHEN w1.s01_no_renov_motivo = '1' THEN 'DECISION ENTIDAD'
       WHEN w1.s01_no_renov_motivo = '2' THEN 'DECISION CLIENTE'
       WHEN w1.s01_no_renov_motivo = '3' THEN 'FALLECIMIENTO'
       WHEN w1.s01_no_renov_motivo = '4' THEN 'ORDEN JUDICIAL'
       WHEN w1.s01_no_renov_motivo = '5' THEN 'INHABILITACION BCRA'
       WHEN w1.s01_no_renov_motivo = '6'
           THEN 'INCOBRABLE' END                                                              ds_tcr_no_renov_motivo,
    CASE
       WHEN nvl(w1.s01_no_renov_fecha, '0') = '0' THEN null
       ELSE concat(substr(w1.s01_no_renov_fecha, 0, 4), '-', substr(w1.s01_no_renov_fecha, 5, 2), '-',
                   substr(w1.s01_no_renov_fecha, 7, 2)) END                                   dt_tcr_no_renov_fecha,
    CASE WHEN trim(w1.s01_tipo_paquete) IN ('','null','NULL') THEN null ELSE trim(w1.s01_tipo_paquete) END                                                                         cod_trc_tipo_paquete,
    w1.s01_grupo_afinidad                                                                      cod_tcr_grupo_afinidad,
    CASE
        WHEN (p.ds_tcr_producto_agrup = 'CREDITO' AND p.cod_tcr_producto IN ('7','9','D','5','B')) or trim(w1.s01_rel_aplicacion) not in ('AVIS','AEXP','AMAS') THEN 'SIN AFINIDAD'
        WHEN ((w1.s01_estado_cuenta = 10 and w1.partition_date  >= '2020-06-01') or (w1.s01_estado_cuenta <> 10 and concat(substr(w1.s01_fec_baja, 0, 4), '-', substr(w1.s01_fec_baja, 5, 2), '-', substr(w1.s01_fec_baja, 7, 2)) >= '2020-06-01')) and p.ds_tcr_producto_agrup = 'CREDITO' and trim(w1.s01_rel_aplicacion) in ('AVIS','AEXP','AMAS') then 'SUPERCLUB'
        WHEN ((w1.s01_estado_cuenta = 10 and w1.partition_date  >= '2020-04-29') or (w1.s01_estado_cuenta <> 10 and concat(substr(w1.s01_fec_baja, 0, 4), '-', substr(w1.s01_fec_baja, 5, 2), '-', substr(w1.s01_fec_baja, 7, 2)) >= '2020-04-29')) and p.ds_tcr_producto_agrup = 'CREDITO' and trim(w1.s01_rel_aplicacion) = 'AMAS' then nvl(g.ds_tcr_afinidad, 'SUPERCLUB')
        WHEN ((w1.s01_estado_cuenta = 10 and w1.partition_date  <  '2020-04-29') or (w1.s01_estado_cuenta <> 10 and concat(substr(w1.s01_fec_baja, 0, 4), '-', substr(w1.s01_fec_baja, 5, 2), '-', substr(w1.s01_fec_baja, 7, 2))  < '2020-04-29')) and p.ds_tcr_producto_agrup = 'CREDITO' and trim(w1.s01_rel_aplicacion) = 'AMAS' then nvl(g.ds_tcr_afinidad, 'SIN AFINIDAD')
        ELSE (CASE WHEN p.ds_tcr_producto_agrup = 'CREDITO' THEN nvl(g.ds_tcr_afinidad, 'SUPERCLUB') ELSE nvl(g.ds_tcr_afinidad, 'SIN AFINIDAD') END) END ds_tcr_grupo_afinidad,
    CASE WHEN trim(w1.s01_userid_alta) IN ('','null','NULL') THEN null ELSE trim(w1.s01_userid_alta) END                                                                         cod_tcr_usuario_alta,
    w1.s01_cargo_porc_bonif                                                                    cod_tcr_cargo_porc_bonif,
    CASE WHEN trim(w1.s01_apel_nomb_embozado) IN ('','null','NULL') THEN null ELSE trim(w1.s01_apel_nomb_embozado) END                                                                  ds_tcr_apellido_nombre_embozado,
    a.o20_dolar_visa / 100                                       fc_tcr_dolar_visa,
    CASE
        WHEN nvl(w1.s01_fecha_alta, '0') in ('00000000','0') THEN null
        ELSE concat(substr(w1.s01_fecha_alta, 0, 4), '-', substr(w1.s01_fecha_alta, 5, 2), '-',
                    substr(w1.s01_fecha_alta, 7, 2)) END                                    AS dt_tcr_fecha_alta_original,
    CASE
        WHEN nvl(w1.s01_fec_baja, '0') in ('00000000','0') THEN null
        WHEN w1.s01_fec_baja = '20111200' AND w1.s01_cuenta_tc = '5555811' THEN '2008-08-05'
        ELSE concat(substr(w1.s01_fec_baja, 0, 4), '-', substr(w1.s01_fec_baja, 5, 2), '-',
                    substr(w1.s01_fec_baja, 7, 2)) END                                      AS dt_tcr_fecha_baja_original,
    case when a.o20_mca_cphone_mora='' then null else a.o20_mca_cphone_mora end as cod_tcr_marca_cuotaphone,
    null as ds_tcr_signo_score,
    null as fc_tcr_score_riesgo,
    a.o20_pagos_total as fc_tcr_pagos_total,
    w1.partition_date partition_date
FROM bi_corp_staging.amas_wamas01 w1
-- DIM de Producto
LEFT JOIN bi_corp_common.dim_tcr_producto p
   ON w1.s01_cod_producto = p.cod_tcr_producto
-- DIM de Afinidad
LEFT JOIN bi_corp_common.dim_tcr_afinidad g
   ON (w1.s01_grupo_afinidad = g.cod_tcr_afinidad
       AND w1.s01_rel_aplicacion = CASE
                                       WHEN g.rel_aplicacion = 'AMEX' THEN 'AEXP'
                                       WHEN g.rel_aplicacion = 'VISA' THEN 'AVIS'
                                       ELSE g.rel_aplicacion END
       )
-- Maestro Saldos Diario
LEFT JOIN bi_corp_staging.afma_wasdo20 a
   ON (w1.s01_tipo_cuenta = a.o20_tipo_cuenta
       AND w1.s01_cuenta_tc = a.o20_nro_cuenta
       AND w1.partition_date = a.partition_date)
-- Contratos
LEFT JOIN pedt008_use p8
   ON (w1.s01_rel_aplicacion = p8.rel_aplicacion
       AND w1.s01_rel_cod_suc = p8.pecodofi
     --AND w1.s01_cuenta_tc = p8.penumcon
	   AND w1.s01_rel_nro_cta = p8.penumcon
       AND w1.s01_rel_nro_firm = p8.peordpar
       AND NOT p8.flag_70)
-- Contratos 70
LEFT JOIN pedt008_use p8_70
    ON (w1.s01_rel_aplicacion = p8_70.rel_aplicacion
      AND w1.s01_rel_cod_suc = p8_70.pecodofi
    --AND w1.s01_cuenta_tc = p8_70.penumcon
	  AND w1.s01_rel_nro_cta = p8_70.penumcon
      AND w1.s01_rel_nro_firm = p8_70.peordpar
      AND p8_70.flag_70)
-- Saldo Diario
LEFT JOIN pedt042_use p42
   ON (w1.s01_rel_aplicacion = p42_rel_aplicacion
       AND w1.s01_rel_cod_suc = p42.pecodofi
       AND w1.s01_rel_nro_cta = p42.penumcon)
LEFT JOIN limites_disponibles ld on ld.nro_cuenta=w1.s01_cuenta_tc and ld.marca=upper(w1.s01_rel_aplicacion)
WHERE w1.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Tarjetas_Credito_Cuentas-Daily') }}';