SET hive.merge.size.per.task=4000000;
SET hive.merge.smallfiles.avgsize=16000000;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;


--1. cuentas (bi_corp_common.stk_cue_cuentas):
-- a) estado de cuenta = 'A', y solo para el caso de RIO PERSONAL tiene que tener hecho algún movimiento genuino (el join con la DIM de movimientos que habías hecho y la fecha de primero mov. genuino no nula).. para todo el resto es indistinto, solo por estado de cuenta para el flag_titular_cuenta
DROP TABLE IF EXISTS cuentas;
CREATE TEMPORARY TABLE cuentas AS
select distinct
    c.cod_per_nup
from
    bi_corp_common.stk_cue_cuentas c
left join
    bi_corp_staging.exa_dim_productos dprod on
        coalesce(c.cod_cue_producto_paquete, c.cod_cue_producto) = dprod.producto
        and coalesce(c.cod_cue_subroducto_paquete, c.cod_cue_subproducto) = dprod.subproducto
        and dprod.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_Personas-Daily') }}'
where
    c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_cue_cuentas', dag_id='LOAD_BSN_Personas-Daily') }}'
    and c.cod_cue_estado_cuenta = 'A'
    and concat(c.cod_cue_producto,c.cod_cue_subproducto) != '020018'
    and (coalesce(dprod.producto_agrup,'NULL') != 'Super Cta 1 PAS'
        or (coalesce(dprod.producto_agrup,'NULL') = 'Super Cta 1 PAS'
            and c.dt_cue_primer_movimiento_genuino is not null));

--2. préstamos (bi_corp_common.stk_pre_prestamos)
-- a) situación de préstamos '0' y estado de deuda objetivo DISTINTO de 20  (filtrar además el producto 71 porque no los consideramos hoy titulares) para flag_titular_prestamo
DROP TABLE IF EXISTS prestamos;
CREATE TEMPORARY TABLE prestamos AS
select distinct
    p.cod_per_nup
from
    bi_corp_common.stk_pre_prestamos p
where
    p.cod_pre_situacionprestamo = '0'
    and p.cod_pre_situaciondeudaobjetiva != '20'
    and p.cod_prod_producto != '71'
    and p.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_pre_prestamos', dag_id='LOAD_BSN_Personas-Daily') }}';

-- 3. tarjetas (bi_corp_common.stk_tcr_cuentas):
-- a) estado de cuenta = '10' y limite >= 2, tipo agrupador = 'CREDITO' ('VISA','AMEX','AMAS') y NUP no nulo para el flag_titular_tarjeta
DROP TABLE IF EXISTS tarjetas;
CREATE TEMPORARY TABLE tarjetas AS
select distinct
    c.cod_per_nup_titular as cod_per_nup
from
    bi_corp_common.stk_tcr_cuentas c
where
    c.cod_tcr_estado_cuenta = 10
    and c.ds_tcr_producto_agrup = 'CREDITO'
    and c.cod_per_nup_titular is not null
    and c.fc_tcr_importe_limite_compra >= 2
    and c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_tcr_cuentas', dag_id='LOAD_BSN_Personas-Daily') }}';


-- 4. caja de seguridad (bi_corp_common.stk_cjs_caja_seguridad):
-- a) que el NUP esté en esa tabla para esa fecha, así de simple para el flag_titular_caja_seguridad (no tiene su versión _GYM este producto)

DROP TABLE IF EXISTS caja_seguridad;
CREATE TEMPORARY TABLE caja_seguridad AS
select distinct
    s.cod_per_nup
from
    bi_corp_common.stk_cjs_caja_seguridad s
where
    s.cod_per_nup is not null
    and s.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_cjs_caja_seguridad', dag_id='LOAD_BSN_Personas-Daily') }}';


-- 5. seguros (bi_corp_common.stk_seg_seguros):
--a) tipo de mercado = 'OM' (open market). La única especialidad de este producto es que para el tipo de seguro like '%FRAUDE%', para que ese seguro se considere titular tiene que tener ALGUNA cuenta de la tabla bi_corp_common.stk_cue_cuentas en estado de cuenta 'A' (acá no se hace lógica por tipo de producto). Y con eso se tendría el flag_titular_seguro (no tiene su versión _GYM este producto)

DROP TABLE IF EXISTS seguros;
CREATE TEMPORARY TABLE seguros AS
select distinct
    s.cod_per_nup
from
    bi_corp_common.stk_seg_seguros s
left join
    bi_corp_common.stk_cue_cuentas c on c.cod_per_nup = s.cod_per_nup and c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_cue_cuentas', dag_id='LOAD_BSN_Personas-Daily') }}'
where
    s.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_seg_seguros', dag_id='LOAD_BSN_Personas-Daily') }}'
    and (
         (s.cod_seg_tipo_mercado = 'OM' and s.ds_seg_tipo_seguro not like '%FRAUDE%')
      OR (s.cod_seg_tipo_mercado = 'OM' and s.ds_seg_tipo_seguro like '%FRAUDE%' AND c.cod_cue_estado_cuenta = 'A')
        );

-- 6 plazo fijo
DROP TABLE IF EXISTS plazo_fijo;
CREATE TEMPORARY TABLE plazo_fijo AS
select distinct
    p.cod_per_nup
from
    bi_corp_common.stk_pla_plazofijo p
where
    p.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_Personas-Daily') }}'
    and p.cod_pla_estado in ('A','V');


-- 7 fondos
DROP TABLE IF EXISTS fondos;
CREATE TEMPORARY TABLE fondos AS
select distinct
    f.cod_per_nup
from
    bi_corp_common.stk_inv_fondos f
where
    f.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_inv_fondos', dag_id='LOAD_BSN_Personas-Daily') }}';


-- 8 acciones y bonos:
DROP TABLE IF EXISTS acc_bonos;
CREATE TEMPORARY TABLE acc_bonos AS
select distinct
    t.cod_per_nup
from
    bi_corp_common.stk_inv_titulos t
where
    t.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_inv_titulos', dag_id='LOAD_BSN_Personas-Daily') }}'
    and t.ds_inv_tipo_especie in ('Bonos','Acciones');

-- 9 moria
DROP TABLE IF EXISTS moria;
CREATE TEMPORARY TABLE moria AS
select distinct
    c.cod_per_nup
from
    bi_corp_common.stk_rcp_contratosmoria c
where
    c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_Personas-Daily') }}'
    and c.cod_prod_producto = '70'
    and c.cod_rcp_estado = '00';


-- 10 contratos
DROP TABLE IF EXISTS contratos;
CREATE TEMPORARY TABLE contratos AS
select distinct
    cast(cli.ascl_nu_nup as bigint) as cod_per_nup
FROM
    bi_corp_staging.rio6_asit_contratos c
LEFT JOIN
    bi_corp_staging.rio6_asit_clientes cli ON
    (c.asct_tp_documento = cli.ASCL_TP_DOCUMENTO
    AND c.asct_nu_documento = cli.ASCL_NU_DOCUMENTO
    AND cli.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_Personas-Daily') }}')
WHERE
    c.asct_st_certificado = '1'
    AND c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_Personas-Daily') }}';


-------calculo titular-------
DROP TABLE IF EXISTS titulares;
CREATE TEMPORARY TABLE titulares AS
select distinct
    p.cod_per_nup,
    IF(
        c.cod_per_nup is not  null or
        pr.cod_per_nup is not  null or
        t.cod_per_nup is not  null or
        cs.cod_per_nup is not  null or
        s.cod_per_nup is not  null or
        pf.cod_per_nup is not  null or
        fo.cod_per_nup is not  null or
        ab.cod_per_nup is not  null or
        con.cod_per_nup is not  null,
        1,
        0
    ) AS flag_titular,
    if(m.cod_per_nup is not null, 1, 0) as flag_titular_gym,
    p.partition_date
from
    bi_corp_common.stk_per_personas p
left join cuentas c on c.cod_per_nup = p.cod_per_nup
left join prestamos pr on pr.cod_per_nup = p.cod_per_nup
left join tarjetas t on t.cod_per_nup = p.cod_per_nup
left join caja_seguridad cs on cs.cod_per_nup = p.cod_per_nup
left join seguros s on s.cod_per_nup = p.cod_per_nup
left join plazo_fijo pf on pf.cod_per_nup = p.cod_per_nup
left join fondos fo on fo.cod_per_nup = p.cod_per_nup
left join acc_bonos ab on ab.cod_per_nup = p.cod_per_nup
left join moria m on m.cod_per_nup = p.cod_per_nup
left join contratos con on con.cod_per_nup = p.cod_per_nup
where
    p.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_per_personas', dag_id='LOAD_BSN_Personas-Daily') }}';

DROP TABLE IF EXISTS fechas_alta;
CREATE TEMPORARY TABLE fechas_alta AS
select
    t.partition_date as dt_per_fechainformacion,
    t.cod_per_nup,
    cast(NULL  as string) as ds_mejor_relacion,
    t.flag_titular,
    t.flag_titular_gym,
    cast(NULL as int) as int_antiguedad_titular,
    CASE
        WHEN t.flag_titular = 1 AND bp.dt_alta_titular IS NULL THEN t.partition_date
        WHEN t.flag_titular = 1 AND bp.dt_alta_titular IS NOT NULL THEN bp.dt_alta_titular
    ELSE cast(NULL as string)
    END as dt_alta_titular,
    cast(NULL as string) as ds_mejor_produ,
    cast(NULL as string) as ds_mejor_produ_cta,
    t.partition_date
from
    titulares t
left join bi_corp_business.stk_personas bp on bp.cod_per_nup = t.cod_per_nup AND bp.partition_date = date_sub(t.partition_date, 1);

-- MEJOR RELACION --
DROP TABLE IF EXISTS flag_per_cotitular;
CREATE TEMPORARY TABLE flag_per_cotitular AS
select
    p.cod_per_nup,
    count(*) flag_per_cotitular
from
    bi_corp_common.stk_per_personas p
where
    p.flag_per_cotitular = 1
    and p.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_per_personas', dag_id='LOAD_BSN_Personas-Daily') }}'
group by p.cod_per_nup;


DROP TABLE IF EXISTS flag_per_firmante;
CREATE TEMPORARY TABLE flag_per_firmante AS
select
    p.cod_per_nup,
    count(*) flag_per_firmante
from
    bi_corp_common.stk_per_personas p
where
    p.flag_per_firmante = 1
    and p.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_per_personas', dag_id='LOAD_BSN_Personas-Daily') }}'
group by p.cod_per_nup;

DROP TABLE IF EXISTS flag_adicional;
CREATE TEMPORARY TABLE flag_adicional AS
select
    t.cod_per_nup_tarjeta cod_per_nup,
    count(*) flag_adicional
from
    bi_corp_common.stk_tcr_tarjetas t
where
    t.partition_Date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_tcr_tarjetas', dag_id='LOAD_BSN_Personas-Daily') }}'
    and t.flag_tcr_titular = 0
    and t.ds_tcr_producto_agrup = 'CREDITO'
    and t.cod_tcr_estado_cuenta = 10
    and t.cod_tcr_estado_tarjeta in (20,22)
    and concat(t.cod_tcr_vigencia_anio_hasta,'-',t.cod_tcr_vigencia_mes_hasta) >= substr(t.partition_date,1,7)
    and exists (select
                    1
                from
                    bi_corp_common.stk_tcr_cuentas c
                where
                    c.partition_Date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_tcr_cuentas', dag_id='LOAD_BSN_Personas-Daily') }}'
                    and c.ds_tcr_producto_agrup = 'CREDITO'
                    and c.fc_tcr_importe_limite_compra >= 2
                    and c.partition_date = t.partition_date
                    and c.cod_tcr_tipo_cuenta = t.cod_tcr_tipo_cuenta
                    and c.cod_nro_cuenta = t.cod_nro_cuenta)
group by t.cod_per_nup_tarjeta;


DROP TABLE IF EXISTS flag_recargable_titular;
CREATE TEMPORARY TABLE flag_recargable_titular AS
select
    c.cod_per_nup_titular cod_per_nup,
    count(*) flag_recargable_titular
from
    bi_corp_common.stk_tcr_cuentas c
where
    c.partition_Date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_tcr_cuentas', dag_id='LOAD_BSN_Personas-Daily') }}'
    and c.ds_tcr_producto_agrup = 'RECARGABLE'
    and c.cod_tcr_estado_cuenta = 10
    and exists (select 1
                from bi_corp_common.stk_tcr_tarjetas t
                where
                    t.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_tcr_tarjetas', dag_id='LOAD_BSN_Personas-Daily') }}'
                    and t.ds_tcr_producto_agrup = 'RECARGABLE'
                    and t.cod_tcr_estado_tarjeta in (20,22)
                    and concat(t.cod_tcr_vigencia_anio_hasta,'-',t.cod_tcr_vigencia_mes_hasta) >= substr(t.partition_date,1,7)
                    and t.partition_date = c.partition_date
                    and t.cod_tcr_tipo_cuenta = c.cod_tcr_tipo_cuenta
                    and t.cod_nro_cuenta = c.cod_nro_cuenta)
group by c.cod_per_nup_titular;

DROP TABLE IF EXISTS flag_recargable_usuario;
CREATE TEMPORARY TABLE flag_recargable_usuario AS
select
    t.cod_per_nup_tarjeta cod_per_nup,
    count(*) flag_recargable_usuario
from
    bi_corp_common.stk_tcr_tarjetas t
where
    t.partition_Date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_tcr_tarjetas', dag_id='LOAD_BSN_Personas-Daily') }}'
    and t.ds_tcr_producto_agrup = 'RECARGABLE'
    and t.cod_tcr_estado_cuenta = 10
    and t.cod_tcr_estado_tarjeta in (20,22)
    and concat(t.cod_tcr_vigencia_anio_hasta,'-',t.cod_tcr_vigencia_mes_hasta) >= substr(t.partition_date,1,7)
group by
    t.cod_per_nup_tarjeta;


DROP TABLE IF EXISTS ds_mejor_relacion;
CREATE TEMPORARY TABLE ds_mejor_relacion AS
select
    p.dt_per_fechainformacion,
    p.cod_per_nup,
    case
        when p.flag_titular > 0 then 'TITULAR'
        when co.flag_per_cotitular > 0 then 'COTITULAR'
        when ad.flag_adicional > 0 then 'ADICIONAL'
        when fir.flag_per_firmante > 0 then 'OTRO FIRMANTE'
        when rt.flag_recargable_titular > 0 then 'RECARGABLE TITULAR'
        when ru.flag_recargable_usuario > 0 then 'RECARGABLE USUARIO'
    end as ds_mejor_relacion,
    p.flag_titular,
    p.flag_titular_gym,
    IF( p.partition_date = p.dt_alta_titular,
        1,
        ceil(datediff(p.partition_date,p.dt_alta_titular)/30)
    ) AS int_antiguedad_titular,
    p.dt_alta_titular,
    p.ds_mejor_produ,
    p.ds_mejor_produ_cta,
    p.partition_date
from
    fechas_alta p
left join
    flag_per_cotitular co on co.cod_per_nup = p.cod_per_nup
left join
    flag_per_firmante fir on fir.cod_per_nup = p.cod_per_nup
left join
    flag_adicional ad on ad.cod_per_nup = p.cod_per_nup
left join
    flag_recargable_titular rt on rt.cod_per_nup = p.cod_per_nup
left join
    flag_recargable_usuario ru on ru.cod_per_nup = p.cod_per_nup;


INSERT overwrite TABLE bi_corp_business.stk_personas partition(partition_date)
select
    r.dt_per_fechainformacion,
    r.cod_per_nup,
    r.ds_mejor_relacion,
    r.flag_titular,
    r.flag_titular_gym,
    r.int_antiguedad_titular,
    r.dt_alta_titular as dt_alta_titular,
    r.ds_mejor_produ,
    r.ds_mejor_produ_cta as ds_mejor_produ_mes_ant,
    r.ds_mejor_produ_cta as ds_mejor_produ_semestre,
    r.partition_date
from
    ds_mejor_relacion r
;