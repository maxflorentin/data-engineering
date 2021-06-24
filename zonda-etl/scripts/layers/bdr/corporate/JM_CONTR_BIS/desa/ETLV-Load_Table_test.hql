set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


with core_contratos as (
select core.pk_s_d, dat.idf_cto_supra, dat.cod_centro, dat.cod_subprodu_altair, dat.cod_producto, dat.cod_divisa, dat.num_persona, dat.cod_reesctr, dat.fec_vencimiento, dat.fec_alta_cto, dat.cod_tip_tasa
  from (
        select num_persona, pk_s_d, count(*) as cant_divisas
        from (
        select concat_ws("_",a.cod_entidad,a.cod_centro,a.num_cuenta,a.cod_producto,a.cod_subprodu_altair) as pk_s_d, a.cod_divisa, a.num_persona
          FROM santander_business_risk_arda.contratos a
--         WHERE data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
         WHERE a.data_date_part =  '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_contratos', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
           and (a.cod_marca != "FA" or a.cod_marca is null)
           and (    a.cod_estado_rel_cliente is null
                or not (    a.cod_estado_rel_cliente = "C"
--                       and fec_baja_rel_cliente <= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}')
                         and a.fec_baja_rel_cliente <= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' ))
        group by concat_ws("_",a.cod_entidad,a.cod_centro,a.num_cuenta,a.cod_producto,a.cod_subprodu_altair), a.cod_divisa, a.num_persona
             ) pt
        group by num_persona, pk_s_d
        ) core
inner join santander_business_risk_arda.contratos dat
--        on dat.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
        on dat.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_contratos', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
       and concat_ws("_",dat.cod_entidad,dat.cod_centro,dat.num_cuenta,dat.cod_producto,dat.cod_subprodu_altair) = core.pk_s_d
       and nvl(dat.num_persona,"0") = nvl(core.num_persona,"0")
where (dat.cod_marca != "FA" or dat.cod_marca is null)
  and ( (core.cant_divisas = 2 and dat.cod_divisa = "ARS") or core.cant_divisas = 1)
  and (    dat.cod_estado_rel_cliente is null
        or not (    dat.cod_estado_rel_cliente = "C"
--              and dat.fec_baja_rel_cliente <= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'))
                and dat.fec_baja_rel_cliente <= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'))
)

insert overwrite table bi_corp_bdr.test_jm_contr_bis
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}')

SELECT distinct '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' AS G4095_FEOPERAC,
                23100 AS G4095_S1EMP,
                lpad(m.id_cto_bdr,9,'0') AS G4095_CONTRA1,
                rpad(a.cod_centro, 40, ' ') AS G4095_IDSUCUR,
                lpad(nvl(bgla.cod_baremo_global,0), 5, '0') AS G4095_ID_PAIS,
                lpad(nvl(vblg.cod_baremo_global, 0), 5, '0') AS G4095_ID_PROD,
                lpad(nvl(vbl.cod_baremo_local, 0), 5, '0') AS G4095_IDPRO_LC,
                CASE WHEN a.fec_vencimiento = '0' OR a.fec_vencimiento IS NULL or a.cod_producto = '71' THEN '9999-12-31' ELSE to_date(from_unixtime(UNIX_TIMESTAMP(cast(nvl(a.fec_vencimiento, 99991231) as varchar(10)),"yyyyMMdd"))) END AS G4095_FECVENTO,
                CASE WHEN a.fec_vencimiento = '0' OR a.fec_vencimiento IS NULL or a.cod_producto = '71' THEN '9999-12-31' ELSE to_date(from_unixtime(UNIX_TIMESTAMP(cast(nvl(a.fec_vencimiento, 99991231) as varchar(10)),"yyyyMMdd"))) END AS G4095_FECVE2,
                CASE WHEN a.fec_alta_cto IS NULL OR a.fec_alta_cto = 0 THEN '0001-01-01' ELSE to_date(from_unixtime(UNIX_TIMESTAMP(cast(a.fec_alta_cto as string),"yyyyMMdd"))) END AS G4095_FECHAPER,
                '9999-12-31' as G4095_FECCANCE,
                CASE WHEN a.cod_producto = '71' AND a.cod_reesctr = '1' AND a.fec_alta_cto IS NOT NULL AND a.fec_alta_cto <> 0 THEN to_date(from_unixtime(UNIX_TIMESTAMP(cast(a.fec_alta_cto as varchar(10)),"yyyyMMdd"))) ELSE '0001-01-01' END AS G4095_FECREES,
                CASE WHEN a.cod_producto = '71' AND a.cod_reesctr = '2' AND a.fec_alta_cto IS NOT NULL AND a.fec_alta_cto <> 0 THEN to_date(from_unixtime(UNIX_TIMESTAMP(cast(a.fec_alta_cto as varchar(10)),"yyyyMMdd"))) ELSE '0001-01-01' END AS G4095_FECREFI,
                '0001-01-01' AS G4095_FECNOVAC,
                lpad(nvl(bg.cod_baremo_global,0), 5, '0') AS G4095_IDFINALI,
                lpad(nvl(cg.cod_destino_de_los_fondos, 0), 5, '0') AS G4095_IDFINALD,
                lpad(0, 9, '0') AS G4095_CONTRA2,
                a.cod_divisa AS G4095_CODDIV,
                lpad(0, 5, '0') AS G4095_ID_CANAL,
                lpad(0, 5, '0') AS G4095_ID_CANA2,
                lpad(0, 5, '0') AS G4095_ID_NATUR,
                lpad(0, 5, '0') AS G4095_ID_NSUBY,
                CASE WHEN a.cod_producto IN ('40', '41', '42', '05', '06', '07', '08', '15', '45', '46', '68', '98', '99') THEN 'S' ELSE 'N' END AS G4095_INDLIM,
                ' ' AS G4095_INDAVA,
                'N' AS G4095_IND_TITU,
                ' ' AS G4095_INDDEUD,
                ' ' AS G4095_INDDEUD2,
                lpad(nvl(vblg_tasa.cod_baremo_global, 0), 5, '0') as G4095_TIP_INTE,
                lpad(9, 5, '9') AS G4095_IDEMIS,
                lpad(' ', 40, ' ') AS G4095_IDEMISI,
                lpad(0, 9, '0') AS G4095_IDNETING,
                lpad(0, 9, '0') AS G4095_IDCOLATE,
                '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' AS G4095_FECULTMO,
                lpad(0, 9, '0') AS G4095_VENCORIG,
                lpad(0, 5, '0') AS G4095_DEUDPREL,
                '9999-12-31' AS G4095_FECENTVI,
                lpad(9, 5, '9') AS G4095_IDPROLC2
FROM core_contratos a
   INNER JOIN bi_corp_bdr.test_normalizacion_id_contratos m
                  ON a.pk_s_d = m.id_cto_source
                 AND m.source = 'credito'
                 AND m.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
   LEFT JOIN santander_business_risk_arda.personas p
                 ON a.num_persona = p.num_persona
 --               AND p.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
                AND p.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_personas', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
   LEFT JOIN santander_business_risk_arda.contratos_garra cg
                 ON a.idf_cto_supra = concat(cg.cod_entidad,'_',cg.cod_centro,'_',cg.num_cuenta,'_',cg.cod_producto,'_',cg.cod_subprodu,'_',cg.cod_divisa)
--                AND cg.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
                AND cg.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_contratos_garra', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
   LEFT JOIN bi_corp_bdr.v_map_baremos_global_local bgla
                 ON bgla.cod_baremo_local = cast(p.residencia_cliente as string)
                AND bgla.cod_negocio = 2
   LEFT JOIN bi_corp_bdr.v_map_baremos_global_local bg
                 ON bg.cod_baremo_local = cg.cod_destino_de_los_fondos
                AND bg.cod_negocio = 4
   LEFT JOIN bi_corp_bdr.v_baremos_local vbl
                 ON TRIM(vbl.cod_baremo_alfanumerico_local) = TRIM(concat(a.cod_producto, a.cod_subprodu_altair))
                AND vbl.cod_negocio_local = '3'
   LEFT JOIN bi_corp_bdr.v_map_baremos_global_local vblg
                 ON vblg.cod_baremo_local = vbl.cod_baremo_local
                AND vblg.cod_negocio = 3
   LEFT JOIN bi_corp_bdr.v_baremos_local vbtasa
                 ON vbtasa.cod_baremo_alfanumerico_local = a.cod_tip_tasa
                AND vbtasa.cod_negocio_local = '8'
   LEFT JOIN bi_corp_bdr.v_map_baremos_global_local vblg_tasa
                 ON vblg_tasa.cod_baremo_local = vbtasa.cod_baremo_local
                AND vblg_tasa.cod_negocio = 8
    LEFT JOIN bi_corp_bdr.test1_jm_trz_cont_ren ren
                ON ren.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
                AND ren.g7025_cont_ant = m.id_cto_bdr
                AND ren.G7025_MOTRENU not in ('00020','00060', '00061', '00062','00063','00064','00065','00066','00067') --Código de renumeraciones que se den de alta! (Para covid) Motivo de renumeración
WHERE ren.g7025_cont_ant is null;

--MMFF Contratos Bis

insert into table bi_corp_bdr.test_jm_contr_bis
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}')
SELECT distinct
        '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' AS G4095_FEOPERAC,
        lpad(nvl(23100,'0'),5,'0') as G4095_S1EMP,
        lpad(nc.id_cto_bdr,9,'0') AS G4095_CONTRA1,
        rpad(' ', 40, ' ') AS G4095_IDSUCUR,
        lpad('00032', 5, '0') AS G4095_ID_PAIS,
        lpad('70', 5, '0') AS G4095_ID_PROD,
        lpad('61026', 5, '0') AS G4095_IDPRO_LC,
        nvl(to_date(from_unixtime(UNIX_TIMESTAMP(cast(nvl(tac.maturity, '31/12/9999') as varchar(10)),"dd/MM/yyyy"))),'9999-12-31') AS G4095_FECVENTO,
        nvl(to_date(from_unixtime(UNIX_TIMESTAMP(cast(nvl(tac.maturity, '31/12/9999') as varchar(10)),"dd/MM/yyyy"))),'9999-12-31') AS G4095_FECVE2,
        '0001-01-01' AS G4095_FECHAPER,
        '9999-12-31' as G4095_FECCANCE,
        '0001-01-01' AS G4095_FECREES,
        '0001-01-01' AS G4095_FECREFI,
        '0001-01-01' AS G4095_FECNOVAC,
        lpad(0, 5, '0') AS G4095_IDFINALI,
        lpad(0, 5, '0') AS G4095_IDFINALD,
        lpad(0, 9, '0') AS G4095_CONTRA2,
        lpad(tac.Currency,3,' ') AS G4095_CODDIV,
        lpad(0, 5, '0') AS G4095_ID_CANAL,
        lpad(0, 5, '0') AS G4095_ID_CANA2,
        lpad(0, 5, '0') AS G4095_ID_NATUR,
        lpad(0, 5, '0') AS G4095_ID_NSUBY,
        ' ' AS G4095_INDLIM,
        ' ' AS G4095_INDAVA,
        ' ' AS G4095_IND_TITU,
        ' ' AS G4095_INDDEUD,
        ' ' AS G4095_INDDEUD2,
        nvl(case when tac.instrument = 'FIXED_COUPON_BOND' then '00001'
                 when tac.instrument = 'ZEROCOUPON_BOND'   then '00001'
                 when tac.instrument = 'FLOATING_BOND'     then '00002'
                 else '00000' end, '00000') as G4095_TIP_INTE,
        lpad(1, 5, '0') as G4095_IDEMIS,
        rpad(tac.isin, 40, ' ') AS G4095_IDEMISI,
        lpad(0, 9, '0') AS G4095_IDNETING,
        lpad(0, 9, '0') AS G4095_IDCOLATE,
        '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' AS G4095_FECULTMO,
        lpad(0, 9, '0') AS G4095_VENCORIG,
        lpad(0, 5, '0') AS G4095_DEUDPREL,
        '9999-12-31' AS G4095_FECENTVI,
        lpad(0, 5, '0') AS G4095_IDPROLC2
 from bi_corp_staging.mmff_tactico_especie tac
 inner join bi_corp_bdr.test_normalizacion_id_contratos nc
        on nc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
       and nc.source = 'mmff-tactico'
       and nc.id_cto_source = concat_ws('_', trim(tac.cuenta),trim(tac.isin))
where tac.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_mmff_tactico_especie', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}';


--Táctico Corresponsales
insert into table bi_corp_bdr.test_jm_contr_bis
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}')
SELECT 
        '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' AS G4095_FEOPERAC
        ,lpad(nvl(23100,'0'),5,'0') as G4095_S1EMP
        ,lpad(nct.id_cto_bdr,9,'0') AS G4095_CONTRA1 -- Normalización de Contratos
        ,rpad('0000', 40, ' ') AS G4095_IDSUCUR
        ,lpad('00032', 5, '0') AS G4095_ID_PAIS
        ,lpad(crp.agrupador_producto, 5, '0') AS G4095_ID_PROD
        ,lpad('61001', 5, '0') AS G4095_IDPRO_LC
        ,concat(substr('{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}',0,8),'05') AS G4095_FECVENTO --Valor por defecto: Actualizar mensualmente el día 5 del mes siguiente al de los datos.
        ,concat(substr('{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}',0,8),'05') AS G4095_FECVE2 --Valor por defecto: Actualizar mensualmente el día 5 del mes siguiente al de los datos.
        ,'2019-09-01' AS G4095_FECHAPER -- Valor por defecto: 01/09/2019
        ,'9999-12-31' as G4095_FECCANCE
        ,'0001-01-01' AS G4095_FECREES
        ,'0001-01-01' AS G4095_FECREFI
        ,'0001-01-01' AS G4095_FECNOVAC
        ,lpad(15, 5, '0') AS G4095_IDFINALI
        ,lpad(0, 5, '0') AS G4095_IDFINALD
        ,lpad(0, 9, '0') AS G4095_CONTRA2
        ,lpad(crp.moneda,3,' ') AS G4095_CODDIV
        ,lpad(0, 5, '0') AS G4095_ID_CANAL
        ,lpad(0, 5, '0') AS G4095_ID_CANA2
        ,lpad(0, 5, '0') AS G4095_ID_NATUR
        ,lpad(0, 5, '0') AS G4095_ID_NSUBY
        ,'N' AS G4095_INDLIM
        ,' ' AS G4095_INDAVA
        ,' ' AS G4095_IND_TITU
        ,' ' AS G4095_INDDEUD
        ,' ' AS G4095_INDDEUD2
        ,lpad(0, 5, '0')  as G4095_TIP_INTE
        ,lpad(0, 5, '0') as G4095_IDEMIS
        ,rpad(' ', 40, ' ') AS G4095_IDEMISI
        ,lpad(0, 9, '0') AS G4095_IDNETING
        ,lpad(0, 9, '0') AS G4095_IDCOLATE
        ,'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' AS G4095_FECULTMO
        ,lpad(0, 9, '0') AS G4095_VENCORIG
        ,lpad(0, 5, '0') AS G4095_DEUDPREL
        ,'9999-12-31' AS G4095_FECENTVI
        ,lpad(0, 5, '0') AS G4095_IDPROLC2
from 
    (select *
       from bi_corp_staging.corresponsales crp
      where crp.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_corresponsales', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
    ) crp
left join 
    (select * 
        from bi_corp_bdr.test_normalizacion_id_contratos 
        where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
        and source = 'corresponsales-tactico' 
    ) nct
    on concat_ws('_', trim(crp.nup), trim(crp.moneda), trim(crp.rubro_altair)) = trim(nct.id_cto_source);