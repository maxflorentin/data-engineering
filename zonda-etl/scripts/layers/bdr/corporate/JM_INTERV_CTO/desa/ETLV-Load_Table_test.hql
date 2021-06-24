set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


insert OVERWRITE TABLE bi_corp_bdr.test_jm_interv_cto
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}')
SELECT DISTINCT
       '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' AS G4128_FEOPERAC,
       23100 AS G4128_S1EMP,
       lpad( m.id_cto_bdr, 9, '0' ) AS G4128_CONTRA1,
       lpad(bgl.cod_baremo_global, 5, '0') AS G4128_TIPINTEV,
       lpad(bgl.cod_baremo_local, 5, '0') AS G4128_TIPINTV2,
       case when c.cal_participacion = "TI" then lpad('1000000', 17, '0') ELSE lpad('0', 17, '0') end AS G4128_NUMORDIN,
       lpad(c.num_persona, 9, '0') AS G4128_IDNUMCLI,
       lpad(nvl(vind.cod_baremo_global, 0), 5, '0') AS G4128_FORMINTV,
       lpad(concat((case when mae.ind_mancomu != 'O' then 100 else 0 end),'000000') , 9, '0') AS G4128_PORPARTN,
       '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' AS G4128_FECULTMO

FROM santander_business_risk_arda.relacion_contrato_cliente c

INNER JOIN (SELECT data_date_part, cod_entidad, cod_centro, num_cuenta, cod_producto, cod_subprodu, cal_participacion, cod_estado_relacion,
                   min(ord_participacion) as ord_participacion_min
              FROM santander_business_risk_arda.relacion_contrato_cliente
             WHERE data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_relacion_contrato_cliente', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
               AND cal_participacion IN ("TI", "AV", "FI", "GA")
               AND (cod_estado_relacion = 'A' or (cod_estado_relacion = 'C' and fec_baja > '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'))
             GROUP BY data_date_part, cod_entidad, cod_centro, num_cuenta, cod_producto, cod_subprodu, cal_participacion, cod_estado_relacion
             ) fil
             on fil.cod_entidad = c.cod_entidad
            and fil.cod_centro = c.cod_centro
            and fil.num_cuenta = c.num_cuenta
            and fil.cod_producto = c.cod_producto
            and fil.cod_subprodu = c.cod_subprodu
            and fil.cal_participacion = c.cal_participacion
            and fil.cod_estado_relacion = c.cod_estado_relacion
            and fil.ord_participacion_min = c.ord_participacion


INNER JOIN bi_corp_bdr.test_normalizacion_id_contratos m
            on c.cod_entidad = m.cred_cod_entidad
           AND c.cod_centro = m.cred_cod_centro
           AND c.num_cuenta = m.cred_num_cuenta
           AND c.cod_producto = m.cred_cod_producto
           AND c.cod_subprodu = m.cred_cod_subprodu_altair
           AND m.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'

INNER JOIN bi_corp_bdr.test_jm_contr_bis cb
            on cb.g4095_contra1 = m.id_cto_bdr
           AND cb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'

LEFT JOIN bi_corp_bdr.v_baremos_local bl
            ON bl.cod_baremo_alfanumerico_local = c.cal_participacion
           AND bl.cod_negocio_local = '10'

LEFT JOIN bi_corp_bdr.v_map_baremos_global_local bgl
            ON bl.cod_baremo_local = bgl.cod_baremo_local
           AND bgl.cod_negocio_local = 10

LEFT JOIN bi_corp_staging.bgdtmae mae
            on mae.entidad = m.cred_cod_entidad
           and mae.centro_alta = m.cred_cod_centro
           and mae.cuenta = concat('0', m.cred_cod_producto, substring(m.cred_num_cuenta, 4, 12) )
           and mae.producto = m.cred_cod_producto
           and trim(mae.subprodu) = m.cred_cod_subprodu_altair
           and mae.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_bgdtmae', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'

LEFT JOIN bi_corp_bdr.baremos_local ind
            on ind.cod_baremo_alfanumerico_local = mae.ind_mancomu
           and ind.cod_negocio_local = '11'

LEFT JOIN bi_corp_bdr.v_map_baremos_global_local vind
            on vind.cod_baremo_local = '7'
           and vind.cod_negocio_local = 11

WHERE c.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_relacion_contrato_cliente', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
;

--MMFF Intervinientes Contratos BIS
with origen_rtra as (select distinct t.*
                       from (select case
                                      when cpty = 'GBAR' then
                                        'BCEA'
                                      else
                                        cpty
                                    end cpty,
                                    dealstamp
                               from bi_corp_staging.rtra_regulatorio
                              where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
                                and cpty_name != 'CLIENTE FICTICIO ARGENTINA'
                              union all
                             select case
                                      when cpty = 'GBAR' then
                                         'BCEA'
                                      else
                                         cpty
                                    end cpty,
                                    dealstamp
                               from bi_corp_staging.rtra_economico
                              where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
                                and cpty_name != 'CLIENTE FICTICIO ARGENTINA'
                            ) t
                     )
insert INTO TABLE bi_corp_bdr.test_jm_interv_cto
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}')
SELECT DISTINCT
       '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' AS G4128_FEOPERAC,
       23100 AS G4128_S1EMP,
       lpad(nc.id_cto_bdr,9,'0') AS G4128_CONTRA1,
       lpad('3', 5, '0') AS G4128_TIPINTEV,
       lpad('14', 5, '0') AS G4128_TIPINTV2,
       lpad('1000000', 17, '0') AS G4128_NUMORDIN,
       lpad(regexp_replace(nvl(mdr.alias_nup,0),'<NOT FOUND>','0'), 9, '0') AS G4128_IDNUMCLI,
       lpad('1', 5, '0') AS G4128_FORMINTV,
       lpad(concat('100','000000') , 9, '0') AS G4128_PORPARTN,
       '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' AS G4128_FECULTMO
 from bi_corp_staging.mmff_tactico_especie tac
inner join bi_corp_bdr.test_normalizacion_id_contratos nc
        on nc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
       and nc.source = 'mmff-tactico'
       and nc.id_cto_source = concat_ws('_', trim(tac.cuenta),trim(tac.isin))
inner join origen_rtra rtra
        on rtra.dealstamp = concat(tac.especie,'-I')
inner join bi_corp_staging.mdr_contrapartes mdr
     on mdr.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_mdr_contrapartes', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
    and trim(mdr.shortname) = trim(rtra.cpty)
where tac.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_mmff_tactico_especie', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
and regexp_replace(nvl(mdr.alias_nup,0),'<NOT FOUND>','0') != 0
;

--Táctico Corresponsales
insert into table bi_corp_bdr.test_jm_interv_cto
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}')
SELECT 
       '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' AS G4128_FEOPERAC
       ,23100 AS G4128_S1EMP
       ,lpad(nct.id_cto_bdr,9,'0') AS G4128_CONTRA1 -- Normalización de contrato
       ,lpad('1', 5, '0') AS G4128_TIPINTEV
       ,lpad('6', 5, '0') AS G4128_TIPINTV2
       ,lpad('1000000', 17, '0') AS G4128_NUMORDIN
       ,lpad(crp.nup, 9, '0') AS G4128_IDNUMCLI
       ,lpad('1', 5, '0') AS G4128_FORMINTV
       ,lpad('0', 9, '0') AS G4128_PORPARTN
       ,'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' AS G4128_FECULTMO
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