set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


CREATE TEMPORARY TABLE bi_corp_bdr.limites_ctas_tarj
  (id_cto_contratos_sin_divisa STRING,
   limite_credito STRING,
   deuda STRING,
   disponible STRING
   );

-- limites para tarjetas y cuentas
--     insert de limites para tarjetas
with cto_perimetro_tarj as (
    select pc.id_cto_contratos_sin_divisa,
           max(nvl(co.imp_lim_credito_ml,0)) as limite_credito
      from bi_corp_bdr.perim_arda_contratos pc
    inner join santander_business_risk_arda.CONTRATOS co
            on co.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_contratos', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
           and concat_ws('_',co.cod_entidad,co.cod_centro,co.num_cuenta,co.cod_producto,co.cod_subprodu_altair) = pc.id_cto_contratos_sin_divisa
           and co.cod_producto in ('40', '41', '42')
           and nvl(co.ind_mora_pcr16,0) <> 1
           and nvl(co.cod_est_cta,0) not in (11,12,19)
     where pc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
    group by pc.id_cto_contratos_sin_divisa
),
 datos_saldos_tarj as (
    select p.id_cto_contratos_sin_divisa, sum(nvl(co.imp_deuda,0)) as deuda,
           max(nvl(p.limite_credito,0)) - sum(nvl(co.imp_deuda,0)) as disponible
      from cto_perimetro_tarj p
    inner join santander_business_risk_arda.contratos co
           on co.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_contratos', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
          and concat_ws('_',co.cod_entidad,co.cod_centro,co.num_cuenta,co.cod_producto,co.cod_subprodu_altair) = p.id_cto_contratos_sin_divisa
    group by p.id_cto_contratos_sin_divisa
    having sum(nvl(co.imp_deuda,0)) < max(nvl(p.limite_credito,0))
    order by p.id_cto_contratos_sin_divisa)
insert overwrite table bi_corp_bdr.limites_ctas_tarj
select cpt.id_cto_contratos_sin_divisa, cpt.limite_credito, nvl(dst.deuda,0) as deuda, nvl(dst.disponible,cpt.limite_credito) as disponible
  from cto_perimetro_tarj cpt
  left join datos_saldos_tarj dst
        on cpt.id_cto_contratos_sin_divisa = dst.id_cto_contratos_sin_divisa;

--     insert de limites de cuentas
 with cto_perimetro_cta as (
    select pc.id_cto_contratos_sin_divisa,
           max(nvl(co.imp_lim_credito_ml,0)) as limite_credito
      from bi_corp_bdr.perim_arda_contratos pc
    inner join santander_business_risk_arda.CONTRATOS co
            on co.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_contratos', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
           and concat_ws('_',co.cod_entidad,co.cod_centro,co.num_cuenta,co.cod_producto,co.cod_subprodu_altair) = pc.id_cto_contratos_sin_divisa
           and co.cod_producto in ('05', '06', '07')
           and nvl(co.ind_mora_pcr16,0) <> 1
    where pc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
    group by pc.id_cto_contratos_sin_divisa
),
 datos_saldos_cta as (
    select cpc.id_cto_contratos_sin_divisa,
           max(nvl(co.imp_deuda,0)) as deuda, max(nvl(cpc.limite_credito,0)) - max(nvl(co.imp_deuda,0)) as disponible
      from cto_perimetro_cta cpc
    inner join santander_business_risk_arda.contratos co
           on co.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_contratos', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
          and concat_ws('_',co.cod_entidad,co.cod_centro,co.num_cuenta,co.cod_producto,co.cod_subprodu_altair) = cpc.id_cto_contratos_sin_divisa
    group by cpc.id_cto_contratos_sin_divisa
    having max(nvl(co.imp_deuda,0)) < max(nvl(cpc.limite_credito,0))
 )
insert into table bi_corp_bdr.limites_ctas_tarj
    select cpc.id_cto_contratos_sin_divisa, cpc.limite_credito, nvl(dsc.deuda,0) as deuda, nvl(dsc.disponible,cpc.limite_credito) as disponible
      from cto_perimetro_cta cpc
    left join datos_saldos_cta dsc
        on cpc.id_cto_contratos_sin_divisa = dsc.id_cto_contratos_sin_divisa;

with record_set_insert as (
select pc.id_cto_contratos_sin_divisa, (row_number() over(partition by concat_ws("_",dat.cod_entidad,dat.cod_centro,dat.num_cuenta,dat.cod_producto,dat.cod_subprodu_altair)
                  order by dat.cod_divisa asc)) as orden_divisa,
        dat.cod_centro, dat.cod_subprodu_altair, dat.cod_producto, dat.cod_divisa, dat.num_persona, dat.cod_reesctr,
       dat.fec_vencimiento, dat.fec_alta_cto, dat.cod_tip_tasa, pc.id_cto_bdr
  from bi_corp_bdr.perim_arda_contratos pc
  inner join santander_business_risk_arda.contratos dat
        on dat.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_contratos', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
       and concat_ws("_",dat.cod_entidad,dat.cod_centro,dat.num_cuenta,dat.cod_producto,dat.cod_subprodu_altair) = pc.id_cto_contratos_sin_divisa
       and nvl(dat.num_persona,"0") = nvl(pc.num_persona,"0")
 where pc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
   and (dat.cod_marca != "FA" or dat.cod_marca is null)
   and (   dat.cod_estado_rel_cliente is null
        or not (    dat.cod_estado_rel_cliente = "C"
                and dat.fec_baja_rel_cliente <= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')))
insert overwrite table bi_corp_bdr.datos_arda_contratos
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
select distinct
       rsi.num_persona,
       rsi.id_cto_bdr,
       rsi.id_cto_contratos_sin_divisa,
       rsi.cod_centro,
       rsi.cod_producto,
       rsi.cod_subprodu_altair,
       rsi.cod_divisa,
       rsi.fec_alta_cto,
       rsi.fec_vencimiento,
       rsi.cod_tip_tasa,
       rsi.cod_reesctr,
       lct.limite_credito,
       lct.deuda,
       lct.disponible
  from record_set_insert rsi
 left join bi_corp_bdr.limites_ctas_tarj lct
   on lct.id_cto_contratos_sin_divisa = rsi.id_cto_contratos_sin_divisa
 where orden_divisa = 1;