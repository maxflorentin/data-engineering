SET mapred.job.queue.name=root.dataeng;

CREATE VIEW IF NOT EXISTS bi_corp_bdr.view_bdr_controles_reg AS
select itlo.partition_date
    ,sum(case when itlo.tabla = 'bi_corp_bdr.jm_clien_juri' then itlo.cuenta else 0 end) as  jm_clien_juri
    ,sum(case when itlo.tabla = 'bi_corp_bdr.JM_TRZ_CLIENTE' then itlo.cuenta else 0 end) as  JM_TRZ_CLIENTE
    ,sum(case when itlo.tabla = 'bi_corp_bdr.jm_client_bii' then itlo.cuenta else 0 end) as  jm_client_bii
    ,sum(case when itlo.tabla = 'bi_corp_bdr.jm_contr_bis' then itlo.cuenta else 0 end) as  jm_contr_bis
    ,sum(case when itlo.tabla = 'bi_corp_bdr.jm_contr_otros' then itlo.cuenta else 0 end) as   jm_contr_otros
    ,sum(case when itlo.tabla = 'bi_corp_bdr.jm_ctos_cance' then itlo.cuenta else 0 end) as   jm_ctos_cance
    ,sum(case when itlo.tabla = 'bi_corp_bdr.JM_EAD_CONTR' then itlo.cuenta else 0 end) as  JM_EAD_CONTR
    ,sum(case when itlo.tabla = 'bi_corp_bdr.jm_expos_no_con' then itlo.cuenta else 0 end) as   jm_expos_no_con
    ,sum(case when itlo.tabla = 'bi_corp_bdr.jm_flujos' then itlo.cuenta else 0 end) as  jm_flujos
    ,sum(case when itlo.tabla = 'bi_corp_bdr.jm_grup_econo' then itlo.cuenta else 0 end) as   jm_grup_econo
    ,sum(case when itlo.tabla = 'bi_corp_bdr.jm_grup_rela' then itlo.cuenta else 0 end) as   jm_grup_rela
    ,sum(case when itlo.tabla = 'bi_corp_bdr.jm_interv_cto' then itlo.cuenta else 0 end) as  jm_interv_cto
    ,sum(case when itlo.tabla = 'bi_corp_bdr.jm_posic_contr' then itlo.cuenta else 0 end) as  jm_posic_contr
    ,sum(case when itlo.tabla = 'bi_corp_bdr.JM_PROV_ESOTR' then itlo.cuenta else 0 end) as   JM_PROV_ESOTR
    ,sum(case when itlo.tabla = 'bi_corp_bdr.jm_trz_cont_ren' then itlo.cuenta else 0 end) as   jm_trz_cont_ren
    ,sum(case when itlo.tabla = 'bi_corp_bdr.jm_vta_carter' then itlo.cuenta else 0 end) as   jm_vta_carter
    ,sum(case when itlo.tabla = 'bi_corp_bdr.JM_GARANT_CTO' then itlo.cuenta else 0 end) as   JM_GARANT_CTO
    ,sum(case when itlo.tabla = 'bi_corp_bdr.JM_GARA_REAL' then itlo.cuenta else 0 end) as   JM_GARA_REAL
    ,sum(case when itlo.tabla = 'bi_corp_bdr.JM_CAL_EMISION' then itlo.cuenta else 0 end) as  JM_CAL_EMISION
    ,sum(case when itlo.tabla = 'bi_corp_bdr.JM_CAL_IN_CL' then itlo.cuenta else 0 end) as  JM_CAL_IN_CL
    ,sum(case when itlo.tabla = 'bi_corp_bdr.JM_CAL_EXT_CL' then itlo.cuenta else 0 end) as  JM_CAL_EXT_CL
    ,sum(case when itlo.tabla = 'bi_corp_bdr.JM_VAL_GARA' then itlo.cuenta else 0 end) as  JM_VAL_GARA
    ,sum(case when itlo.tabla = 'bi_corp_bdr.JM_INF_AD_CLI' then itlo.cuenta else 0 end) as  JM_INF_AD_CLI
from
(
    select count(*) as cuenta, partition_date, 'bi_corp_bdr.JM_TRZ_CLIENTE'  as tabla from  bi_corp_bdr.JM_TRZ_CLIENTE group by partition_date
    union all
    select count(*) as cuenta, partition_date, 'bi_corp_bdr.jm_clien_juri'  as tabla from  bi_corp_bdr.jm_clien_juri group by partition_date
    union all
    select count(*) as cuenta, partition_date, 'bi_corp_bdr.jm_client_bii'  as tabla from bi_corp_bdr.jm_client_bii group by partition_date
    union all
    select count(*) as cuenta, partition_date, 'bi_corp_bdr.jm_contr_bis'  as tabla from bi_corp_bdr.jm_contr_bis  group by partition_date
    union all
    select count(*) as cuenta, partition_date, 'bi_corp_bdr.jm_contr_otros'  as tabla from  bi_corp_bdr.jm_contr_otros group by partition_date
    union all
    select count(*) as cuenta, partition_date, 'bi_corp_bdr.jm_ctos_cance'  as tabla from bi_corp_bdr.jm_ctos_cance group by partition_date
    union all
    select count(*) as cuenta, partition_date, 'bi_corp_bdr.JM_EAD_CONTR'  as tabla from  bi_corp_bdr.JM_EAD_CONTR group by partition_date
    union all
    select count(*) as cuenta, partition_date, 'bi_corp_bdr.jm_expos_no_con'  as tabla from  bi_corp_bdr.jm_expos_no_con group by partition_date
    union all
    select count(*) as cuenta, partition_date, 'bi_corp_bdr.jm_flujos'  as tabla from  bi_corp_bdr.jm_flujos group by partition_date
    union all
    select count(*) as cuenta, partition_date, 'bi_corp_bdr.jm_grup_econo'  as tabla from  bi_corp_bdr.jm_grup_econo group by partition_date
    union all
    select count(*) as cuenta, partition_date, 'bi_corp_bdr.jm_grup_rela'  as tabla from bi_corp_bdr.jm_grup_rela  group by partition_date
    union all
    select count(*) as cuenta, partition_date, 'bi_corp_bdr.jm_interv_cto'  as tabla from bi_corp_bdr.jm_interv_cto group by partition_date
    union all
    select count(*) as cuenta, partition_date, 'bi_corp_bdr.jm_posic_contr'  as tabla from  bi_corp_bdr.jm_posic_contr group by partition_date
    union all
    select count(*) as cuenta, partition_date, 'bi_corp_bdr.JM_PROV_ESOTR'  as tabla from  bi_corp_bdr.JM_PROV_ESOTR group by partition_date
    union all
    select count(*) as cuenta, partition_date, 'bi_corp_bdr.jm_trz_cont_ren'  as tabla from  bi_corp_bdr.jm_trz_cont_ren group by partition_date
    union all
    select count(*) as cuenta, partition_date, 'bi_corp_bdr.jm_vta_carter'  as tabla from  bi_corp_bdr.jm_vta_carter group by partition_date
    union all
    select count(*) as cuenta, partition_date, 'bi_corp_bdr.JM_GARANT_CTO'  as tabla from  bi_corp_bdr.JM_GARANT_CTO group by partition_date
    union all
    select count(*) as cuenta, partition_date, 'bi_corp_bdr.JM_GARA_REAL'  as tabla from  bi_corp_bdr.JM_GARA_REAL group by partition_date
    union all
    select count(*) as cuenta, partition_date, 'bi_corp_bdr.JM_CAL_EMISION'  as tabla from  bi_corp_bdr.JM_CAL_EMISION group by partition_date
    union all
    select count(*) as cuenta, partition_date, 'bi_corp_bdr.JM_CAL_IN_CL'  as tabla from  bi_corp_bdr.JM_CAL_IN_CL group by partition_date
    union all
    select count(*) as cuenta, partition_date, 'bi_corp_bdr.JM_CAL_EXT_CL'  as tabla from  bi_corp_bdr.JM_CAL_EXT_CL group by partition_date
    union all
    select count(*) as cuenta, partition_date, 'bi_corp_bdr.JM_VAL_GARA'  as tabla from  bi_corp_bdr.JM_VAL_GARA group by partition_date
    union all
    select count(*) as cuenta, partition_date, 'bi_corp_bdr.JM_INF_AD_CLI'  as tabla from  bi_corp_bdr.JM_INF_AD_CLI group by partition_date
 ) itlo
group by itlo.partition_date
order by partition_date;