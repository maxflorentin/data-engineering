set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

CREATE TEMPORARY TABLE bi_corp_bdr.divisas as
SELECT DISTINCT tab.t_cod_entidad
                ,tab.t_cod_centro
                ,tab.t_num_cuenta
                ,tab.t_cod_producto
                ,tab.t_cod_subprodu_altair
                ,CASE WHEN tab_div.q_div > 1 THEN 'ARS' ELSE tab.t_cod_divisa END AS divisa
FROM santander_business_risk_ifrs9.ifrs9_tablon tab
INNER JOIN (SELECT t_cod_entidad, t_cod_centro, t_num_cuenta, t_cod_producto, t_cod_subprodu_altair, count(*) as q_div
            FROM santander_business_risk_ifrs9.ifrs9_tablon
            WHERE cast(periodo as string) = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_ifrs9_tablon', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
            GROUP BY t_cod_entidad, t_cod_centro, t_num_cuenta, t_cod_producto, t_cod_subprodu_altair
            ) tab_div
ON tab.t_cod_entidad = tab_div.t_cod_entidad
AND tab.t_cod_centro = tab_div.t_cod_centro
AND tab.t_num_cuenta = tab_div.t_num_cuenta
AND tab.t_cod_producto = tab_div.t_cod_producto
AND tab.t_cod_subprodu_altair = tab_div.t_cod_subprodu_altair
WHERE cast(tab.periodo as string) = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_ifrs9_tablon', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}';



--Si existen saldo y rubro dentro y fuera de balance y estos están completos, entonces se deberá ver reflejado en la BDR 2 registros, uno para cada dupla.

INSERT overwrite table bi_corp_bdr.jm_prov_esotr
partition(partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
SELECT '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' as N0625_FEOPERAC,
        23100 as N0625_S1EMP,
        m.id_cto_bdr as N0625_CONTRA,
        lpad("2", 5, "0") as N0625_TIP_IMPT,
        concat('+', lpad(regexp_replace(format_number(cast(regexp_replace(nvl(sum(a.s_provision_in_balance_bdr),  0) ,"\\,",".") as double), 2),"\\,|\\.|\\+",""),16,"0")) as N0625_IMPORTH,
        divi.divisa as N0625_CODDIV,
        rpad(' ', 40, " ") as N0625_CTA_CONT,
        rpad(' ', 40, " ") as N0625_AGRCTACB,
        rpad('0000', 40, " ") as N0625_CENTCTBL,
        rpad(trim(a.s_rubro_cargabal_in_provision), 40, ' ') as N0625_CTACGBAL,
        lpad(nvl(a.s_final_stage, 0), 5, "0") as N0625_STAGE
from 
    (select a.t_cod_entidad
            ,t_cod_centro
            ,t_num_cuenta
            ,t_cod_producto
            ,t_cod_subprodu_altair
            ,s_provision_in_balance_bdr
            ,s_provision_out_balance_bdr
            ,s_provision_amount_bdr
            ,s_rubro_cargabal_in_provision
            ,s_rubro_cargabal_out_provision
            ,s_rubro_cargabal_provision
            ,s_final_stage 
    from bi_corp_bdr.perim_ifrs9_tablon_prov a 
    where a.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
            and COALESCE(cast(a.s_provision_in_balance_bdr as double),0) > 0 
            and COALESCE(cast(a.s_provision_out_balance_bdr as double),0) > 0 
    ) a
INNER JOIN 
    (
        select m.id_cto_bdr
                ,m.cred_cod_entidad
                ,m.cred_cod_centro
                ,m.cred_num_cuenta
                ,m.cred_cod_producto
                ,m.cred_cod_subprodu_altair
        from bi_corp_bdr.normalizacion_id_contratos m
        where m.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
    ) m
    on a.t_cod_entidad = m.cred_cod_entidad
    AND a.t_cod_centro = m.cred_cod_centro
    AND a.t_num_cuenta = m.cred_num_cuenta
    AND a.t_cod_producto = m.cred_cod_producto
    AND a.t_cod_subprodu_altair = m.cred_cod_subprodu_altair
LEFT JOIN bi_corp_bdr.divisas divi
    ON divi.t_cod_entidad = a.t_cod_entidad
    AND divi.t_cod_centro = a.t_cod_centro
    AND divi.t_num_cuenta = a.t_num_cuenta
    AND divi.t_cod_producto = a.t_cod_producto
    AND divi.t_cod_subprodu_altair = a.t_cod_subprodu_altair    
group by  
        m.id_cto_bdr,    
        divi.divisa,      
        rpad(trim(a.s_rubro_cargabal_in_provision), 40, ' '),
        lpad(nvl(a.s_final_stage, 0), 5, "0");



INSERT INTO TABLE  bi_corp_bdr.jm_prov_esotr partition(partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
SELECT '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' as N0625_FEOPERAC,
        23100 as N0625_S1EMP,
        m.id_cto_bdr as N0625_CONTRA,
        lpad("2", 5, "0") as N0625_TIP_IMPT,
        concat('+', lpad(regexp_replace(format_number(cast(regexp_replace(nvl(sum(a.s_provision_out_balance_bdr),  0) ,"\\,",".") as double), 2),"\\,|\\.|\\+",""),16,"0")) as N0625_IMPORTH,
        divi.divisa as N0625_CODDIV,
        rpad(' ', 40, " ") as N0625_CTA_CONT,
        rpad(' ', 40, " ") as N0625_AGRCTACB,
        rpad('0000', 40, " ") as N0625_CENTCTBL,
        rpad(trim(a.s_rubro_cargabal_out_provision), 40, ' ') as N0625_CTACGBAL,
        lpad(nvl(a.s_final_stage, 0), 5, "0") as N0625_STAGE
from 
    (select a.t_cod_entidad
            ,t_cod_centro
            ,t_num_cuenta
            ,t_cod_producto
            ,t_cod_subprodu_altair
            ,s_provision_in_balance_bdr
            ,s_provision_out_balance_bdr
            ,s_provision_amount_bdr
            ,s_rubro_cargabal_in_provision
            ,s_rubro_cargabal_out_provision
            ,s_rubro_cargabal_provision
            ,s_final_stage 
    from bi_corp_bdr.perim_ifrs9_tablon_prov a 
    where a.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
            and COALESCE(cast(a.s_provision_in_balance_bdr as double),0) > 0 
            and COALESCE(cast(a.s_provision_out_balance_bdr as double),0) > 0 
    ) a
INNER JOIN 
    (
        select m.id_cto_bdr
                ,m.cred_cod_entidad
                ,m.cred_cod_centro
                ,m.cred_num_cuenta
                ,m.cred_cod_producto
                ,m.cred_cod_subprodu_altair
        from bi_corp_bdr.normalizacion_id_contratos m
        where m.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
            
    ) m
    on a.t_cod_entidad = m.cred_cod_entidad
    AND a.t_cod_centro = m.cred_cod_centro
    AND a.t_num_cuenta = m.cred_num_cuenta
    AND a.t_cod_producto = m.cred_cod_producto
    AND a.t_cod_subprodu_altair = m.cred_cod_subprodu_altair
LEFT JOIN bi_corp_bdr.divisas divi
    ON divi.t_cod_entidad = a.t_cod_entidad
    AND divi.t_cod_centro = a.t_cod_centro
    AND divi.t_num_cuenta = a.t_num_cuenta
    AND divi.t_cod_producto = a.t_cod_producto
    AND divi.t_cod_subprodu_altair = a.t_cod_subprodu_altair  
group by  
        m.id_cto_bdr,    
        divi.divisa,      
        rpad(trim(a.s_rubro_cargabal_out_provision), 40, ' '),
        lpad(nvl(a.s_final_stage, 0), 5, "0");


--Si existen solo saldo y rubro dentro de balance y estos están completos, entonces se deberá ver reflejado en la BDR 1 registros asociado al origen.    
INSERT INTO TABLE  bi_corp_bdr.jm_prov_esotr partition(partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
SELECT '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' as N0625_FEOPERAC,
        23100 as N0625_S1EMP,
        m.id_cto_bdr as N0625_CONTRA,
        lpad("2", 5, "0") as N0625_TIP_IMPT,
        concat('+', lpad(regexp_replace(format_number(cast(regexp_replace(nvl(sum(a.s_provision_in_balance_bdr),  0) ,"\\,",".") as double), 2),"\\,|\\.|\\+",""),16,"0")) as N0625_IMPORTH,
        divi.divisa as N0625_CODDIV,
        rpad(' ', 40, " ") as N0625_CTA_CONT,
        rpad(' ', 40, " ") as N0625_AGRCTACB,
        rpad('0000', 40, " ") as N0625_CENTCTBL,
        rpad(trim(a.s_rubro_cargabal_in_provision), 40, ' ') as N0625_CTACGBAL,
        lpad(nvl(a.s_final_stage, 0), 5, "0") as N0625_STAGE
from 
    (select a.t_cod_entidad
            ,t_cod_centro
            ,t_num_cuenta
            ,t_cod_producto
            ,t_cod_subprodu_altair
            ,s_provision_in_balance_bdr
            ,s_provision_out_balance_bdr
            ,s_provision_amount_bdr
            ,s_rubro_cargabal_in_provision
            ,s_rubro_cargabal_out_provision
            ,s_rubro_cargabal_provision
            ,s_final_stage 
    from bi_corp_bdr.perim_ifrs9_tablon_prov a 
    where a.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
            and COALESCE(cast(a.s_provision_in_balance_bdr as double),0) > 0 
            and COALESCE(cast(a.s_provision_out_balance_bdr as double),0) = 0 
    ) a
INNER JOIN 
    (
        select m.id_cto_bdr
                ,m.cred_cod_entidad
                ,m.cred_cod_centro
                ,m.cred_num_cuenta
                ,m.cred_cod_producto
                ,m.cred_cod_subprodu_altair
        from bi_corp_bdr.normalizacion_id_contratos m
        where m.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
    ) m
    on a.t_cod_entidad = m.cred_cod_entidad
    AND a.t_cod_centro = m.cred_cod_centro
    AND a.t_num_cuenta = m.cred_num_cuenta
    AND a.t_cod_producto = m.cred_cod_producto
    AND a.t_cod_subprodu_altair = m.cred_cod_subprodu_altair
LEFT JOIN bi_corp_bdr.divisas divi
    ON divi.t_cod_entidad = a.t_cod_entidad
    AND divi.t_cod_centro = a.t_cod_centro
    AND divi.t_num_cuenta = a.t_num_cuenta
    AND divi.t_cod_producto = a.t_cod_producto
    AND divi.t_cod_subprodu_altair = a.t_cod_subprodu_altair   
group by  
        m.id_cto_bdr,    
        divi.divisa,      
        rpad(trim(a.s_rubro_cargabal_in_provision), 40, ' '),
        lpad(nvl(a.s_final_stage, 0), 5, "0");

--Si existen solo saldo y rubro fuera de balance y estos están completos, entonces se deberá ver reflejado en la BDR 1 registros asociado al origen.
INSERT INTO TABLE  bi_corp_bdr.jm_prov_esotr partition(partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
SELECT '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' as N0625_FEOPERAC,
        23100 as N0625_S1EMP,
        m.id_cto_bdr as N0625_CONTRA,
        lpad("2", 5, "0") as N0625_TIP_IMPT,
        concat('+', lpad(regexp_replace(format_number(cast(regexp_replace(nvl(sum(a.s_provision_out_balance_bdr),  0) ,"\\,",".") as double), 2),"\\,|\\.|\\+",""),16,"0")) as N0625_IMPORTH,
        divi.divisa as N0625_CODDIV,
        rpad(' ', 40, " ") as N0625_CTA_CONT,
        rpad(' ', 40, " ") as N0625_AGRCTACB,
        rpad('0000', 40, " ") as N0625_CENTCTBL,
        rpad(trim(a.s_rubro_cargabal_out_provision), 40, ' ') as N0625_CTACGBAL,
        lpad(nvl(a.s_final_stage, 0), 5, "0") as N0625_STAGE
from 
    (select a.t_cod_entidad
            ,t_cod_centro
            ,t_num_cuenta
            ,t_cod_producto
            ,t_cod_subprodu_altair
            ,s_provision_in_balance_bdr
            ,s_provision_out_balance_bdr
            ,s_provision_amount_bdr
            ,s_rubro_cargabal_in_provision
            ,s_rubro_cargabal_out_provision
            ,s_rubro_cargabal_provision
            ,s_final_stage 
    from bi_corp_bdr.perim_ifrs9_tablon_prov a 
    where a.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
            and COALESCE(cast(a.s_provision_in_balance_bdr as double),0) = 0 
            and COALESCE(cast(a.s_provision_out_balance_bdr as double),0) > 0  
    ) a
INNER JOIN 
    (
        select m.id_cto_bdr
                ,m.cred_cod_entidad
                ,m.cred_cod_centro
                ,m.cred_num_cuenta
                ,m.cred_cod_producto
                ,m.cred_cod_subprodu_altair
        from bi_corp_bdr.normalizacion_id_contratos m
        where m.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
    ) m
    on a.t_cod_entidad = m.cred_cod_entidad
    AND a.t_cod_centro = m.cred_cod_centro
    AND a.t_num_cuenta = m.cred_num_cuenta
    AND a.t_cod_producto = m.cred_cod_producto
    AND a.t_cod_subprodu_altair = m.cred_cod_subprodu_altair
LEFT JOIN bi_corp_bdr.divisas divi
    ON divi.t_cod_entidad = a.t_cod_entidad
    AND divi.t_cod_centro = a.t_cod_centro
    AND divi.t_num_cuenta = a.t_num_cuenta
    AND divi.t_cod_producto = a.t_cod_producto
    AND divi.t_cod_subprodu_altair = a.t_cod_subprodu_altair  
group by  
        m.id_cto_bdr,    
        divi.divisa,      
        rpad(trim(a.s_rubro_cargabal_out_provision), 40, ' '),
        lpad(nvl(a.s_final_stage, 0), 5, "0");


--MMFF Provisiones

INSERT INTO TABLE  bi_corp_bdr.jm_prov_esotr
partition(partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
 select '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'  N0625_FEOPERAC
        ,lpad(nvl(23100,'0'),5,'0') AS N0625_S1EMP
        ,nc.id_cto_bdr AS E0621_CONTRA1
        ,case when nvl(ift.s_provision_in_balance_bdr,  0) > 0 then '00002'
            else '99999'
         end AS N0625_TIP_IMPT
        ,concat('+', lpad(regexp_replace(format_number(cast(regexp_replace(nvl(ift.s_provision_in_balance_bdr * tac.porcentaje,  0) ,"\\,",".") as double), 2),"\\,|\\.|\\+",""),16,"0")) as N0625_IMPORTH
        ,tac.currency AS N0625_CODDIV
        ,rpad(' ', 40, " ") AS N0625_CTA_CONT
        ,rpad(' ', 40, " ") AS N0625_AGRCTACB
        ,rpad('0000', 40, " ") AS N0625_CENTCTBL
        ,rpad(trim(ift.s_rubro_cargabal_in_provision), 40, ' ') AS N0625_CTACGBAL
        ,lpad(nvl(ift.s_final_stage, 0), 5, "0") as N0625_STAGE
from 
    (
    select distinct cbi.g4095_contra1
    from bi_corp_bdr.jm_contr_bis cbi
    where cbi.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
    ) cbi
inner join 
         (
         select nc.id_cto_bdr
                , nc.id_cto_source
         from bi_corp_bdr.normalizacion_id_contratos nc
         where nc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
          and nc.source = 'mmff-tactico' 
          ) nc
        on nc.id_cto_bdr = cbi.g4095_contra1
inner join    
    (
        select cast(tac.suma_nominales as double) / (sum(cast(tac.suma_nominales as double) ) over (partition by tac.especie)) porcentaje
                 , tac.especie 
                 , tac.currency 
                 , tac.cuenta
                 , tac.isin
        from bi_corp_staging.mmff_tactico_especie tac
        where tac.partition_date  = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_mmff_tactico_especie', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
         
    ) tac
     on  nc.id_cto_source = concat_ws('_', trim(tac.cuenta),trim(tac.isin))
inner join        
(   

         select 
                    ift.t_idf_cto_ifrs
                    ,cast(ift.s_provision_in_balance_bdr as double) as s_provision_in_balance_bdr
                    ,ift.s_rubro_cargabal_in_provision
                    ,ift.s_final_stage   
        from bi_corp_bdr.perim_ifrs9_tablon_prov_mmff ift
        where ift.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
            and COALESCE(cast(ift.s_provision_in_balance_bdr as double),0) > 0 
            and COALESCE(cast(ift.s_provision_out_balance_bdr as double),0) > 0           
        
      ) ift
on trim(ift.t_idf_cto_ifrs) = trim(tac.especie);

        
        
INSERT INTO TABLE  bi_corp_bdr.jm_prov_esotr
partition(partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')        
 select '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'  N0625_FEOPERAC
        ,lpad(nvl(23100,'0'),5,'0') AS N0625_S1EMP
        ,nc.id_cto_bdr AS E0621_CONTRA1
        ,case when nvl(ift.s_provision_out_balance_bdr,  0) > 0 then '00002'
            else '99999'
         end AS N0625_TIP_IMPT
        ,concat('+', lpad(regexp_replace(format_number(cast(regexp_replace(nvl(ift.s_provision_out_balance_bdr * tac.porcentaje,  0) ,"\\,",".") as double), 2),"\\,|\\.|\\+",""),16,"0")) as N0625_IMPORTH
        ,tac.currency AS N0625_CODDIV
        ,rpad(' ', 40, " ") AS N0625_CTA_CONT
        ,rpad(' ', 40, " ") AS N0625_AGRCTACB
        ,rpad('0000', 40, " ") AS N0625_CENTCTBL
        ,rpad(trim(ift.s_rubro_cargabal_out_provision), 40, ' ') AS N0625_CTACGBAL
        ,lpad(nvl(ift.s_final_stage, 0), 5, "0") as N0625_STAGE
from 
    (
    select distinct cbi.g4095_contra1
    from bi_corp_bdr.jm_contr_bis cbi
    where cbi.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
    ) cbi
inner join 
         (
         select nc.id_cto_bdr
                , nc.id_cto_source
         from bi_corp_bdr.normalizacion_id_contratos nc
         where nc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
          and nc.source = 'mmff-tactico' 
          ) nc
        on nc.id_cto_bdr = cbi.g4095_contra1
inner join    
    (
        select cast(tac.suma_nominales as double) / (sum(cast(tac.suma_nominales as double) ) over (partition by tac.especie)) porcentaje
                 , tac.especie 
                 , tac.currency 
                 , tac.cuenta
                 , tac.isin
        from bi_corp_staging.mmff_tactico_especie tac
        where tac.partition_date  = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_mmff_tactico_especie', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
         
    ) tac
     on  nc.id_cto_source = concat_ws('_', trim(tac.cuenta),trim(tac.isin))
inner join        
(   

         select 
                    ift.t_idf_cto_ifrs
                    ,cast(ift.s_provision_out_balance_bdr as double) as s_provision_out_balance_bdr
                    ,ift.s_rubro_cargabal_out_provision
                    ,ift.s_final_stage   
        from bi_corp_bdr.perim_ifrs9_tablon_prov_mmff ift
        where ift.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
            and COALESCE(cast(ift.s_provision_in_balance_bdr as double),0) > 0 
            and COALESCE(cast(ift.s_provision_out_balance_bdr as double),0) > 0           
        
      ) ift
on trim(ift.t_idf_cto_ifrs) = trim(tac.especie);
        
 --Si existen solo saldo y rubro dentro de balance y estos están completos, entonces se deberá ver reflejado en la BDR 1 registros asociado al origen.    

INSERT INTO TABLE  bi_corp_bdr.jm_prov_esotr
partition(partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')       
 select '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'  N0625_FEOPERAC
        ,lpad(nvl(23100,'0'),5,'0') AS N0625_S1EMP
        ,nc.id_cto_bdr AS E0621_CONTRA1
        ,case when nvl(ift.s_provision_in_balance_bdr,  0) > 0 then '00002'
            else '99999'
         end AS N0625_TIP_IMPT
        ,concat('+', lpad(regexp_replace(format_number(cast(regexp_replace(nvl(ift.s_provision_in_balance_bdr * tac.porcentaje,  0) ,"\\,",".") as double), 2),"\\,|\\.|\\+",""),16,"0")) as N0625_IMPORTH
        ,tac.currency AS N0625_CODDIV
        ,rpad(' ', 40, " ") AS N0625_CTA_CONT
        ,rpad(' ', 40, " ") AS N0625_AGRCTACB
        ,rpad('0000', 40, " ") AS N0625_CENTCTBL
        ,rpad(trim(ift.s_rubro_cargabal_in_provision), 40, ' ') AS N0625_CTACGBAL
        ,lpad(nvl(ift.s_final_stage, 0), 5, "0") as N0625_STAGE
from 
    (
    select distinct cbi.g4095_contra1
    from bi_corp_bdr.jm_contr_bis cbi
    where cbi.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
    ) cbi
inner join 
         (
         select nc.id_cto_bdr
                , nc.id_cto_source
         from bi_corp_bdr.normalizacion_id_contratos nc
         where nc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
          and nc.source = 'mmff-tactico' 
          ) nc
        on nc.id_cto_bdr = cbi.g4095_contra1
inner join    
    (
        select cast(tac.suma_nominales as double) / (sum(cast(tac.suma_nominales as double) ) over (partition by tac.especie)) porcentaje
                 , tac.especie 
                 , tac.currency 
                 , tac.cuenta
                 , tac.isin
        from bi_corp_staging.mmff_tactico_especie tac
        where tac.partition_date  = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_mmff_tactico_especie', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
         
    ) tac
     on  nc.id_cto_source = concat_ws('_', trim(tac.cuenta),trim(tac.isin))
inner join        
(   

         select 
                    ift.t_idf_cto_ifrs
                    ,cast(ift.s_provision_in_balance_bdr as double) as s_provision_in_balance_bdr
                    ,ift.s_rubro_cargabal_in_provision
                    ,ift.s_final_stage   
        from bi_corp_bdr.perim_ifrs9_tablon_prov_mmff ift
        where ift.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
            and COALESCE(cast(ift.s_provision_in_balance_bdr as double),0) > 0 
            and COALESCE(cast(ift.s_provision_out_balance_bdr as double),0) = 0           
        
      ) ift
on trim(ift.t_idf_cto_ifrs) = trim(tac.especie);




--Si existen solo saldo y rubro fuera de balance y estos están completos, entonces se deberá ver reflejado en la BDR 1 registros asociado al origen.
INSERT INTO TABLE  bi_corp_bdr.jm_prov_esotr
partition(partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}') 
 select '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'  N0625_FEOPERAC
        ,lpad(nvl(23100,'0'),5,'0') AS N0625_S1EMP
        ,nc.id_cto_bdr AS E0621_CONTRA1
        ,case when nvl(ift.s_provision_out_balance_bdr,  0) > 0 then '00002'
            else '99999'
         end AS N0625_TIP_IMPT
        ,concat('+', lpad(regexp_replace(format_number(cast(regexp_replace(nvl(ift.s_provision_out_balance_bdr * tac.porcentaje,  0) ,"\\,",".") as double), 2),"\\,|\\.|\\+",""),16,"0")) as N0625_IMPORTH
        ,tac.currency AS N0625_CODDIV
        ,rpad(' ', 40, " ") AS N0625_CTA_CONT
        ,rpad(' ', 40, " ") AS N0625_AGRCTACB
        ,rpad('0000', 40, " ") AS N0625_CENTCTBL
        ,rpad(trim(ift.s_rubro_cargabal_out_provision), 40, ' ') AS N0625_CTACGBAL
        ,lpad(nvl(ift.s_final_stage, 0), 5, "0") as N0625_STAGE
from 
    (
    select distinct cbi.g4095_contra1
    from bi_corp_bdr.jm_contr_bis cbi
    where cbi.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
    ) cbi
inner join 
         (
         select nc.id_cto_bdr
                , nc.id_cto_source
         from bi_corp_bdr.normalizacion_id_contratos nc
         where nc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
          and nc.source = 'mmff-tactico' 
          ) nc
        on nc.id_cto_bdr = cbi.g4095_contra1
inner join    
    (
        select cast(tac.suma_nominales as double) / (sum(cast(tac.suma_nominales as double) ) over (partition by tac.especie)) porcentaje
                 , tac.especie 
                 , tac.currency 
                 , tac.cuenta
                 , tac.isin
        from bi_corp_staging.mmff_tactico_especie tac
        where tac.partition_date  = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_mmff_tactico_especie', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
         
    ) tac
     on  nc.id_cto_source = concat_ws('_', trim(tac.cuenta),trim(tac.isin))
inner join        
(   

         select 
                    ift.t_idf_cto_ifrs
                    ,cast(ift.s_provision_out_balance_bdr as double) as s_provision_out_balance_bdr
                    ,ift.s_rubro_cargabal_out_provision
                    ,ift.s_final_stage   
        from bi_corp_bdr.perim_ifrs9_tablon_prov_mmff ift
        where ift.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
            and COALESCE(cast(ift.s_provision_in_balance_bdr as double),0) = 0 
            and COALESCE(cast(ift.s_provision_out_balance_bdr as double),0) > 0           
        
      ) ift
on trim(ift.t_idf_cto_ifrs) = trim(tac.especie);