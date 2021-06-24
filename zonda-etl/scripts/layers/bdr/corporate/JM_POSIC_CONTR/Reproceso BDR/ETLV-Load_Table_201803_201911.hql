set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

CREATE TEMPORARY TABLE temp_import_posicion (
e0621_feoperac string,
e0621_s1emp  string,
e0621_contra1 string,
e0621_cta_cont string,
e0621_tip_impt string,
e0621_fec_mes string,
e0621_agrctacb string,
e0621_importh string,
e0621_coddiv string,
e0621_fecultmo string,
e0621_centctbl string,
e0621_ctacgbal string); 


with cto_perimetro as (
    select   m.id_cto_bdr
            ,m.id_cto_source
            ,m.cred_cod_entidad as cod_entidad
            ,m.cred_cod_centro as cod_centro
            ,m.cred_num_cuenta as num_cuenta
            ,m.cred_cod_producto as cod_producto
            ,m.cred_cod_subprodu_altair as cod_subprodu
    from bi_corp_bdr.jm_contr_bis cb
    INNER JOIN bi_corp_bdr.normalizacion_id_contratos m
           on cb.partition_date = m.partition_date
          and m.id_cto_bdr = cb.G4095_CONTRA1
          AND m.source = 'credito'
    where cb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
),
cto_deudores as (
SELECT   b.cod_entidad
        ,b.num_cuenta
        ,b.cod_producto
        ,b.cod_subprodu
        ,b.cod_rubro_altair
        ,b.cod_rubro_bcra
        ,b.cod_rubro_cargabal
        ,b.cod_centro
        ,b.dias_de_atraso
        ,b.data_date_part
        ,CASE WHEN k.q_div = 1 
            THEN b.cod_divisa 
            ELSE 'ARS' 
        END divisa
        ,sum(b.imp_deuda) as imp_deuda
FROM santander_business_risk_arda.contratos_adsf b
    INNER JOIN (SELECT cod_entidad
                        ,num_cuenta
                        ,cod_producto
                        ,cod_subprodu
                        ,cod_centro
                        ,count(*) as q_div
                FROM santander_business_risk_arda.contratos_adsf
                WHERE data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_contratos_adsf', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
                GROUP BY cod_entidad,num_cuenta,cod_producto,cod_subprodu,cod_centro
                ) k
    ON k.cod_entidad = b.cod_entidad
    AND k.cod_centro = b.cod_centro
    AND k.num_cuenta = b.num_cuenta
    AND k.cod_producto = b.cod_producto
    AND k.cod_subprodu = b.cod_subprodu
WHERE b.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_contratos_adsf', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
GROUP BY b.cod_entidad
        ,b.num_cuenta
        ,b.cod_producto
        ,b.cod_subprodu
        ,b.cod_rubro_altair
        ,b.cod_rubro_bcra
        ,b.cod_rubro_cargabal
        ,b.cod_centro
        ,b.dias_de_atraso
        ,b.data_date_part
        ,CASE WHEN k.q_div = 1 
                THEN b.cod_divisa 
                ELSE 'ARS' 
        END
UNION ALL
SELECT   x.cod_entidad
        ,x.num_cuenta
        ,x.cod_producto
        ,x.cod_subprodu
        , x.cod_rubro_altair
        ,x.cod_rubro_bcra
        ,x.cod_rubro_cargabal
        ,x.cod_centro
        ,cast(null as string) as dias_de_atraso
        ,x.data_date_part
        ,CASE WHEN k.q_div = 1 THEN x.cod_divisa ELSE 'ARS' END divisa,
        sum(x.imp_deuda) as imp_deuda
FROM santander_business_risk_arda.contratos_deudores_adic x
    INNER JOIN (SELECT  cod_entidad
                        ,num_cuenta
                        ,cod_producto
                        ,cod_subprodu
                        ,cod_centro
                        ,count(*) q_div
                FROM santander_business_risk_arda.contratos_deudores_adic
                WHERE data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_contratos_deudores_adic', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
                GROUP BY cod_entidad
                        ,num_cuenta
                        ,cod_producto
                        ,cod_subprodu
                        ,cod_centro
                ) k
    ON k.cod_entidad = x.cod_entidad
    AND k.cod_centro = x.cod_centro
    AND k.num_cuenta = x.num_cuenta
    AND k.cod_producto = x.cod_producto
    AND k.cod_subprodu = x.cod_subprodu
WHERE x.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_contratos_deudores_adic', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
GROUP BY x.cod_entidad
        ,x.num_cuenta
        ,x.cod_producto
        ,x.cod_subprodu
        ,x.cod_rubro_altair
        ,x.cod_rubro_bcra
        ,x.cod_rubro_cargabal
        ,x.cod_centro
        ,cast(null as string) 
        ,x.data_date_part
        ,CASE WHEN k.q_div = 1 
                THEN x.cod_divisa 
                ELSE 'ARS' 
        END)
INSERT OVERWRITE TABLE temp_import_posicion 
SELECT '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' AS E0621_FEOPERAC
        ,23100 AS E0621_S1EMP
        ,ncto_div.id_cto_bdr AS E0621_CONTRA1
        ,rpad(nvl(c.cod_rubro_altair, ' '), 40, ' ') AS E0621_CTA_CONT
        ,CASE WHEN nvl(c.dias_de_atraso, 0) = 0 THEN '00001'
              WHEN nvl(c.dias_de_atraso, 0) BETWEEN  1 AND 30 THEN '00002'
              WHEN nvl(c.dias_de_atraso, 0) BETWEEN 31 AND 90 THEN '00003'
              WHEN nvl(c.dias_de_atraso, 0) > 90 THEN '00004'
              ELSE '00000' 
        END AS E0621_TIP_IMPT
        ,'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' AS E0621_FEC_MES
        ,rpad(nvl(c.cod_rubro_bcra, ' '), 40, ' ') AS E0621_AGRCTACB
        ,case when cast(nvl(sum(c.imp_deuda)*-1,0) as double) < 0
                then concat('-', lpad(regexp_replace(format_number(cast(nvl(sum(c.imp_deuda)*-1,0) as double), 2),'\\,|\\.|\\-',''),16,'0'))
                else lpad(regexp_replace(format_number(cast(nvl(sum(c.imp_deuda)*-1,0) as double), 2),'\\,|\\.|\\-',''),17,'0') 
        end AS E0621_IMPORTH
        ,c.divisa AS E0621_CODDIV
        ,'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' AS E0621_FECULTMO
        ,rpad(nvl(c.cod_centro, ' '), 40, ' ') AS E0621_CENTCTBL
        ,rpad(nvl(c.cod_rubro_cargabal, ' '), 40, ' ') AS E0621_CTACGBAL
FROM cto_perimetro ncto_div
    INNER JOIN cto_deudores c
    ON ncto_div.cod_entidad = c.cod_entidad
    AND ncto_div.cod_centro = c.cod_centro
    AND ncto_div.num_cuenta = c.num_cuenta
    AND ncto_div.cod_producto = c.cod_producto
    AND ncto_div.cod_subprodu = c.cod_subprodu
GROUP BY
    ncto_div.id_cto_bdr
    ,rpad(nvl(c.cod_rubro_altair, ' '), 40, ' ')
    ,CASE WHEN nvl(c.dias_de_atraso, 0) = 0 THEN '00001'
         WHEN nvl(c.dias_de_atraso, 0) BETWEEN 1 AND 30 THEN '00002'
         WHEN nvl(c.dias_de_atraso, 0) BETWEEN 31 AND 90 THEN '00003'
         WHEN nvl(c.dias_de_atraso, 0) > 90 THEN '00004'
         ELSE '00000' 
    END
    ,rpad(nvl(c.cod_rubro_bcra, ' '), 40, ' ')
    ,c.divisa
    ,rpad(nvl(c.cod_centro, ' '), 40, ' ')
    ,rpad(nvl(c.cod_rubro_cargabal, ' '), 40, ' ');

-- inserto el perímetro de tarjetas con su saldo disponible.
with cto_perimetro as (
    select DISTINCT
           m.id_cto_bdr, m.id_cto_source, m.cred_cod_entidad as cod_entidad, m.cred_cod_centro as cod_centro,
           m.cred_num_cuenta as num_cuenta, m.cred_cod_producto as cod_producto, m.cred_cod_subprodu_altair as cod_subprodu
    from bi_corp_bdr.jm_contr_bis cb
    INNER JOIN bi_corp_bdr.normalizacion_id_contratos m
           on m.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
          and m.id_cto_bdr = cb.G4095_CONTRA1
    inner join santander_business_risk_arda.CONTRATOS co
            on co.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_contratos', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
           and concat_ws('_',co.cod_entidad,co.cod_centro,co.num_cuenta,co.cod_producto,co.cod_subprodu_altair) = m.id_cto_source
           and co.cod_producto in ('40', '41', '42')
           and nvl(co.ind_mora_pcr16,0) <> 1
           and nvl(co.cod_est_cta,0) not in (11,12,19)
    where cb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
),
 core_datos as (
select p.id_cto_bdr, p.id_cto_source, p.cod_centro, sum(nvl(co.imp_deuda,0)) as deuda, max(nvl(co.imp_lim_credito_ml,0)) as lim_cred,
       max(nvl(co.imp_lim_credito_ml,0)) - sum(nvl(co.imp_deuda,0)) as disponible
  from cto_perimetro p
inner join santander_business_risk_arda.contratos co
       on co.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_contratos', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
      and concat_ws('_',co.cod_entidad,co.cod_centro,co.num_cuenta,co.cod_producto,co.cod_subprodu_altair) = p.id_cto_source
group by p.id_cto_bdr, p.id_cto_source, p.cod_centro
having sum(nvl(co.imp_deuda,0)) < max(nvl(co.imp_lim_credito_ml,0)))
INSERT into TABLE temp_import_posicion
select
      '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' as E0621_FEOPERAC,
      23100 as E0621_S1EMP,
      lpad(t.id_cto_bdr,9,'0') as E0621_CONTRA1,
      rpad(case when g4093_clisegl2 in ('00001','00004') then '7110018'
                when g4093_clisegl2 in ('00002','00003','00006','00011','00012','00014','00015','00017','00018','00019','00020','00021','00022','00023','00024') then '7110019'
                when g4093_clisegl2 in ('00007','00008','00009','00010') then '7110021'
                else '' end,
           40, ' ')  as E0621_CTA_CONT,
      '00001' as E0621_TIP_IMPT,
      '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'  as E0621_FEC_MES,
      rpad('711084',40,' ') as E0621_AGRCTACB,
      case when cast(t.disponible *-1 as double) < 0 then
                  concat('-', lpad(regexp_replace(format_number(cast(t.disponible*-1 as double), 2),'\\,|\\.|\\-',''),16,'0'))
             else
                              lpad(regexp_replace(format_number(cast(t.disponible*-1 as double), 2),'\\,|\\.|\\+',''),17,'0')
             end
                 as E0621_IMPORTH,
      'ARS' as E0621_CODDIV,
      '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' as E0621_FECULTMO,
      rpad(cod_centro,40, ' ') as E0621_CENTCTBL,
      rpad(case when g4093_clisegl2 in ('00001','00004') then '310200'
                when g4093_clisegl2 in ('00002','00003','00006','00011','00012','00014','00015','00017','00018','00019','00020','00021','00022','00023','00024') then '310201'
                when g4093_clisegl2 in ('00007','00008','00009','00010') then '310202'
                else '' end,
           40, ' ') as E0621_CTACGBAL
from core_datos t
inner join bi_corp_bdr.jm_interv_cto ic
    on ic.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
   and ic.g4128_contra1 = t.id_cto_bdr
   and ic.g4128_tipintev = '00001'
inner join bi_corp_bdr.jm_client_bii cl
    on cl.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
   and cl.g4093_idnumcli = ic.g4128_idnumcli;

-- inserto el perímetro de acuerdos utilizado para cuentas.
with cto_perimetro as (
    select m.id_cto_bdr, m.id_cto_source, m.cred_cod_entidad as cod_entidad, m.cred_cod_centro as cod_centro,
           m.cred_num_cuenta as num_cuenta, m.cred_cod_producto as cod_producto, m.cred_cod_subprodu_altair as cod_subprodu,
           max(nvl(co.imp_deuda,0)) as deuda, max(nvl(co.imp_lim_credito_ml,0)) as lim_cred,
           max(nvl(co.imp_lim_credito_ml,0)) - max(nvl(co.imp_deuda,0)) as disponible
    from bi_corp_bdr.jm_contr_bis cb
    INNER JOIN bi_corp_bdr.normalizacion_id_contratos m
           on m.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
          and m.id_cto_bdr = cb.G4095_CONTRA1
    inner join santander_business_risk_arda.CONTRATOS co
            on co.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_contratos', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
           and concat_ws('_',co.cod_entidad,co.cod_centro,co.num_cuenta,co.cod_producto,co.cod_subprodu_altair) = m.id_cto_source
           and co.cod_producto in ('05', '06', '07')
           and nvl(co.ind_mora_pcr16,0) <> 1
    where cb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
   group by m.id_cto_bdr, m.id_cto_source, m.cred_cod_entidad, m.cred_cod_centro, m.cred_num_cuenta, m.cred_cod_producto,
            m.cred_cod_subprodu_altair
   having max(nvl(co.imp_deuda,0)) < max(nvl(co.imp_lim_credito_ml,0))
)
INSERT into TABLE temp_import_posicion
select
      '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' as E0621_FEOPERAC,
      23100 as E0621_S1EMP,
      lpad(t.id_cto_bdr,9,'0') as E0621_CONTRA1,
      rpad('711003000001', 40, ' ')  as E0621_CTA_CONT,
      '00001' as E0621_TIP_IMPT,
      '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' as E0621_FEC_MES,
      rpad('711084',40,' ') as E0621_AGRCTACB,
      case when cast(t.disponible *-1 as double) < 0 then
                  concat('-', lpad(regexp_replace(format_number(cast(t.disponible*-1 as double), 2),'\\,|\\.|\\-',''),16,'0'))
             else
                              lpad(regexp_replace(format_number(cast(t.disponible*-1 as double), 2),'\\,|\\.|\\+',''),17,'0')
             end
                 as E0621_IMPORTH,
      'ARS' as E0621_CODDIV,
      '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' as E0621_FECULTMO,
      rpad(cod_centro, 40, ' ') as E0621_CENTCTBL,
      rpad('310202', 40, ' ') as E0621_CTACGBAL
from cto_perimetro t;


--Reclasificación Cargabal

DROP TABLE temp_recla_hogares;
        
--Reclasificación Hogares Cargabal        
CREATE TEMPORARY TABLE temp_recla_hogares AS
    select  
    distinct ipc.e0621_feoperac
    ,ipc.e0621_s1emp
    ,ipc.e0621_contra1
    ,ipc.e0621_cta_cont
    ,ipc.e0621_tip_impt
    ,ipc.e0621_fec_mes
    ,ipc.e0621_agrctacb
    ,ipc.e0621_importh
    ,ipc.e0621_coddiv
    ,ipc.e0621_fecultmo
    ,ipc.e0621_centctbl
    , nvl(case when substring(trim(ipc.e0621_ctacgbal),1,2) = '12' then      
                        case when substring(trim(ipc.e0621_ctacgbal),1,4) = '1224' 
                                then trim(ipc.e0621_ctacgbal)
                              when (trim(cmr.ind_riesgo) = 'PFPYME' or  trim(cmr.marca_bp)  = 'M') 
                                then trim(ipc.e0621_ctacgbal)
                             else trim(CONCAT(substring(trim(ipc.e0621_ctacgbal),1,3), cast(cast(trim(cbi.g4093_cod_sec3) as bigint) as string), substring(trim(ipc.e0621_ctacgbal),5,LENGTH(trim(ipc.e0621_ctacgbal)))))
                        end
                    else trim(ipc.e0621_ctacgbal)
              end, trim(ipc.e0621_ctacgbal))  as e0621_ctacgbal
    from
            (
                select distinct ipc.e0621_feoperac
                        ,ipc.e0621_s1emp
                        ,ipc.e0621_contra1
                        ,ipc.e0621_cta_cont
                        ,ipc.e0621_tip_impt
                        ,ipc.e0621_fec_mes
                        ,ipc.e0621_agrctacb
                        ,ipc.e0621_importh
                        ,ipc.e0621_coddiv
                        ,ipc.e0621_fecultmo
                        ,ipc.e0621_centctbl
                        ,ipc.e0621_ctacgbal
                from temp_import_posicion ipc
            ) ipc
        left join 
            (
                select ict.g4128_contra1
                        ,ict.g4128_idnumcli
                from bi_corp_bdr.jm_interv_cto ict
                where ict.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'  
                    and ict.g4128_tipintev = '00001' and g4128_numordin = '00000000001000000'
            ) ict on trim(ipc.e0621_contra1) = trim(ict.g4128_contra1)
        left join
            (
                select distinct cbi.g4093_idnumcli
                                ,cbi.g4093_cod_sec3
                from bi_corp_bdr.jm_client_bii cbi
                where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'  
            ) cbi on trim(ict.g4128_idnumcli) = trim(cbi.g4093_idnumcli)
        left join 
            (
                select  cmr.num_persona
                        ,cmr.ind_riesgo
                        ,cmr.marca_bp
                from bi_corp_staging.view_clientes_en_mora cmr
                where fec_periodo = '{{ ti.xcom_pull(task_ids='InputConfig', key='month', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' 
            ) cmr on lpad(trim(cmr.num_persona),9,"0") = trim(cbi.g4093_idnumcli); 
        
    
 drop table temp_recla_hogares_2;
 
 CREATE TEMPORARY TABLE temp_recla_hogares_2 AS 
 select ipc.e0621_feoperac
    ,ipc.e0621_s1emp
    ,ipc.e0621_contra1
    ,ipc.e0621_cta_cont
    ,ipc.e0621_tip_impt
    ,ipc.e0621_fec_mes
    ,ipc.e0621_agrctacb
    ,ipc.e0621_importh
    ,ipc.e0621_coddiv
    ,ipc.e0621_fecultmo
    ,ipc.e0621_centctbl
    , nvl(case  when trim(ipc.e0621_ctacgbal) = '122215E' -- Cuarto Filtro 
                                then  trim(CONCAT( 
                                            substring(trim(ipc.e0621_ctacgbal),1, LENGTH(trim(ipc.e0621_ctacgbal))-1),
                                            '9',
                                             substring(trim(ipc.e0621_ctacgbal),LENGTH(trim(ipc.e0621_ctacgbal)),LENGTH(trim(ipc.e0621_ctacgbal)))
                                            )
                                           )
                when  trim(ipc.e0621_ctacgbal) in ('122000,122001,122015')
                    then  trim(CONCAT(substring(trim(ipc.e0621_ctacgbal),1,3), '4', substring(trim(ipc.e0621_ctacgbal),5,LENGTH(trim(ipc.e0621_ctacgbal)))))
                    else trim(ipc.e0621_ctacgbal)
              end, trim(ipc.e0621_ctacgbal))  as e0621_ctacgbal
 from temp_recla_hogares ipc;               
        
drop table temp_recla_morosidad;  

--Reclasificación Morosidad Cargabal
CREATE TEMPORARY TABLE temp_recla_morosidad AS
select distinct 
    rhg.e0621_feoperac
    ,rhg.e0621_s1emp
    ,rhg.e0621_contra1
    ,rhg.e0621_cta_cont
    ,rhg.e0621_tip_impt
    ,rhg.e0621_fec_mes
    ,rhg.e0621_agrctacb
    ,rhg.e0621_importh
    ,rhg.e0621_coddiv
    ,rhg.e0621_fecultmo
    ,rhg.e0621_centctbl
    ,nvl(case when substring(trim(rhg.e0621_ctacgbal),1,2) = '12' then   
            case when mrm.ind_mora_pcr16 = '1'
                  then trim(recla.rubro_cargabal_14)
                  else trim(rhg.e0621_ctacgbal)
            end 
          else trim(rhg.e0621_ctacgbal) 
     end,trim(rhg.e0621_ctacgbal))  as e0621_ctacgbal
from 
    (
        select 
            ipc.e0621_feoperac
            ,ipc.e0621_s1emp
            ,ipc.e0621_contra1
            ,ipc.e0621_cta_cont
            ,ipc.e0621_tip_impt
            ,ipc.e0621_fec_mes
            ,ipc.e0621_agrctacb
            ,ipc.e0621_importh
            ,ipc.e0621_coddiv
            ,ipc.e0621_fecultmo
            ,ipc.e0621_centctbl
            ,ipc.e0621_ctacgbal
        from temp_recla_hogares_2 ipc
    ) rhg
left join 
    (
        select norm.id_cto_bdr        
                ,norm.cred_cod_centro
                ,norm.cred_cod_entidad
                ,norm.cred_num_cuenta
                ,norm.cred_cod_producto
                ,norm.cred_cod_subprodu_altair
        from bi_corp_bdr.normalizacion_id_contratos norm
        where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'  
    ) norm on
    trim(rhg.e0621_contra1) = trim(norm.id_cto_bdr)
left join 
    (
        select  mrm.cod_centro
                ,mrm.cod_entidad
                ,mrm.num_cuenta
                ,mrm.cod_producto
                ,mrm.cod_subprodu
                ,mrm.ind_mora_pcr16
        from santander_business_risk_arda.marcas_riesgo mrm
        where data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_marcas_riesgo', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'  
    ) mrm on
        trim(mrm.cod_centro) = trim(norm.cred_cod_centro)
        and trim(mrm.cod_entidad) = trim(norm.cred_cod_entidad)
        and trim(mrm.num_cuenta) = trim(norm.cred_num_cuenta)
        and trim(mrm.cod_producto) = trim(norm.cred_cod_producto)
        and trim(mrm.cod_subprodu) = trim(norm.cred_cod_subprodu_altair) 
left join 
    (select recla.rubro_cargabal_12
            ,recla.rubro_cargabal_14
    from bi_corp_staging.recla_moro_cargabal recla
    where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig',key='max_partition_recla_moro_cargabal', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
    ) recla on
    trim(rhg.e0621_ctacgbal) = trim(recla.rubro_cargabal_12);
 
drop table temp_recla_adicional;

--Reclasificación Adicional Cargabal    
CREATE TEMPORARY TABLE temp_recla_adicional AS    
select distinct 
    rmc.e0621_feoperac
    ,rmc.e0621_s1emp
    ,rmc.e0621_contra1
    ,rmc.e0621_cta_cont
    ,rmc.e0621_tip_impt
    ,rmc.e0621_fec_mes
    ,rmc.e0621_agrctacb
    ,rmc.e0621_importh
    ,rmc.e0621_coddiv
    ,rmc.e0621_fecultmo
    ,rmc.e0621_centctbl
    ,nvl(   case  when trim(rmc.e0621_ctacgbal) = trim(mrm.cargabal_actual)
                then trim(mrm.cargabal_nuevo)
                else trim(rmc.e0621_ctacgbal)
            end 
          ,trim(rmc.e0621_ctacgbal)
        )  as e0621_ctacgbal
from
    (
        select  rmc.e0621_feoperac
                ,rmc.e0621_s1emp
                ,rmc.e0621_contra1
                ,rmc.e0621_cta_cont
                ,rmc.e0621_tip_impt
                ,rmc.e0621_fec_mes
                ,rmc.e0621_agrctacb
                ,rmc.e0621_importh
                ,rmc.e0621_coddiv
                ,rmc.e0621_fecultmo
                ,rmc.e0621_centctbl
                ,rmc.e0621_ctacgbal
        from temp_recla_morosidad rmc
    ) rmc
left join 
    (
        select norm.id_cto_bdr        
                ,norm.cred_cod_centro
                ,norm.cred_cod_entidad
                ,norm.cred_num_cuenta
                ,norm.cred_cod_producto
                ,norm.cred_cod_subprodu_altair
        from bi_corp_bdr.normalizacion_id_contratos norm
        where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'  
    ) norm on
    trim(rmc.e0621_contra1) = trim(norm.id_cto_bdr)
left join 
    (
        select  rac.sucursal    
                ,rac.entidad  
                ,rac.cuenta   
                ,rac.producto   
                ,rac.subproducto  
                ,rac.cargabal_actual
                ,rac.cargabal_nuevo
        from bi_corp_staging.recla_adicional_cargabal rac
        where rac.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_recla_adicional_cargabal', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
        and periodo = '{{ ti.xcom_pull(task_ids='InputConfig', key='month', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' 
    ) mrm on
        trim(mrm.sucursal) = trim(norm.cred_cod_centro)
        and trim(mrm.entidad) = trim(norm.cred_cod_entidad)
        and trim(mrm.cuenta) = trim(norm.cred_num_cuenta)
        and trim(mrm.producto) = trim(norm.cred_cod_producto)
        and trim(mrm.subproducto) = trim(norm.cred_cod_subprodu_altair);   
    
INSERT overwrite table bi_corp_bdr.jm_posic_contr
partition(partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
select rcla.e0621_feoperac
        ,rcla.e0621_s1emp
        ,rcla.e0621_contra1
        ,rcla.e0621_cta_cont
        ,rcla.e0621_tip_impt
        ,rcla.e0621_fec_mes
        ,rcla.e0621_agrctacb
        ,rcla.e0621_importh
        ,rcla.e0621_coddiv
        ,rcla.e0621_fecultmo
        ,rcla.e0621_centctbl
        ,rpad(nvl(blg.cod_baremo_global,rcla.e0621_ctacgbal), 40, ' ') as e0621_ctacgbal
from temp_recla_adicional rcla 
    left join 
    bi_corp_bdr.map_baremos_global_local blg 
        on blg.cod_negocio = '130' and rcla.e0621_ctacgbal = blg.cod_baremo_local ;
    
--MMFF Importe Posición Contrato
INSERT INTO table bi_corp_bdr.jm_posic_contr
partition(partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
 select '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' E0621_FEOPERAC
        ,lpad(nvl(23100,'0'),5,'0') AS E0621_S1EMP
        ,lpad(nc.id_cto_bdr,9,'0')  AS E0621_CONTRA1
        ,rpad(tac.cuenta, 40, ' ') AS E0621_CTA_CONT
        ,'00000' AS e0621_tip_impt 
        ,'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' AS e0621_fec_mes
        ,rpad(tac.bcra, 40, ' ') AS E0621_AGRCTACB
        ,case when cast(nvl(regexp_replace(trim(tac.suma_posicion),"\\,","."),0) as double)  >= 0
                then concat("-", nvl(lpad(regexp_replace(format_number(cast(nvl(regexp_replace(trim(tac.suma_posicion),"\\,","."),0) as double), 2),"\\,|\\.",""),16,"0"),0))
                else concat("+", nvl(lpad(regexp_replace(format_number(cast(nvl(regexp_replace(trim(tac.suma_posicion),"\\,","."),0) as double), 2),"\\,|\\.|\\-",""),16,"0"),0))
         end as E0621_IMPORTH   
        ,rpad(tac.currency,3,' ') as E0621_CODDIV
        ,'0001-01-01' as e0621_fecultmo 
        ,lpad(' ', 40, ' ') as E0621_CENTCTBL
        ,rpad(tac.cargabal, 40, ' ') as E0621_CTACGBAL
from 
    (
        select tac.cuenta
            ,tac.bcra
            ,tac.suma_posicion
            ,tac.currency
            ,tac.cargabal
            ,tac.isin
        from bi_corp_staging.mmff_tactico_especie tac
        where tac.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_mmff_tactico_especie', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
    ) tac
     inner join 
    (
        select nc.id_cto_source
                ,nc.id_cto_bdr
        from bi_corp_bdr.normalizacion_id_contratos nc
        where nc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
            and nc.source = 'mmff-tactico' 
    ) nc
     on nc.id_cto_source = concat_ws('_', trim(tac.cuenta),trim(tac.isin));   


--Táctico Corresponsales
INSERT INTO table bi_corp_bdr.jm_posic_contr
partition(partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
select  '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' E0621_FEOPERAC
        ,lpad(nvl(23100,'0'),5,'0') AS E0621_S1EMP
        ,lpad(nct.id_cto_bdr,9,'0')  AS E0621_CONTRA1 --Normalización de Contrato
        ,rpad(crp.rubro_altair , 40, ' ') AS E0621_CTA_CONT
        ,'00001' AS e0621_tip_impt 
        ,'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' AS e0621_fec_mes
        ,rpad(crp.rubro_bcra , 40, ' ') AS E0621_AGRCTACB
        ,case when cast(nvl(regexp_replace(trim(alh.importe),"\\,","."),0) as double)  >= 0
                then concat("-", nvl(lpad(regexp_replace(format_number(cast(nvl(regexp_replace(trim(alh.importe),"\\,","."),0) as double), 2),"\\,|\\.",""),16,"0"),0))
                else concat("+", nvl(lpad(regexp_replace(format_number(cast(nvl(regexp_replace(trim(alh.importe),"\\,","."),0) as double), 2),"\\,|\\.|\\-",""),16,"0"),0))
         end as E0621_IMPORTH   
        ,rpad(crp.moneda ,3,' ') as E0621_CODDIV
        ,'0001-01-01' as e0621_fecultmo 
        ,lpad(' ', 40, ' ') as E0621_CENTCTBL
        ,rpad(crp.cargabal , 40, ' ') as E0621_CTACGBAL
from 
    (select crp.nup
            ,crp.moneda
            ,crp.rubro_altair
            ,crp.rubro_bcra
            ,crp.cargabal
       from bi_corp_staging.corresponsales crp
      where crp.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_corresponsales', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
    ) crp
left join 
    (select nct.id_cto_source
            ,nct.id_cto_bdr
        from bi_corp_bdr.normalizacion_id_contratos nct
        where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
        and source = 'corresponsales-tactico' 
    ) nct
    on concat_ws('_', trim(crp.nup), trim(crp.moneda), trim(crp.rubro_altair)) = trim(nct.id_cto_source)
left join 
    (
        select  alh.rubro_altair 
                ,trim(group_map[cast(month(to_date(from_unixtime(UNIX_TIMESTAMP(cast( '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' as varchar(10)),"yyyy-MM-dd")))) as string)]) as importe
        from 
            (    
            select rubro_altair, 
                map('1',enero
                    ,'2',febrero
                    ,'3',marzo
                    ,'4',abril
                    ,'5',mayo
                    ,'6',junio
                    ,'7',julio
                    ,'8',agosto
                    ,'9',septiembre
                    ,'10',octubre
                    ,'11',noviembre
                    ,'12',diciembre) as group_map 
                from bi_corp_staging.alha9600
                where partition_date = '{{ var.json.jm_posic_contr.alha9600 }}'  
            ) alh
    ) alh
    on trim(alh.rubro_altair) =  trim(crp.rubro_altair);
