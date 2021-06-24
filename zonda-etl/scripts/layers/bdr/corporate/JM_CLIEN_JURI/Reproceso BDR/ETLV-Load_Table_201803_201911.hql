set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


--Mayoristas
--Lista cerrada Aqua/Nilo

drop table cli_bii;

CREATE TEMPORARY TABLE cli_bii AS
select cbb.g4093_idnumcli , cbb.g4093_cod_sec2 
  from bi_corp_bdr.jm_client_bii cbb 
 where cbb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'  --'2020-03' 
 and g4093_clisegm not in ('00006');
 
drop table aqua_cli;

CREATE TEMPORARY TABLE aqua_cli (nup string, fecha_ejercicio string,  facturacion string, total_activo string); 

with aqua_clientes as 
(
    select  lpad(mkg.nup,9,"0") as nup
            ,to_date(from_unixtime(UNIX_TIMESTAMP(concat(acsg.fecha_informacion,"1231"),"yyyyMMdd"))) as fecha_ejercicio
            ,lpad(regexp_replace(format_number(cast(regexp_replace(nvl(( 
                case    when trim(acsg.divisa) = "ARS" and acsg.unidad_monetaria = "MM" then (acsg.facturacion * "1000000")  
                        when trim(acsg.divisa) = "ARS" and acsg.unidad_monetaria = "M" then (acsg.facturacion * "1000")
                        when trim(acsg.divisa) <> "ARS" and acsg.unidad_monetaria = "MM" then (acsg.facturacion * "1000000") * tcm.tipo_de_cambio
                        when trim(acsg.divisa) <> "ARS" and acsg.unidad_monetaria = "M" then (acsg.facturacion * "1000") * tcm.tipo_de_cambio
                else acsg.facturacion
                end
                ),0) ,"\\,",".") as double), 2),"\\,|\\.",""),16,"0") as facturacion
            ,lpad(regexp_replace(format_number(cast(regexp_replace(nvl(( 
                case    when trim(acsg.divisa) = "ARS" and acsg.unidad_monetaria = "MM" then (acsg.total_activos * "1000000")  
                        when trim(acsg.divisa) = "ARS" and acsg.unidad_monetaria = "M" then (acsg.total_activos * "1000")
                        when trim(acsg.divisa) <> "ARS" and acsg.unidad_monetaria = "MM" then (acsg.total_activos * "1000000") * tcm.tipo_de_cambio
                        when trim(acsg.divisa) <> "ARS" and acsg.unidad_monetaria = "M" then (acsg.total_activos * "1000") * tcm.tipo_de_cambio
                else acsg.total_activos
                end
                ),0) ,"\\,",".") as double), 2),"\\,|\\.",""),16,"0") as total_activo
    from    (
                select acsg.unidad_operativa
                        ,acsg.fecha_informacion
                        ,acsg.unidad_monetaria
                        ,acsg.divisa
                        ,acsg.facturacion
                        ,acsg.total_activos
                    from bi_corp_staging.aqua_clientes_asoc_geconomicos acsg  
                    where acsg.partition_date =  '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'  
            ) acsg
        inner join 
            ( 
                    select mkg.nup
                        ,mkg.shortname
                    from bi_corp_bdr.perim_mdr_contraparte mkg
                    where  mkg.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'               
            ) mkg
                on trim(acsg.unidad_operativa) = trim(mkg.shortname)
        left OUTER JOIN 
            (
                select substr(tcm.par_de_divisas,1,3) as par_de_divisas 
                        ,tcm.tipo_de_cambio
                from bi_corp_staging.aqua_tipo_cambio tcm  
                where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_aqua_tipo_cambio', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
                        and substr(tcm.par_de_divisas,5,3) = 'ARS' 
            ) tcm 
                on trim(acsg.divisa) = trim(tcm.par_de_divisas)
)            
INSERT OVERWRITE TABLE aqua_cli            
select acli.*
from 
    (
        select *
        from aqua_clientes acli 
        where acli.fecha_ejercicio >= add_months(current_date,-24)
    ) acli
inner join 
    cli_bii cbb 
        on trim(acli.nup) = trim(cbb.g4093_idnumcli);
        
drop table cli_bii_2;

CREATE TEMPORARY TABLE cli_bii_2 AS
SELECT cbb.*
FROM cli_bii cbb 
left join 
    aqua_cli acl 
        on cbb.g4093_idnumcli = acl.nup
where acl.nup is null;


--Clientes No MRG

drop table no_mrg;

CREATE TEMPORARY TABLE no_mrg (nup string, fecha_ejercicio string,  facturacion string, total_activo string); 

with no_mrg_1 as (
select lpad(naj.identificador_cliente ,9,"0") as nup 
        ,case when trim(naj.fecha_informacion) = '-' 
                then '0001-01-01' 
                else trim(naj.fecha_informacion) 
         end as fecha_ejercicio
        ,nvl(lpad(regexp_replace(format_number(cast(regexp_replace(regexp_replace(nvl(trim(naj.facturacion),0),"\\-","0"),"\\.","")  as double), 2),"\\,|\\.",""),16,"0"),0) as facturacion
        ,nvl(lpad(regexp_replace(format_number(cast(regexp_replace(regexp_replace(nvl(trim(naj.total_activos_cliente),0),"\\-","0"),"\\.","")  as double), 2),"\\,|\\.",""),16,"0"),0) as total_activo 
from bi_corp_staging.no_mrg_juridica naj  
where naj.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_no_mrg_juridica', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' 
)
INSERT OVERWRITE TABLE no_mrg  
select nrg.*
from (select *
        from no_mrg_1 nrg
        where nrg.fecha_ejercicio >= add_months(current_date,-24)
       ) nrg
       inner join cli_bii_2 cbb 
   on trim(nrg.nup) = trim(cbb.g4093_idnumcli);
   
drop table cli_bii_3;

CREATE TEMPORARY TABLE cli_bii_3 AS
SELECT cbb.*
FROM cli_bii_2 cbb left join no_mrg acl on cbb.g4093_idnumcli = acl.nup
where acl.nup is null;   

    
--Empresas y grandes empresas
--SGE
drop table sge_grupos;

CREATE TEMPORARY TABLE sge_grupos (nup string, fecha_ejercicio string,  facturacion string, total_activo string); 

with sge_grupos_1 as (
    SELECT lpad(sge.nup,9,"0") as nup 
                 ,to_date(from_unixtime(UNIX_TIMESTAMP(sge.fecha_ejercicio,"dd/MM/yyyy"))) as fecha_ejercicio
                 ,nvl(lpad(regexp_replace(format_number(cast(regexp_replace(nvl(sge.facturacion,0) ,"\\,",".") as double), 2),"\\,|\\.",""),16,"0"),0) as facturacion
                 ,nvl(lpad(regexp_replace(format_number(cast(regexp_replace(nvl(sge.total_activo,0) ,"\\,",".") as double), 2),"\\,|\\.",""),16,"0"),0) as total_activo
            FROM bi_corp_staging.sge_grupos_economicos sge 
            where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_sge_grupos_economicos', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'   --'2020-04-14' 
)  
INSERT OVERWRITE TABLE sge_grupos  
select sge.*
from (select *
        from sge_grupos_1 sge 
        where sge.fecha_ejercicio >= add_months(current_date,-24)
        ) sge
    inner join cli_bii_3 cbb 
        on trim(sge.nup) = trim(cbb.g4093_idnumcli);

drop table cli_bii_4;

CREATE TEMPORARY TABLE cli_bii_4 AS
SELECT cbb.*
FROM cli_bii_3 cbb left join sge_grupos acl on cbb.g4093_idnumcli = acl.nup
where acl.nup is null   ;
    
    
--Pyme 2, Pymes 1 y PF Pyme
--Altair Personas

drop table altair_personas;

CREATE TEMPORARY TABLE altair_personas (nup string, fecha_ejercicio string,  facturacion string, total_activo string); 


with altair_p as (
select lpad(pcf.num_persona,9,"0") as nup
       ,pcf.fec_fecrfa as fecha_ejercicio
       ,nvl(lpad(regexp_replace(format_number(cast(regexp_replace(nvl(pcf.imp_facturacion_anual,0) ,"\\,",".") as double), 2),"\\,|\\.",""),16,"0"),0) as facturacion
       ,nvl(lpad(0,16,"0"),0) as total_activo  
from 
    (
        select *
        from santander_business_risk_arda.PERSONAS_COMP_FISICAS
        where data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_personas_comp_fisicas', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'   --'20200630'
        and fec_fecrfa >= add_months(current_date,-24) 
    ) pcf
)
INSERT OVERWRITE TABLE altair_personas
select alp.*
from altair_p alp
inner join cli_bii_4 cbb 
    on trim(alp.nup) = trim(cbb.g4093_idnumcli) ;        

drop table cli_bii_5;

CREATE TEMPORARY TABLE cli_bii_5 AS
SELECT cbb.*
FROM cli_bii_4 cbb left join altair_personas acl on cbb.g4093_idnumcli = acl.nup
where acl.nup is null   ;


--Listado AFIP

drop table afip;

CREATE TEMPORARY TABLE afip (nup string, fecha_ejercicio string,  facturacion string, total_activo string); 


with afip_1 as (

select lpad(p15.penumper,9,"0") as nup
        ,af.partition_date as fecha_ejercicio -- fecha partition 
       ,nvl(lpad(regexp_replace(format_number(cast(regexp_replace(nvl(trim(cmt.ingresos_brutos),0),"\\,|\\.","") as double), 2),"\\,|\\.",""),16,"0"),0) as facturacion
       ,nvl(lpad(regexp_replace(format_number(cast(regexp_replace(nvl(trim(cmt.ingresos_brutos),0),"\\,|\\.","") as double), 2),"\\,|\\.",""),16,"0"),0)  as total_activo        
from 
 (select *
 from bi_corp_staging.afip af 
 where partition_date  = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' 
 and monotributo not in ('NI','61', 'BC', 'BL', 'BT','BP', 'BV')
 ) af
  left join 
  (
  
  select categoria
        ,trim(cast(regexp_replace(ingresos_brutos,"\\$","") as string) )as  ingresos_brutos
  from bi_corp_staging.afip_cat_monotributo cmt
  where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'  --'2020-08-01'
  
  ) cmt on trim(af.monotributo) = trim(cmt.categoria)
  left join 
  (
    select *
  from bi_corp_staging.malpe_pedt015
  where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt015', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'  --'2020-06-30'
  ) p15 on trim(af.cuit) = trim(p15.penumdoc)
  left join 
  (
    select *
    from santander_business_risk_arda.PERSONAS_COMP_FISICAS
    where data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_personas_comp_fisicas', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'  --'20200630'
    and fec_fecrfa >= add_months(current_date,-24) 
    ) pcf on trim(pcf.num_persona) = trim(p15.penumper)   
  ) 
INSERT OVERWRITE TABLE afip  
select afi.*
from afip_1 afi 
    inner join cli_bii_5 cbb 
        on trim(afi.nup) = trim(cbb.g4093_idnumcli); 
  
drop table cli_bii_6;
        
CREATE TEMPORARY TABLE cli_bii_6 AS
SELECT cbb.*
FROM cli_bii_5 cbb left join afip acl on cbb.g4093_idnumcli = acl.nup
where acl.nup is null   ;
        
--MiPyme BCRA
--Falta segmentación

drop table bcra;

CREATE TEMPORARY TABLE bcra (nup string, fecha_ejercicio string,  facturacion string, total_activo string); 

with bcra_1 as (
 select lpad(p15.penumper,9,"0") as nup
         ,bcr.partition_date as fecha_ejercicio
 from 
     (select *
     from bi_corp_staging.bcra 
     where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_bcra', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' --'2020-06-01'
     ) bcr
   left join 
      (
        select *
      from bi_corp_staging.malpe_pedt015
      where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt015', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' --'2020-06-30'
      ) p15 on trim(bcr.salida_072 ) = trim(p15.penumdoc)
   left join 
      (
        select *
        from santander_business_risk_arda.PERSONAS_COMP_FISICAS
        where data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_personas_comp_fisicas', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'  --'20200630'
        and fec_fecrfa >= add_months(current_date,-24) 
        ) pcf on trim(pcf.num_persona) = trim(p15.penumper)   
 
 )
INSERT OVERWRITE TABLE bcra 
select bcr.nup
        , bcr.fecha_ejercicio
        , nvl(lpad(regexp_replace(format_number(cast(regexp_replace(nvl(trim(bcsd.limite),0),"\\,|\\.","") as double), 2),"\\,|\\.",""),16,"0"),0) as facturacion
        , nvl(lpad(regexp_replace(format_number(cast(regexp_replace(nvl(trim(bcsd.limite),0),"\\,|\\.","") as double), 2),"\\,|\\.",""),16,"0"),0) as total_activo
from bcra_1 bcr
    inner join cli_bii_6 cbb
        on trim(bcr.nup) = trim(cbb.g4093_idnumcli)
    left join 
        (select *
            from 
            (select *
            from bi_corp_bdr.baremos_local bl9 
                where bl9.cod_negocio_local = '9' ) bl9
            left join 
                    (select * from bi_corp_staging.bcra_sector where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_bcra_sector', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' ) bcs  --'2020-06-01'
                on lpad(trim(bl9.cod_baremo_alfanumerico_local),8,"0") = trim(bcs.cod_actividad_f883)
         ) bcs 
        on lpad(nvl(trim(bcs.cod_baremo_local), 0), 5, '0') = trim(cbb.g4093_cod_sec2)        
    left join 
        (select * from bi_corp_staging.bcra_sector_desc where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig',key='max_partition_bcra_sector_desc', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'  ) bcsd -- '2020-06-01'
        on trim(bcsd.sector) = trim(bcs.sector);
    

        
drop table cli_bii_7;
    
CREATE TEMPORARY TABLE cli_bii_7 AS
SELECT cbb.*
FROM cli_bii_6 cbb left join bcra acl on cbb.g4093_idnumcli = acl.nup
where acl.nup is null   ;    


--Archivo inferencia de ingresos

drop table infe_ingreso;

CREATE TEMPORARY TABLE infe_ingreso (nup string, fecha_ejercicio string,  facturacion string, total_activo string); 

with infe_ing as
(
select lpad(idi.nup,9,"0") as nup
       ,idi.partition_date as fecha_ejercicio
       ,nvl(lpad(regexp_replace(format_number(cast(regexp_replace(nvl(idi.ingreso,0) ,"\\,",".") as double), 2),"\\,|\\.",""),16,"0"),0) as facturacion
       ,nvl(lpad(regexp_replace(format_number(cast(regexp_replace(nvl(idi.ingreso,0) ,"\\,",".") as double), 2),"\\,|\\.",""),16,"0"),0) as total_activo  
from 
    (select *
    from bi_corp_staging.inferencia_ingreso
    where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'  ) idi --'2020-06-01' 
 left join 
  (
    select *
    from santander_business_risk_arda.PERSONAS_COMP_FISICAS
    where data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_personas_comp_fisicas', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'  --'20200630'
    and fec_fecrfa >= add_months(current_date,-24) 
    ) pcf on trim(pcf.num_persona) = trim(idi.nup)
)
INSERT OVERWRITE TABLE infe_ingreso 
select inf.*
from infe_ing inf 
    inner join cli_bii_7 cbb
        on trim(inf.nup) = trim(cbb.g4093_idnumcli);

insert overwrite table bi_corp_bdr.jm_clien_juri 
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
SELECT DISTINCT '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' as G5508_FEOPERAC
        ,'23100' as G5508_S1EMP
        ,lpad(nvl(ajr.nup,'0'),9,"0") as G5508_IDNUMCLI
        , nvl(ajr.fecha_ejercicio,'0001-01-01') as G5508_INFFECHA 
        ,case when nvl(ajr.facturacion,0) >= 0 
                then concat('+', lpad(nvl(ajr.facturacion,0),16,"0")) 
                else concat('-', lpad(regexp_replace(nvl(ajr.facturacion,0),"\\-",""),16,"0")) 
         end as G5508_IMPFACTM 
        ,case when nvl(ajr.total_activo,0) >= 0 
                then concat('+', lpad(nvl(ajr.total_activo,0),16,"0")) 
                else concat('-', lpad(regexp_replace(nvl(ajr.total_activo,0),"\\-",""),16,"0")) 
         end as G5508_TOT_ACTI 
        ,lpad("",9,"0") as G5508_NUM_EMPL
        ,lpad("",5,"0") as G5508_ORIG_FAC 
        ,lpad("",5,"0") as G5508_ORIG_ACT 
        ,COALESCE(lpad(crp.cargabal,5,0),"99999") as G5508_ORIG_EMP 
        ,'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'  as G5508_FECULTMO
        ,lpad("",17,"0") as G5508_TDEUDACL
        ,lpad("",9,"0") as G5508_RAT_CET1
        ,lpad("",9,"0") as G5508_TASAMORA
        ,lpad("",17,"0") as G5508_TOT_EQTY
        ,lpad("",9,"0") as G5508_ORGDEPEN
        ,"N" as G5508_FLGEMPNO
from        
    (select * from aqua_cli
    union all
    select * from no_mrg
    union all
    select * from sge_grupos
    union all
    select * from altair_personas
    union all
    select * from afip
    union all
    select * from bcra
    union all
    select * from infe_ingreso
    ) ajr
left join 
(
    select DISTINCT trim(nup) as nup,
            trim(cargabal) as cargabal
    from bi_corp_staging.corresponsales_adicional_juridica crp       
    where crp.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_corresponsales_adicional_juridica', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
) crp
on lpad(nvl(ajr.nup,'0'),9,"0") =  lpad(nvl(crp.nup,'0'),9,"0");

--Corresponsales
insert into table bi_corp_bdr.jm_clien_juri 
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
SELECT DISTINCT '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' as G5508_FEOPERAC
        ,'23100' as G5508_S1EMP
        ,lpad(nvl(crp.nup,'0'),9,"0") as G5508_IDNUMCLI
        ,'0001-01-01' as G5508_INFFECHA 
        ,concat('+', lpad("0",16,"0")) as G5508_IMPFACTM 
        ,concat('+', lpad("0",16,"0")) as G5508_TOT_ACTI 
        ,lpad("",9,"0") as G5508_NUM_EMPL
        ,lpad("",5,"0") as G5508_ORIG_FAC 
        ,lpad("",5,"0") as G5508_ORIG_ACT 
        ,COALESCE(lpad(crp.cargabal,5,0),"99999") as G5508_ORIG_EMP 
        ,'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' as G5508_FECULTMO
        ,lpad("",17,"0") as G5508_TDEUDACL
        ,lpad("",9,"0") as G5508_RAT_CET1
        ,lpad("",9,"0") as G5508_TASAMORA
        ,lpad("",17,"0") as G5508_TOT_EQTY
        ,lpad("",9,"0") as G5508_ORGDEPEN
        ,"N" as G5508_FLGEMPNO
from         
(
    select DISTINCT trim(nup) as nup,
            trim(cargabal) as cargabal
    from bi_corp_staging.corresponsales_adicional_juridica crp       
    where crp.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_corresponsales_adicional_juridica', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
) crp
left join 
(

    select distinct ajr.G5508_IDNUMCLI
    from bi_corp_bdr.jm_clien_juri ajr
    where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'

) ajr 
on ajr.G5508_IDNUMCLI = lpad(nvl(crp.nup,'0'),9,"0")
where ajr.G5508_IDNUMCLI is null


