set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


--Fogar Nómina
INSERT INTO TABLE bi_corp_bdr.jm_trz_cont_ren 
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
SELECT
    '23100' as G7025_S1EMP
    ,lpad(cbi.g4095_contra1 , 9, '0') as G7025_CONTRA1 --Normalización de Contratos
    ,'23100' as G7025_EMP_ANT
    ,lpad(cbi.g4095_contra1, 9, '0') as G7025_CONT_ANT --Normalización de Contratos
    ,lpad('60', 5, '0')  as G7025_MOTRENU -- Baremos Local
    ,cbi.g4095_fechaper as G7025_FEALTREL 
    ,'0001-01-01' as G7025_FEC_MOD 
    ,case when cast(ipc.E0621_IMPORTH as double)  >= 0
            then concat("-", lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(ipc.E0621_IMPORTH as double), 0), 2),"\\,|\\.",""),"\\,|\\.","") ,16,"0") )
            else concat("+", lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(ipc.E0621_IMPORTH as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-",""),16,"0") ) 
    end as G7025_IMPRESTR
    ,cbi.g4095_coddiv as G7025_CODDIV
    ,lpad('30', 5, '0')  as G7025_MOTRENUG -- Baremos Global
    ,coalesce(cbi.pefecest,g4095_fecvento,'9999-12-31') as G7025_FEC_BAJA
from     
(
    select *
    from bi_corp_bdr.perim_contratos_bis_covid
    where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
    and desc_tipo = 'Fogar'    
) cbi 
    left join 
(
    select ipc.e0621_contra1
           , sum(cast(ipc.E0621_IMPORTH as double) * -1 / 100) as E0621_IMPORTH
           , ipc.partition_date
    from bi_corp_bdr.jm_posic_contr ipc
    where ipc.partition_date >= '2020-01'
    group by ipc.e0621_contra1, ipc.partition_date
) ipc
    on cbi.g4095_contra1 = ipc.e0621_contra1
    and cbi.contr_partition_date = ipc.partition_date;

    
--MiPyme Plus
INSERT INTO TABLE bi_corp_bdr.jm_trz_cont_ren 
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
SELECT
    '23100' as G7025_S1EMP
    ,lpad(cbi.g4095_contra1 , 9, '0') as G7025_CONTRA1 --Normalización de Contratos
    ,'23100' as G7025_EMP_ANT
    ,lpad(cbi.g4095_contra1, 9, '0') as G7025_CONT_ANT --Normalización de Contratos
    ,lpad('61', 5, '0')  as G7025_MOTRENU -- Baremos Local
    ,cbi.g4095_fechaper as G7025_FEALTREL 
    ,'0001-01-01' as G7025_FEC_MOD 
    ,case when cast(ipc.E0621_IMPORTH as double)  >= 0
            then concat("-", lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(ipc.E0621_IMPORTH as double), 0), 2),"\\,|\\.",""),"\\,|\\.","") ,16,"0") )
            else concat("+", lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(ipc.E0621_IMPORTH as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-",""),16,"0") ) 
    end as G7025_IMPRESTR
    ,cbi.g4095_coddiv as G7025_CODDIV
    ,lpad('30', 5, '0') as G7025_MOTRENUG -- Baremos Global
    ,coalesce(cbi.pefecest,g4095_fecvento,'9999-12-31') as G7025_FEC_BAJA
from 
(
    select *
    from bi_corp_bdr.perim_contratos_bis_covid
    where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
    and desc_tipo = 'MiPyme' 
) cbi 
    left join 
(
    select ipc.e0621_contra1
           , sum(cast(ipc.E0621_IMPORTH as double) * -1 / 100) as E0621_IMPORTH
           , ipc.partition_date
    from bi_corp_bdr.jm_posic_contr ipc
    where ipc.partition_date >= '2020-01'
    group by ipc.e0621_contra1, ipc.partition_date
) ipc
    on cbi.g4095_contra1 = ipc.e0621_contra1
    and cbi.contr_partition_date = ipc.partition_date;


--ATP

INSERT INTO TABLE bi_corp_bdr.jm_trz_cont_ren 
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
SELECT
    '23100' as G7025_S1EMP
    ,lpad(cbi.g4095_contra1 , 9, '0') as G7025_CONTRA1 --Normalización de Contratos
    ,'23100' as G7025_EMP_ANT
    ,lpad(cbi.g4095_contra1, 9, '0') as G7025_CONT_ANT --Normalización de Contratos
    , lpad('62', 5, '0') as G7025_MOTRENU -- Baremos Local
    ,cbi.g4095_fechaper as G7025_FEALTREL 
    ,'0001-01-01' as G7025_FEC_MOD 
    ,case when cast(ipc.E0621_IMPORTH as double)  >= 0
            then concat("-", lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(ipc.E0621_IMPORTH as double), 0), 2),"\\,|\\.",""),"\\,|\\.","") ,16,"0") )
            else concat("+", lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(ipc.E0621_IMPORTH as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-",""),16,"0") ) 
    end as G7025_IMPRESTR
    ,cbi.g4095_coddiv as G7025_CODDIV
    ,lpad('30', 5, '0') as G7025_MOTRENUG -- Baremos Global
    ,coalesce(cbi.pefecest,g4095_fecvento,'9999-12-31') as G7025_FEC_BAJA
from 
(
    select *
    from bi_corp_bdr.perim_contratos_bis_covid
    where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
    and desc_tipo = 'ATP'  
) cbi 
left join 
(
    select ipc.e0621_contra1
           , sum(cast(ipc.E0621_IMPORTH as double) * -1 / 100) as E0621_IMPORTH
           , ipc.partition_date
    from bi_corp_bdr.jm_posic_contr ipc
    where ipc.partition_date >= '2020-01'
    group by ipc.e0621_contra1, ipc.partition_date
) ipc
    on cbi.g4095_contra1 = ipc.e0621_contra1
    and cbi.contr_partition_date = ipc.partition_date;

--Préstamo Flexible

INSERT INTO TABLE bi_corp_bdr.jm_trz_cont_ren 
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
SELECT
    '23100' as G7025_S1EMP
    ,lpad(cbi.g4095_contra1 , 9, '0') as G7025_CONTRA1 --Normalización de Contratos
    ,'23100' as G7025_EMP_ANT
    ,lpad(cbi.g4095_contra1, 9, '0') as G7025_CONT_ANT --Normalización de Contratos
    ,lpad('63', 5, '0')  as G7025_MOTRENU -- Baremos Local
    ,cbi.g4095_fechaper as G7025_FEALTREL 
    ,'0001-01-01' as G7025_FEC_MOD 
    ,case when cast(ipc.E0621_IMPORTH as double)  >= 0
            then concat("-", lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(ipc.E0621_IMPORTH as double), 0), 2),"\\,|\\.",""),"\\,|\\.","") ,16,"0") )
            else concat("+", lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(ipc.E0621_IMPORTH as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-",""),16,"0") ) 
    end as G7025_IMPRESTR
    ,cbi.g4095_coddiv as G7025_CODDIV
    ,lpad('29', 5, '0') as G7025_MOTRENUG -- Baremos Global
    ,coalesce(cbi.pefecest,g4095_fecvento,'9999-12-31') as G7025_FEC_BAJA
from 
(    
    select *
    from bi_corp_bdr.perim_contratos_bis_covid
    where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
    and desc_tipo = 'PresFlex' 
) cbi 
left join 
(
    select ipc.e0621_contra1
           , sum(cast(ipc.E0621_IMPORTH as double) * -1 / 100) as E0621_IMPORTH
           , ipc.partition_date
    from bi_corp_bdr.jm_posic_contr ipc
    where ipc.partition_date >= '2020-01'
    group by ipc.e0621_contra1, ipc.partition_date
) ipc
    on cbi.g4095_contra1 = ipc.e0621_contra1
    and cbi.contr_partition_date = ipc.partition_date;
      
--Skip Cuotas
INSERT INTO TABLE bi_corp_bdr.jm_trz_cont_ren 
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
SELECT
    '23100' as G7025_S1EMP
    ,lpad(ncr.id_cto_bdr , 9, '0') as G7025_CONTRA1 --Normalización de Contratos
    ,'23100' as G7025_EMP_ANT
    ,lpad(ncr.id_cto_bdr, 9, '0') as G7025_CONT_ANT --Normalización de Contratos
    ,lpad('64', 5, '0') as G7025_MOTRENU -- Baremos Local
    ,cbi.g4095_fechaper  as G7025_FEALTREL 
    ,'0001-01-01' as G7025_FEC_MOD 
    ,case when cast(mlu.impconce  as double)  >= 0
            then concat("-", lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(mlu.impconce as double), 0), 2),"\\,|\\.",""),"\\,|\\.","") ,16,"0") )
            else concat("+", lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(mlu.impconce as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-",""),16,"0") ) 
    end as G7025_IMPRESTR
    ,cbi.g4095_coddiv as G7025_CODDIV
    ,lpad('26', 5, '0') as G7025_MOTRENUG -- Baremos Global
    ,coalesce(mlu.fecha_pago,cbi.g4095_fecvento,'9999-12-31') as G7025_FEC_BAJA
from 
(
    select *
    from bi_corp_bdr.perim_contratos_covid
    where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
    and tipo = 'COVID'
) mlu
inner join  
(
    select ncr.id_cto_bdr
        , ncr.id_cto_source
        ,ncr.cred_cod_entidad
        ,ncr.cred_cod_centro
        ,ncr.cred_num_cuenta
        ,ncr.cred_cod_producto
        ,ncr.cred_cod_subprodu_altair
    from bi_corp_bdr.normalizacion_id_contratos ncr
    where ncr.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
) ncr
on concat_ws("_", mlu.entidad, mlu.oficina, mlu.cuenta, mlu.producto, mlu.subpro) = ncr.id_cto_source
inner join 
(
    select cbi.g4095_contra1
            ,cbi.g4095_fechaper
            ,cbi.g4095_coddiv
            ,cbi.g4095_idpro_lc
            ,cbi.g4095_fecvento
    from bi_corp_bdr.jm_contr_bis cbi
    where cbi.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
) cbi 
on cbi.g4095_contra1 = ncr.id_cto_bdr;

--Tasa 0   
INSERT INTO TABLE bi_corp_bdr.jm_trz_cont_ren 
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
select  
    '23100' as G7025_S1EMP
    ,lpad(ncr.g4095_contra1 , 9, '0') as G7025_CONTRA1 --Normalización de Contratos
    ,'23100' as G7025_EMP_ANT
    ,lpad(ncr.g4095_contra1, 9, '0') as G7025_CONT_ANT --Normalización de Contratos
    ,lpad('65', 5, '0')  as G7025_MOTRENU -- Baremos Local
    ,cov.fecha_alta_producto  as G7025_FEALTREL 
    ,'0001-01-01' as G7025_FEC_MOD 
    ,case when cov.saldo_original  >= 0
            then concat("-", lpad(regexp_replace(regexp_replace(format_number(coalesce (cov.saldo_original, 0), 2),"\\,|\\.",""),"\\,|\\.","") ,16,"0") )
            else concat("+", lpad(regexp_replace(regexp_replace(format_number(coalesce (cov.saldo_original, 0), 2),"\\,|\\.",""),"\\,|\\.|\\-",""),16,"0") ) 
    end as G7025_IMPRESTR
    ,ncr.g4095_coddiv as G7025_CODDIV
    ,lpad('30', 5, '0') as G7025_MOTRENUG -- Baremos Global
    ,coalesce(cov.fecha_vencimiento_producto,'9999-12-31') as G7025_FEC_BAJA
from 
(
    select cov.sucursal
            ,cov.nro_cuenta
            ,cov.codigo_producto
            ,cov.codigo_subproducto
            ,sum(cov.saldo_original) as saldo_original
            ,sum(cov.saldo_hoy)  as saldo_hoy
            ,cov.tipo
            ,cov.moneda
            ,fecha_alta_producto
            ,fecha_vencimiento_producto
    from 
        (
            select distinct cov.sucursal
                            ,cov.nro_cuenta
                            ,cov.codigo_producto
                            ,cov.codigo_subproducto
                            ,cast(cov.saldo_original as double) as saldo_original
                            ,cast(cov.saldo_hoy as double) as saldo_hoy
                            ,cov.tipo
                            ,cov.moneda
                            ,SUBSTRING(cov.fecha_alta_producto,1,10) as fecha_alta_producto
                            ,SUBSTRING(cov.fecha_vencimiento_producto,1,10) as fecha_vencimiento_producto
            from bi_corp_bdr.saldos_tarjetas_covid cov
            where cov.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_saldos_tarjetas_covid', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
                and tipo = 'TASA 0'
            
        ) cov
        group by cov.sucursal
            ,cov.nro_cuenta
            ,cov.codigo_producto
            ,cov.codigo_subproducto
            ,cov.tipo
            ,cov.moneda
            ,fecha_alta_producto
            ,fecha_vencimiento_producto
) cov
inner join 
(
	select cbi.cred_cod_centro,
			cbi.cred_num_cuenta,
			cbi.cred_cod_producto,
			cbi.cred_cod_subprodu_altair,
			cbi.g4095_contra1 ,
			cbi.g4095_coddiv ,
			min(cbi.contr_partition_date) as contr_partition_date
	from bi_corp_bdr.perim_contratos_bis_tarjetas_covid cbi
	where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
	group by cbi.cred_cod_centro,
			cbi.cred_num_cuenta,
			cbi.cred_cod_producto,
			cbi.cred_cod_subprodu_altair,
			cbi.g4095_contra1 ,
			cbi.g4095_coddiv 
) ncr
on 
 lpad(cast(trim(cov.sucursal) as string) ,4,'0') = ncr.cred_cod_centro
and lpad(cast(trim(cov.nro_cuenta) as string),12,'0') = ncr.cred_num_cuenta 
and lpad(cast(trim(cov.codigo_producto) as string),2,'0') = ncr.cred_cod_producto
and cast(trim(cov.codigo_subproducto)  as string) = ncr.cred_cod_subprodu_altair
and trim(cov.moneda) = trim(ncr.g4095_coddiv);


--EARLYCARDS  
INSERT INTO TABLE bi_corp_bdr.jm_trz_cont_ren 
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
select  
    '23100' as G7025_S1EMP
    ,lpad(ncr.g4095_contra1 , 9, '0') as G7025_CONTRA1 --Normalización de Contratos
    ,'23100' as G7025_EMP_ANT
    ,lpad(ncr.g4095_contra1, 9, '0') as G7025_CONT_ANT --Normalización de Contratos
    ,lpad('66', 5, '0') as G7025_MOTRENU -- Baremos Local
    ,cov.fecha_alta_producto  as G7025_FEALTREL 
    ,'0001-01-01' as G7025_FEC_MOD 
    ,case when cov.saldo_original  >= 0
            then concat("-", lpad(regexp_replace(regexp_replace(format_number(coalesce (cov.saldo_original, 0), 2),"\\,|\\.",""),"\\,|\\.","") ,16,"0") )
            else concat("+", lpad(regexp_replace(regexp_replace(format_number(coalesce (cov.saldo_original, 0), 2),"\\,|\\.",""),"\\,|\\.|\\-",""),16,"0") ) 
    end as G7025_IMPRESTR
    ,ncr.g4095_coddiv as G7025_CODDIV
    ,lpad('29', 5, '0') as G7025_MOTRENUG -- Baremos Global
    ,coalesce(cov.fecha_vencimiento_producto,'9999-12-31') as G7025_FEC_BAJA
from 
(         
    select cov.sucursal
            ,cov.nro_cuenta
            ,cov.codigo_producto
            ,cov.codigo_subproducto
            ,sum(cov.saldo_original) as saldo_original
            ,sum(cov.saldo_hoy)  as saldo_hoy
            ,cov.tipo
            ,cov.moneda
            ,fecha_alta_producto
            ,fecha_vencimiento_producto
    from 
        (
            select distinct cov.sucursal
                            ,cov.nro_cuenta
                            ,cov.codigo_producto
                            ,cov.codigo_subproducto
                            ,cast(cov.saldo_original as double) as saldo_original
                            ,cast(cov.saldo_hoy as double) as saldo_hoy
                            ,cov.tipo
                            ,cov.moneda
                            ,SUBSTRING(cov.fecha_alta_producto,1,10) as fecha_alta_producto
                            ,SUBSTRING(cov.fecha_vencimiento_producto,1,10) as fecha_vencimiento_producto
            from bi_corp_bdr.saldos_tarjetas_covid cov
            where cov.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_saldos_tarjetas_covid', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
                and tipo = 'EARLYCARDS'
            
        ) cov
        group by cov.sucursal
            ,cov.nro_cuenta
            ,cov.codigo_producto
            ,cov.codigo_subproducto
            ,cov.tipo
            ,cov.moneda
            ,fecha_alta_producto
            ,fecha_vencimiento_producto
) cov
inner join 
(
	select cbi.cred_cod_centro,
			cbi.cred_num_cuenta,
			cbi.cred_cod_producto,
			cbi.cred_cod_subprodu_altair,
			cbi.g4095_contra1 ,
			cbi.g4095_coddiv ,
			min(cbi.contr_partition_date) as contr_partition_date
	from bi_corp_bdr.perim_contratos_bis_tarjetas_covid cbi
	where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
	group by cbi.cred_cod_centro,
			cbi.cred_num_cuenta,
			cbi.cred_cod_producto,
			cbi.cred_cod_subprodu_altair,
			cbi.g4095_contra1 ,
			cbi.g4095_coddiv 
) ncr
on 
 lpad(cast(trim(cov.sucursal) as string) ,4,'0') = ncr.cred_cod_centro
and lpad(cast(trim(cov.nro_cuenta) as string),12,'0') = ncr.cred_num_cuenta 
and lpad(cast(trim(cov.codigo_producto) as string),2,'0') = ncr.cred_cod_producto
and cast(trim(cov.codigo_subproducto)  as string) = ncr.cred_cod_subprodu_altair
and trim(cov.moneda) = trim(ncr.g4095_coddiv);

--PLAN V
INSERT INTO TABLE bi_corp_bdr.jm_trz_cont_ren 
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
select  
    '23100' as G7025_S1EMP
    ,lpad(ncr.g4095_contra1 , 9, '0') as G7025_CONTRA1 --Normalización de Contratos
    ,'23100' as G7025_EMP_ANT
    ,lpad(ncr.g4095_contra1, 9, '0') as G7025_CONT_ANT --Normalización de Contratos
    ,lpad('67', 5, '0') as G7025_MOTRENU -- Baremos Local
    ,cov.fecha_alta_producto  as G7025_FEALTREL 
    ,'0001-01-01' as G7025_FEC_MOD 
    ,case when cov.saldo_original  >= 0
            then concat("-", lpad(regexp_replace(regexp_replace(format_number(coalesce (cov.saldo_original, 0), 2),"\\,|\\.",""),"\\,|\\.","") ,16,"0") )
            else concat("+", lpad(regexp_replace(regexp_replace(format_number(coalesce (cov.saldo_original, 0), 2),"\\,|\\.",""),"\\,|\\.|\\-",""),16,"0") ) 
    end as G7025_IMPRESTR
    ,ncr.g4095_coddiv as G7025_CODDIV
    ,lpad('26', 5, '0') as G7025_MOTRENUG -- Baremos Global
    ,coalesce(cov.fecha_vencimiento_producto,'9999-12-31') as G7025_FEC_BAJA
from 
(
    select cov.sucursal
            ,cov.nro_cuenta
            ,cov.codigo_producto
            ,cov.codigo_subproducto
            ,sum(cov.saldo_original) as saldo_original
            ,sum(cov.saldo_hoy)  as saldo_hoy
            ,cov.tipo
            ,cov.moneda
            ,fecha_alta_producto
            ,fecha_vencimiento_producto
    from 
        (
            select distinct cov.sucursal
                            ,cov.nro_cuenta
                            ,cov.codigo_producto
                            ,cov.codigo_subproducto
                            ,cast(cov.saldo_original as double) as saldo_original
                            ,cast(cov.saldo_hoy as double) as saldo_hoy
                            ,cov.tipo
                            ,cov.moneda
                            ,SUBSTRING(cov.fecha_alta_producto,1,10) as fecha_alta_producto
                            ,SUBSTRING(cov.fecha_vencimiento_producto,1,10) as fecha_vencimiento_producto
            from bi_corp_bdr.saldos_tarjetas_covid cov
            where cov.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_saldos_tarjetas_covid', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
                and tipo = 'PLAN V'
            
        ) cov
        group by cov.sucursal
            ,cov.nro_cuenta
            ,cov.codigo_producto
            ,cov.codigo_subproducto
            ,cov.tipo
            ,cov.moneda
            ,fecha_alta_producto
            ,fecha_vencimiento_producto 
) cov
inner join 
(
	select cbi.cred_cod_centro,
			cbi.cred_num_cuenta,
			cbi.cred_cod_producto,
			cbi.cred_cod_subprodu_altair,
			cbi.g4095_contra1 ,
			cbi.g4095_coddiv ,
			min(cbi.contr_partition_date) as contr_partition_date
	from bi_corp_bdr.perim_contratos_bis_tarjetas_covid cbi
	where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
	group by cbi.cred_cod_centro,
			cbi.cred_num_cuenta,
			cbi.cred_cod_producto,
			cbi.cred_cod_subprodu_altair,
			cbi.g4095_contra1 ,
			cbi.g4095_coddiv 
) ncr
on 
 lpad(cast(trim(cov.sucursal) as string) ,4,'0') = ncr.cred_cod_centro
and lpad(cast(trim(cov.nro_cuenta) as string),12,'0') = ncr.cred_num_cuenta 
and lpad(cast(trim(cov.codigo_producto) as string),2,'0') = ncr.cred_cod_producto
and cast(trim(cov.codigo_subproducto)  as string) = ncr.cred_cod_subprodu_altair
and trim(cov.moneda) = trim(ncr.g4095_coddiv);