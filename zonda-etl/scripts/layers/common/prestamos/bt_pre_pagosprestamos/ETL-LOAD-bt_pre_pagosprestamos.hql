set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS pedt008;
CREATE TEMPORARY TABLE pedt008 AS
select p.pecodofi, p.penumcon, p.pecodpro, p.pecodsub, p.penumper
from (
      select pecodofi, penumcon, pecodpro, pecodsub, penumper, ROW_NUMBER() OVER (PARTITION BY pecodofi, penumcon, pecodpro, pecodsub ORDER BY coalesce(pefecbrb,'9999-12-31') DESC, peordpar ASC) AS min_firmante
      from bi_corp_staging.malpe_ptb_pedt008
      where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_ptb_pedt008', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
      AND pecalpar = 'TI'
      AND pecodpro IN ('35', '36', '37', '38', '39', '71', '74')
     ) p
where p.min_firmante = 1;

DROP TABLE IF EXISTS pagos_parciales_centinela;
CREATE TEMPORARY TABLE pagos_parciales_centinela AS
select mvr.mvr_entidad as entidad,mvr.mvr_cuenta as cuenta,mvr.mvr_oficina as oficina,mvr.mvr_feliq as feliq
from bi_corp_staging.malug_ugdtrec rec
left join bi_corp_staging.malug_ugdtmvr mvr on mvr.mvr_entidad = rec.rec_entidad and mvr.mvr_cuenta=rec.rec_cuenta and mvr.mvr_oficina=rec.rec_oficina and rec.rec_feliq=mvr.mvr_feliq and rec.partition_date=mvr.partition_date
where rec.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
and mvr.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
and mvr.mvr_indretro='N'
group by mvr.mvr_entidad,mvr.mvr_cuenta,mvr.mvr_oficina,mvr.mvr_feliq
having count(*) > 1;


INSERT OVERWRITE TABLE bi_corp_common.bt_pre_pagosprestamos
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}')
select
mvr_entidad as cod_pre_entidad,
cast(mvr_oficina as int) as cod_suc_sucursal,
cast(mvr_cuenta as bigint) as cod_nro_cuenta,
cast(pedt.penumper as int) as cod_per_nup,
case when mvr_feliq in ('0001-01-01','9999-12-31') then null else mvr_feliq end as dt_pre_feliq,
mvr_nio as cod_pre_nio,
mae.PRODUCTO as cod_prod_producto,
mae.SUBPRO as cod_prod_subproducto,
mvr_numrec as cod_pre_numrec,
case when mvr_feoper in ('0001-01-01','9999-12-31') then null else mvr_feoper end as dt_pre_feoper,
case when mvr_feconta in ('0001-01-01','9999-12-31') then null else mvr_feconta end as dt_pre_feconta,
case when mvr_fevalor in ('0001-01-01','9999-12-31') then null else mvr_fevalor end as dt_pre_fevalor,
mvr_capinire as fc_pre_capinire,
case when mvr_ind_desgcapi = 'N' then 0 else 1 end as flag_pre_inddesgcapi,
mvr_codconli_cap as cod_pre_conlicap,
case when mvr_ind_desg_reajcap = 'N' then 0 else 1 end as flag_pre_inddesgreajcap,
case when mvr_ind_desg_reajseg = 'N' then 0 else 1 end as flag_pre_inddesgreajseg,
mvr_intinire as fc_pre_intinire,
case when mvr_ind_desginte = 'N' then 0 else 1 end as flag_pre_inddesginte,
mvr_codconli_int as cod_pre_conliint,
case when mvr_ind_desgcomi = 'N' then 0 else 1 end as flag_pre_inddesgcomi,
mvr_cominire as fc_pre_cominire,
case when mvr_codconli_com='' then null else mvr_codconli_com end as cod_pre_conlicom,
case when mvr_ind_desggast = 'N' then 0 else 1 end as flag_pre_inddesggast,
mvr_gasinire as fc_pre_gasinire,
case when mvr_codconli_gas='' then null else mvr_codconli_gas end as cod_pre_conligas,
case when mvr_ind_desgsegu = 'N' then 0 else 1 end as flag_pre_inddesgsegu,
mvr_seginire as fc_pre_seginire,
case when mvr_codconli_seg='' then null else mvr_codconli_seg end as cod_pre_conliseg,
case when mvr_ind_desgimpu = 'N' then 0 else 1 end as flag_pre_inddesgimpu,
mvr_impinire as fc_pre_impinire,
case when mvr_codconli_imp='' then null else mvr_codconli_imp end as cod_pre_conliimp,
mvr_por_alicuota as fc_pre_poralicuota,
case when mvr_ind_liqimpue = 'N' then 0 else 1 end as flag_pre_indliqimpue,
mvr_imp_base as fc_pre_impbase,
case when mvr_fecalmora in ('0001-01-01','9999-12-31') then null else mvr_fecalmora end as dt_pre_fecalmora,
case when mvr_ind_desgmora = 'N' then 0 else 1 end as flag_pre_inddesgmora,
case when mvr_codconli_mor='' then null else mvr_codconli_mor end as cod_pre_conlimor,
mvr_imp_mora as fc_pre_impmora,
mvr_imp_concobex as fc_pre_impconcobex,
case when mvr_codconli_cobex='' then null else mvr_codconli_cobex end as cod_pre_conlicobex,
case when mvr_ind_desgcobe = 'N' then 0 else 1 end as flag_pre_inddesgcobe,
case when mvr_ind_desgcps = 'N' then 0 else 1 end as flag_pre_inddesgcps,
case when mvr_codconli_cps='' then null else mvr_codconli_cps end as cod_pre_conlicps,
mvr_imp_cps as fc_pre_impcps,
mvr_salreal as fc_pre_salreal,
case when mvr_ind_formpago = 'N' then 0 else 1 end as flag_pre_indformpago,
mvr_imp_pago as fc_pre_imppago,
mvr_cod_divi_pago as cod_pre_divipago,
case when mvr_cod_entchequ='' then null else mvr_cod_entchequ end as cod_pre_entchequ,
case when mvr_cod_ofichequ='' then null else mvr_cod_ofichequ end as cod_pre_ofichequ,
case when mvr_cod_ctachequ='' then null else mvr_cod_ctachequ end as cod_pre_ctachequ,
mvr_num_docchequ as cod_pre_numdocchequ,
case when mvr_tip_docchequ='' then null else mvr_tip_docchequ end as cod_pre_tipdocchequ,
case when mvr_fec_dispchequ in ('0001-01-01','9999-12-31') then null else mvr_fec_dispchequ end as dt_pre_fecdispchequ,
case when mvr_cod_plaza='' then null else mvr_cod_plaza end as cod_pre_plaza,
case when mvr_entidad_pag='' then null else mvr_entidad_pag end as cod_pre_entidadpag,
cast(mvr_centro_pag as int) as cod_suc_sucursalpag,
cast(mvr_cuenta_pag as bigint) as cod_nro_cuentapag,
case when mvr_digiccc1_pag='' then null else mvr_digiccc1_pag end as cod_pre_digiccc1pag,
case when mvr_digiccc2_pag='' then null else mvr_digiccc2_pag end  as cod_pre_digiccc2pag,
mvr_cod_divisa as cod_div_divisa,
mvr_imp_cambdivi as fc_pre_impcambdivi,
mvr_sitdeuct as cod_pre_sitdeuct,
case when mvr_tip_condonar='' then null else mvr_tip_condonar end as cod_pre_tipcondonar,
mvr_cod_evento as cod_pre_evento,
mvr_num_cob_ctso as cod_pre_numcobctso, --int
case when mvr_indretro = 'N' then 0 else 1 end as flag_pre_indretro,
case when mvr_fecretro in ('0001-01-01','9999-12-31') then null else mvr_fecretro end as dt_pre_fecretro,
case when mvr_entidad_retro='' then null else mvr_entidad_retro end as cod_pre_entidadretro,
case when mvr_centro_retro='' then null else mvr_centro_retro end as cod_pre_centroretro,
case when mvr_userid_retro='' then null else mvr_userid_retro end as cod_pre_useridretro,
case when mvr_netname_retro='' then null else mvr_netname_retro end  as cod_pre_netnameretro,
case when mvr_timestamp_retro in ('0001-01-01-00.00.00.000000') then null else mvr_timestamp_retro end as ts_pre_timestampretro,
mvr_entidad_umo as cod_pre_entidadumo,
mvr_centro_umo as cod_pre_centroumo,
mvr_userid_umo as cod_pre_useridumo,
mvr_netname_umo as cod_pre_netnameumo,
mvr_timestamp_umo as ts_pre_timestampumo,
case when pp.entidad is not null then 1 else 0 end as flag_pre_cobroparcialcentinela
from bi_corp_staging.malug_ugdtmvr mvr
inner join bi_corp_staging.vmalug_ugdtmae mae on mvr.mvr_entidad=mae.entidad and mvr.mvr_cuenta=mae.cuenta and mvr.mvr_oficina=mae.oficina and mvr.partition_date=mae.partition_date
left join pedt008 pedt on  mae.oficina = pedt.pecodofi and mae.cuenta = pedt.penumcon and mae.producto = pedt.pecodpro and mae.subpro = pedt.pecodsub
left join pagos_parciales_centinela pp on mvr.mvr_entidad = pp.entidad and mvr.mvr_cuenta=pp.cuenta and mvr.mvr_oficina=pp.oficina and mvr.mvr_feliq=pp.feliq and mvr.mvr_userid_umo='CENTINEL'
where mvr.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
and mae.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
and mvr_indretro='N'
and mvr_feoper = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}';