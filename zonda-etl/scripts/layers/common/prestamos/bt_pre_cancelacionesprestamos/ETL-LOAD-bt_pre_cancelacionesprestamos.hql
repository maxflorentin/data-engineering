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


INSERT OVERWRITE TABLE bi_corp_common.bt_pre_cancelacionesprestamos
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}')
select distinct
t.mov_entidad as cod_pre_entidad,
cast(t.mov_oficina as int) as cod_suc_sucursal,
cast(t.mov_cuenta as bigint) as cod_nro_cuenta,
cast(pedt.penumper as int) as cod_per_nup,
t.mov_cod_divisa as cod_div_divisa,
case when t.mov_feoper in ('0001-01-01','9999-12-31') then null else t.mov_feoper end as dt_pre_fechaoper,
case when t.mov_fevalor in ('0001-01-01','9999-12-31') then null else t.mov_fevalor end as dt_pre_fechavalor,
trim(t.mov_cod_evento) as cod_pre_evento,
t.desc_evento as ds_pre_evento,
t.capinire as fc_pre_capinire,
t.intinire as fc_pre_intinire,
t.cominire  as fc_pre_cominire,
t.gasinire as fc_pre_gasinire,
t.seginire as fc_pre_seginire,
t.impinire as fc_pre_impinire,
t.mov_salreal as fc_pre_salreal,
trim(t.mov_ind_formpago) as cod_pre_formpago,
t.mov_imp_pago as fc_pre_imppago,
case when t.mov_entidad_pag='' then null else t.mov_entidad_pag end as cod_pre_entidadpag,
cast(t.mov_centro_pag as int) as cod_pre_sucursalpag,
cast(t.mov_cuenta_pag as bigint) as cod_pre_cuentapag,
trim(t.mov_userid_umo) as cod_pre_useridumo,
t.producto as cod_prod_producto,
t.subpro as cod_prod_subproducto,
cast(t.codides as int) as cod_pre_codides
from
(select mov.mov_cuenta,
mov.mov_oficina,
mov.mov_entidad ,
mov.mov_cod_divisa,
mov.mov_feoper,
mov.mov_fevalor,
mov.mov_cod_evento,
case mov.mov_cod_evento when 'CAAN' then 'CANCELACION TOTAL'
     when 'ENAN' then 'CANCELACION PARCIAL ANTICIPADA'
     end as desc_evento,
sum(case when mov.mov_codconli='K01' then mov.mov_impmovi else 0 end) over(partition by mov.mov_cuenta,mov.mov_oficina,mov.mov_entidad, mov.mov_nio) as capinire,
sum(case when mov.mov_codconli='101' then mov.mov_impmovi else 0 end) over(partition by mov.mov_cuenta,mov.mov_oficina,mov.mov_entidad, mov.mov_nio) as intinire,
sum(case when substring(mov.mov_codconli,1,1)='3' then mov.mov_impmovi else 0 end) over(partition by mov.mov_cuenta,mov.mov_oficina,mov.mov_entidad, mov.mov_nio) as cominire,
sum(case when substring(mov.mov_codconli,1,1)='4' then mov.mov_impmovi else 0 end) over(partition by mov.mov_cuenta,mov.mov_oficina,mov.mov_entidad, mov.mov_nio) as gasinire,
sum(case when substring(mov.mov_codconli,1,1)='S' then mov.mov_impmovi else 0 end) over(partition by mov.mov_cuenta,mov.mov_oficina,mov.mov_entidad, mov.mov_nio) as seginire,
sum(case when substring(mov.mov_codconli,1,1)='I' then mov.mov_impmovi else 0 end) over(partition by mov.mov_cuenta,mov.mov_oficina,mov.mov_entidad, mov.mov_nio) as impinire,
mov.mov_salreal,
max(mov.mov_ind_formpago) over(partition by mov.mov_cuenta,mov.mov_oficina,mov.mov_entidad, mov.mov_nio) as mov_ind_formpago,
max(mov.mov_imp_pago) over(partition by mov.mov_cuenta,mov.mov_oficina,mov.mov_entidad, mov.mov_nio) as mov_imp_pago,
max(mov.mov_entidad_pag) over(partition by mov.mov_cuenta,mov.mov_oficina,mov.mov_entidad, mov.mov_nio) as mov_entidad_pag,
max(mov.mov_centro_pag) over(partition by mov.mov_cuenta,mov.mov_oficina,mov.mov_entidad, mov.mov_nio) as mov_centro_pag,
max(mov.mov_cuenta_pag) over(partition by mov.mov_cuenta,mov.mov_oficina,mov.mov_entidad, mov.mov_nio) as mov_cuenta_pag,
mov.mov_userid_umo,
mae.producto,
mae.subpro,
mae.codides,
mov.partition_date
from bi_corp_staging.malug_ugdtmov mov
left join bi_corp_staging.vmalug_ugdtmae mae on mae.entidad=mov.mov_entidad and mae.oficina=mov.mov_oficina and mae.cuenta=mov.mov_cuenta and mae.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
where mov.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
and mov_feoper = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
and mov_indretro='N'
and mov_tipo_mov='C'
and (mov_cod_evento='CAAN' or mov_cod_evento='ENAN'))t
left join pedt008 pedt on  t.mov_oficina =pedt.pecodofi and t.mov_cuenta = pedt.penumcon and t.producto = pedt.pecodpro and t.subpro = pedt.pecodsub;