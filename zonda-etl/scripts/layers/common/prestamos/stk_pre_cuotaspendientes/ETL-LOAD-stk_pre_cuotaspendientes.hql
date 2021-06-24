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

INSERT OVERWRITE TABLE bi_corp_common.stk_pre_cuotaspendientes
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}')
select
mae.entidad as cod_pre_entidad,
cast(mae.oficina as int) as cod_suc_sucursal,
cast(mae.cuenta as bigint) as cod_nro_cuenta,
cast(pedt.penumper as int) as cod_per_nup,
mae.producto as cod_pro_producto,
mae.subpro as cod_pro_subproducto,
mae.divisa as cod_div_divisa,
cast(translate(drb.UGNPLINI,'.','') as int) as cod_pre_plazo,
case when cq.cuotasq_rec_feliq <= cq.partition_date and mae.acuimpa <> '0' then 'V' else 'N' end as cod_pre_estado,
cq.cuotasq_rec_numrec cod_pre_recibo,
cq.cuotasq_rec_feliq dt_pre_fechavencimiento,
case when ptmosq.ptmosq_mae_sitdeuct = '20' then ptmosq.ptmosq_adi1_nvo_rub_20_k when cq.cuotasq_rec_sitdeuct = '0' then ptmosq_adi1_nvo_rubro_k when cq.cuotasq_rec_sitdeuct = '10' then ptmosq_adi1_nvo_rub_10_k else null end as cod_pre_rubrocapital,
case when ptmosq.ptmosq_mae_sitdeuct = '20' then ptmosq.ptmosq_adi1_nvo_rub_20_intdev when cq.cuotasq_rec_sitdeuct = '0' then ptmosq.ptmosq_adi1_nvo_rubro_intdev when cq.cuotasq_rec_sitdeuct = '10' then ptmosq.ptmosq_adi1_nvo_rub_10_intdev else null end as cod_pre_rubrointeres,
case when ptmosq.ptmosq_mae_sitdeuct = '20' and cq.cuotasq_rec_impajust_cap <> 0 then ptmosq.ptmosq_adi1_rub_20_nvo_cap_ajuste when cq.cuotasq_rec_sitdeuct = '0' and cq.cuotasq_rec_impajust_cap <> 0 then ptmosq.ptmosq_adi1_rubro_nvo_cap_ajuste when cq.cuotasq_rec_sitdeuct = '10' and cq.cuotasq_rec_impajust_cap <> 0 then ptmosq.ptmosq_adi1_rub_10_nvo_cap_ajuste else null end as cod_pre_rubrocapajuste,
case when cq.cuotasq_rec_sitdeuct = '0' then (case when ptmosq.ptmosq_adi1_rubro_int_document = '' then null else ptmosq.ptmosq_adi1_rubro_int_document end) when cq.cuotasq_rec_sitdeuct = '10' then (case when ptmosq.ptmosq_adi1_nvo_rub_10_int_document='' then null else ptmosq.ptmosq_adi1_nvo_rub_10_int_document end) else null end as cod_pre_rubrointeresdocumentadovencido,
drb.ugtfmini as cod_pre_tipoamortizacion,
drb.TIPOTASA as cod_pre_tipotasa,
cq.cuotasq_rec_salteor as fc_pre_saldoteorico,
(cq.cuotasq_rec_capinire - cq.cuotasq_rec_caprecre) - (cq.cuotasq_rec_impajust_cap - cq.cuotasq_rec_rea_orden) as fc_pre_importecapital,
cq.cuotasq_rec_intinire - cq.cuotasq_rec_intrecre as fc_pre_importeinteres,
cq.cuotasq_rec_impajust_cap - cq.cuotasq_rec_rea_orden as fc_pre_ajustecapitalvencido
from bi_corp_staging.malug_cuotasq cq
left join bi_corp_staging.vmalug_ugdtmae mae on cq.cuotasq_rec_cuenta=mae.cuenta and cq.cuotasq_rec_oficina=mae.oficina and cq.cuotasq_rec_entidad=mae.entidad and mae.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
inner join bi_corp_staging.malug_ptmosq ptmosq on cq.cuotasq_rec_cuenta=ptmosq.ptmosq_mae_cuenta and cq.cuotasq_rec_oficina=ptmosq.ptmosq_mae_oficina and cq.cuotasq_rec_entidad=ptmosq.ptmosq_mae_entidad and ptmosq.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join bi_corp_staging.vmalug_ugdtdrb drb on drb.entidad=mae.entidad and drb.cuenta=mae.cuenta and drb.oficina=mae.oficina and drb.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join pedt008 pedt on mae.oficina = pedt.pecodofi and mae.cuenta = pedt.penumcon and mae.producto = pedt.pecodpro and mae.subpro = pedt.pecodsub
where cq.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
and cq.cuotasq_rec_indpeco <> '0' and ptmosq.ptmosq_mae_sitpres in ('0','2');