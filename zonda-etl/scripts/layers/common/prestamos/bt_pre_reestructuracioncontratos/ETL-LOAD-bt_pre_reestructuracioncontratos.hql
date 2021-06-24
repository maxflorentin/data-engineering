set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


INSERT OVERWRITE TABLE bi_corp_common.bt_pre_reestructuracioncontratos
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}')
select
086_cod_entidad as cod_pre_entidad,
086_cod_centro as cod_suc_sucursal,
086_num_contrato as cod_nro_cuenta,
086_num_persona as cod_per_nup,
086_cod_producto as cod_prod_producto,
086_cod_subprodu as cod_prod_subproducto,
086_cod_divisa as cod_div_divisa,
086_cod_centrod as cod_suc_sucursalreestructuracion,
086_num_contratd as cod_pre_contratoreestructuracion,
086_cod_productd as cod_pre_productoreestructuracion,
086_cod_subprodd as cod_pre_subproductoreestructuracion,
086_cod_divisad as cod_pre_divisareestructuracion,
case when 086_fec_refinanc  in ('0001-01-01','9999-12-31') then null else 086_fec_refinanc end as dt_pre_fecrefinanc,
086_cod_reesctr as cod_pre_reesctr,
case when 086_ind_antconint='S' then 1 else 0 end as flag_pre_indantconint,
086_cod_marca_seg_esp as cod_pre_marcasegesp,
086_cod_marca as cod_pre_marcagarra,
086_cod_submarca as cod_pre_submarcagarra,
case when 086_fec_primer_imp in ('0001-01-01','9999-12-31') then null else 086_fec_primer_imp end as dt_pre_fecprimerimp,
086_imp_refnacdo as fc_pre_importerefinanciado,
086_imp_refnacdl as fc_pre_importerefinanciadodolpesif,
086_imp_anticipo as fc_pre_importeintereses
from bi_corp_staging.garra_contratos_ref
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}';