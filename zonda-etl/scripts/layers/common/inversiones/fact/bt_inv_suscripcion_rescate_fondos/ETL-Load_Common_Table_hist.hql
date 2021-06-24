set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

--------------Calculo la maxima particion PEDT008
CREATE TEMPORARY TABLE tmp_maxpart008 AS
select max(partition_date) AS partition_date
  from bi_corp_Staging.malpe_pedt008
 where partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones_hist') }}',7)
  and partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones_hist') }}'
   and pecdgent = '0072' and pecodofi = '0000' and pecalpar = 'TI' and pecodpro = '60' and pecodsub = '0000';

-- Elimino los nups duplicados de PEDT008
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_NUPS as
SELECT cast(t.penumper as bigint) nup,cast(t.penumcon as bigint) cod_cuenta
FROM
  ( SELECT pe1.penumper,pe1.penumcon,PE1.peordpar,row_number() over (partition by pe1.penumcon order by PE1.peordpar) as row_num
   FROM bi_corp_staging.malpe_pedt008 pe1
   INNER JOIN tmp_maxpart008 mx8 on (pe1.partition_date=mx8.partition_date)
   WHERE pe1.pecdgent = '0072' --(Santander)
   AND pe1.pecodofi = '0000'  --(Casa Central)
   AND pe1.pecalpar = 'TI'  -- (Titular)
   AND pe1.pecodpro = '60'   -- (Producto Fondos)
   AND pe1.pecodsub = '0000' -- (SubProducto Fondos)
  ) t
 WHERE t.row_num = 1;

--------------Calculo la maxima particion PEDT042
CREATE TEMPORARY TABLE tmp_maxpart042 AS
select max(partition_date) AS partition_date
  from bi_corp_Staging.malpe_ptb_pedt042
 where partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones_hist') }}',7)
  and partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones_hist') }}'
   and pecdgent = '0072' and pecodpro = '60' and pecodmon = 'ARS';

-- Elimino los  duplicados de PEDT042
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_SUC_OPERATIVA as
SELECT distinct cast(p.pesucope as int) cod_inv_sucursal_operativa,cast(p.penumcon as int) cod_inv_cuenta
   FROM bi_corp_staging.malpe_ptb_pedt042 p
   INNER JOIN tmp_maxpart042 mx42 on (p.partition_date=mx42.partition_date)
   WHERE p.pecdgent = '0072' --(Santander)
   AND p.pecodpro = '60'   -- (Producto Fondos)
   AND p.pecodmon = 'ARS';

-- Creo tabla temporal suscripciones
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_SUSCRIPCION as
select
a.cod_inv_fondo,
a.cod_inv_cuenta,
a.cod_inv_certificado,
a.cod_inv_fondo_orig_dest,
a.cod_inv_cli_orig_dest,
a.cod_inv_canal_liq,
a.cod_inv_agte,
a.cod_inv_canal,
a.cod_inv_agte_dg,
a.cod_inv_canal_dg,
a.cod_inv_oper_dg,
a.cod_inv_agte_cd,
a.cod_inv_canal_cd,
a.cod_inv_oper_cd,
a.ts_inv_input,
a.dt_inv_solic,
a.dt_inv_conver,
a.dt_inv_estorno,
a.cod_inv_forma_pago,
a.cod_inv_tipo_cta,
a.cod_inv_moneda_cta,
a.ds_inv_tipo_cta,
a.cod_inv_cta_num,
a.fc_inv_qt_cot_rgt,
a.cod_inv_cancelacion,
a.cod_inv_moneda_cambio,
a.fc_inv_conv_pact,
a.cod_inv_operacion,
a.cod_inv_cliente_ac,
a.cod_inv_oper_orig,
a.cod_inv_resg_orig,
a.cod_inv_ctod_orig,
a.dt_inv_ult_reg,
a.dt_inv_bloqueio,
a.cod_inv_bloqueio,
a.cod_inv_dias_ut_dec,
a.fc_inv_cota_base,
a.fc_inv_apl_liq,
a.fc_inv_qt_cot_apl,
a.fc_inv_sd_cot_atu,
a.fc_inv_cota_liq_d0,
a.fc_inv_cotacao_pact,
a.dt_inv_comprom,
a.flag_inv_apl_transf,
a.cod_inv_nr_rgt,
a.cod_inv_nr_cert_dest,
a.dt_inv_dtpagto,
a.fc_inv_perc_comis,
a.fc_inv_val_rgt_brt,
a.fc_inv_val_rgt_liq,
a.fc_inv_val_cust_rgt,
a.fc_inv_val_cota_rgt,
a.fc_inv_cotacao_indi,
a.cod_inv_liqu_rgt,
a.ds_inv_tipo_oper,
a.partition_date
    from (
    select
    cast(s.cod_fdo as bigint) cod_inv_fondo,
    cast(substr(s.cod_cliente,4) as bigint) cod_inv_cuenta,
    cast(s.certificado as bigint) cod_inv_certificado,
    cast(s.cod_fdo_orig as bigint) cod_inv_fondo_orig_dest,
    cast(s.cod_cli_orig as bigint) cod_inv_cli_orig_dest,
    cast(s.cod_can_liq as bigint) cod_inv_canal_liq,
    cast(s.cod_agte_solic as bigint) cod_inv_agte,
    cast(s.cod_canal_solic as bigint) cod_inv_canal,
    cast(s.cod_agte_dg as bigint) cod_inv_agte_dg,
    cast(s.cod_can_dg as bigint) cod_inv_canal_dg,
    cast(s.cod_oper_dg as bigint) cod_inv_oper_dg,
    cast(s.cod_agte_cd as bigint) cod_inv_agte_cd,
    cast(s.cod_can_cd as bigint) cod_inv_canal_cd,
    cast(s.cod_oper_cd as bigint) cod_inv_oper_cd,
    case when CONCAT(SUBSTRING(s.dtinput,1,4),'-',SUBSTRING(s.dtinput,5,2),'-',SUBSTRING(s.dtinput,7,2)) in ('9999-12-31', '0001-01-01','0000-00-00') then NULL
         else from_unixtime(UNIX_TIMESTAMP(CONCAT(CONCAT(SUBSTRING(s.dtinput,1,4),'-',SUBSTRING(s.dtinput,5,2),'-',SUBSTRING(s.dtinput,7,2)),' ',CONCAT(SUBSTRING(s.hora,1,2),':',SUBSTRING(s.hora,3,2),':',SUBSTRING(s.hora,5,2))) ,'yyyy-MM-dd HH:mm:ss')) end as ts_inv_input,
    case when CONCAT(SUBSTRING(s.dtsolic,1,4),'-',SUBSTRING(s.dtsolic,5,2),'-',SUBSTRING(s.dtsolic,7,2)) in ('9999-12-31', '0001-01-01','0000-00-00') then NULL
         else CONCAT(SUBSTRING(s.dtsolic,1,4),'-',SUBSTRING(s.dtsolic,5,2),'-',SUBSTRING(s.dtsolic,7,2)) end dt_inv_solic,
    case when CONCAT(SUBSTRING(s.dtconver,1,4),'-',SUBSTRING(s.dtconver,5,2),'-',SUBSTRING(s.dtconver,7,2)) in ('9999-12-31', '0001-01-01','0000-00-00') then NULL
         else CONCAT(SUBSTRING(s.dtconver,1,4),'-',SUBSTRING(s.dtconver,5,2),'-',SUBSTRING(s.dtconver,7,2)) end dt_inv_conver,
    case when CONCAT(SUBSTRING(s.dtestorno,1,4),'-',SUBSTRING(s.dtestorno,5,2),'-',SUBSTRING(s.dtestorno,7,2)) in ('9999-12-31', '0001-01-01','0000-00-00') then NULL
         else CONCAT(SUBSTRING(s.dtestorno,1,4),'-',SUBSTRING(s.dtestorno,5,2),'-',SUBSTRING(s.dtestorno,7,2)) end dt_inv_estorno,
    cast(s.forma_pagto as bigint) cod_inv_forma_pago,
    cast(s.tipo_cta as bigint) cod_inv_tipo_cta,
    cast(s.moeda_cta as bigint) cod_inv_moneda_cta,
    trim(s.nr_cta_tip) ds_inv_tipo_cta,
    cast(s.nr_cta_num as bigint) cod_inv_cta_num,
    (cast(s.qt_cot_rgt as bigint)/10000) fc_inv_qt_cot_rgt,
    trim(s.num_cancel) cod_inv_cancelacion,
    cast(s.moeda_cambio as bigint) cod_inv_moneda_cambio,
    (cast(s.vl_conv_pact as bigint)/100) fc_inv_conv_pact,
    cast(s.nr_oper as bigint) cod_inv_operacion,
    cast(s.cliente_ac as bigint) cod_inv_cliente_ac,
    cast(s.nr_oper_orig as bigint) cod_inv_oper_orig,
    cast(s.nr_resg_orig as bigint) cod_inv_resg_orig,
    cast(s.cod_ctod_orig as bigint) cod_inv_ctod_orig,
    case when CONCAT(SUBSTRING(s.dtultrgt,1,4),'-',SUBSTRING(s.dtultrgt,5,2),'-',SUBSTRING(s.dtultrgt,7,2)) in ('9999-12-31', '0001-01-01','0000-00-00') then NULL
         else CONCAT(SUBSTRING(s.dtultrgt,1,4),'-',SUBSTRING(s.dtultrgt,5,2),'-',SUBSTRING(s.dtultrgt,7,2)) end dt_inv_ult_reg,
    case when CONCAT(SUBSTRING(s.dtbloqueio,1,4),'-',SUBSTRING(s.dtbloqueio,5,2),'-',SUBSTRING(s.dtbloqueio,7,2)) in ('9999-12-31', '0001-01-01','0000-00-00') then NULL
         else CONCAT(SUBSTRING(s.dtbloqueio,1,4),'-',SUBSTRING(s.dtbloqueio,5,2),'-',SUBSTRING(s.dtbloqueio,7,2)) end dt_inv_bloqueio,
    cast(s.cod_bloqueio as bigint) cod_inv_bloqueio,
    cast(s.dias_ut_dec as bigint) cod_inv_dias_ut_dec,
    (cast(s.val_cota_base as bigint)/100000000) fc_inv_cota_base,
    (cast(s.val_apl_liq as bigint)/100) fc_inv_apl_liq,
    (cast(s.qt_cot_apl as bigint)/10000) fc_inv_qt_cot_apl,
    (cast(s.sd_cot_atu as bigint)/10000) fc_inv_sd_cot_atu,
    (cast(s.val_cota_liq_d0 as bigint)/1000000000) fc_inv_cota_liq_d0,
    (cast(s.cotacao_pact as bigint)/100000000) fc_inv_cotacao_pact,
    case when CONCAT(SUBSTRING(s.dtcomprom,1,4),'-',SUBSTRING(s.dtcomprom,5,2),'-',SUBSTRING(s.dtcomprom,7,2)) in ('9999-12-31', '0001-01-01','0000-00-00') then NULL
         else CONCAT(SUBSTRING(s.dtcomprom,1,4),'-',SUBSTRING(s.dtcomprom,5,2),'-',SUBSTRING(s.dtcomprom,7,2)) end dt_inv_comprom,
    case when trim(s.apl_transf) = 'S' then 1
        else 0 end flag_inv_apl_transf,
    cast(NULL as bigint) cod_inv_nr_rgt,
    cast(NULL as bigint) cod_inv_nr_cert_dest,
    cast(NULL as string) dt_inv_dtpagto,
    cast(NULL as decimal(17,2)) fc_inv_perc_comis,
    cast(NULL as decimal(17,2)) fc_inv_val_rgt_brt,
    cast(NULL as decimal(17,2)) fc_inv_val_rgt_liq,
    cast(NULL as decimal(17,2)) fc_inv_val_cust_rgt,
    cast(NULL as decimal(17,2)) fc_inv_val_cota_rgt,
    cast(NULL as decimal(17,2)) fc_inv_cotacao_indi,
    cast(NULL as bigint) cod_inv_liqu_rgt,
    'SUSCRIPCION' ds_inv_tipo_oper,
    CONCAT(SUBSTRING(s.dtinput,1,4),'-',SUBSTRING(s.dtinput,5,2),'-',SUBSTRING(s.dtinput,7,2)) partition_date,
    row_number() over (partition by s.cod_cliente, s.cod_fdo_orig, s.certificado, s.nr_oper order by s.partition_date desc ) as row_num
    from bi_corp_staging.fm_suscripciones s
    where s.partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones_hist') }}',30)
      and s.partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones_hist') }}') a
where a.row_num = 1;

-- Creo tabla temporal rescates
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_RESCATE as
select
a.cod_inv_fondo,
a.cod_inv_cuenta,
a.cod_inv_certificado,
a.cod_inv_fondo_orig_dest,
a.cod_inv_cli_orig_dest,
a.cod_inv_canal_liq,
a.cod_inv_agte,
a.cod_inv_canal,
a.cod_inv_agte_dg,
a.cod_inv_canal_dg,
a.cod_inv_oper_dg,
a.cod_inv_agte_cd,
a.cod_inv_canal_cd,
a.cod_inv_oper_cd,
a.ts_inv_input,
a.dt_inv_solic,
a.dt_inv_conver,
a.dt_inv_estorno,
a.cod_inv_forma_pago,
a.cod_inv_tipo_cta,
a.cod_inv_moneda_cta,
a.ds_inv_tipo_cta,
a.cod_inv_cta_num,
a.fc_inv_qt_cot_rgt,
a.cod_inv_cancelacion,
a.cod_inv_moneda_cambio,
a.fc_inv_conv_pact,
a.cod_inv_operacion,
a.cod_inv_cliente_ac,
a.cod_inv_oper_orig,
a.cod_inv_resg_orig,
a.cod_inv_ctod_orig,
a.dt_inv_ult_reg,
a.dt_inv_bloqueio,
a.cod_inv_bloqueio,
a.cod_inv_dias_ut_dec,
a.fc_inv_cota_base,
a.fc_inv_apl_liq,
a.fc_inv_qt_cot_apl,
a.fc_inv_sd_cot_atu,
a.fc_inv_cota_liq_d0,
a.fc_inv_cotacao_pact,
a.dt_inv_comprom,
a.flag_inv_apl_transf,
a.cod_inv_nr_rgt,
a.cod_inv_nr_cert_dest,
a.dt_inv_dtpagto,
a.fc_inv_perc_comis,
a.fc_inv_val_rgt_brt,
a.fc_inv_val_rgt_liq,
a.fc_inv_val_cust_rgt,
a.fc_inv_val_cota_rgt,
a.fc_inv_cotacao_indi,
a.cod_inv_liqu_rgt,
a.ds_inv_tipo_oper,
a.partition_date
    from
    (select
    cast(r.cod_fdo as bigint) cod_inv_fondo,
    cast(substr(r.cod_cliente,4) as bigint) cod_inv_cuenta,
    cast(r.certificado as bigint) cod_inv_certificado,
    cast(r.cod_fdo_dest as bigint) cod_inv_fondo_orig_dest,
    cast(r.cod_cli_dest as bigint) cod_inv_cli_orig_dest,
    cast(r.cod_can_liq as bigint) cod_inv_canal_liq,
    cast(r.cod_agte_solic as bigint) cod_inv_agte,
    cast(r.cod_canal_solic as bigint) cod_inv_canal,
    cast(r.cod_agte_dg as bigint) cod_inv_agte_dg,
    cast(r.cod_can_dg as bigint) cod_inv_canal_dg,
    cast(r.cod_oper_dg as bigint) cod_inv_oper_dg,
    cast(r.cod_agte_cd as bigint) cod_inv_agte_cd,
    cast(r.cod_can_cd as bigint) cod_inv_canal_cd,
    cast(r.cod_oper_cd as bigint) cod_inv_oper_cd,
    case when CONCAT(SUBSTRING(r.dtinput,1,4),'-',SUBSTRING(r.dtinput,5,2),'-',SUBSTRING(r.dtinput,7,2)) in ('9999-12-31', '0001-01-01','0000-00-00') then NULL
         else from_unixtime(UNIX_TIMESTAMP(CONCAT(CONCAT(SUBSTRING(r.dtinput,1,4),'-',SUBSTRING(r.dtinput,5,2),'-',SUBSTRING(r.dtinput,7,2)),' ',CONCAT(SUBSTRING(r.hora,1,2),':',SUBSTRING(r.hora,3,2),':',SUBSTRING(r.hora,5,2))) ,'yyyy-MM-dd HH:mm:ss')) end as ts_inv_input,
    case when CONCAT(SUBSTRING(r.dtsolic,1,4),'-',SUBSTRING(r.dtsolic,5,2),'-',SUBSTRING(r.dtsolic,7,2)) in ('9999-12-31', '0001-01-01','0000-00-00') then NULL
         else CONCAT(SUBSTRING(r.dtsolic,1,4),'-',SUBSTRING(r.dtsolic,5,2),'-',SUBSTRING(r.dtsolic,7,2)) end dt_inv_solic,
    case when CONCAT(SUBSTRING(r.dtconver,1,4),'-',SUBSTRING(r.dtconver,5,2),'-',SUBSTRING(r.dtconver,7,2)) in ('9999-12-31', '0001-01-01','0000-00-00') then NULL
         else CONCAT(SUBSTRING(r.dtconver,1,4),'-',SUBSTRING(r.dtconver,5,2),'-',SUBSTRING(r.dtconver,7,2)) end dt_inv_conver,
    case when CONCAT(SUBSTRING(r.dtestorno,1,4),'-',SUBSTRING(r.dtestorno,5,2),'-',SUBSTRING(r.dtestorno,7,2)) in ('9999-12-31', '0001-01-01','0000-00-00') then NULL
         else CONCAT(SUBSTRING(r.dtestorno,1,4),'-',SUBSTRING(r.dtestorno,5,2),'-',SUBSTRING(r.dtestorno,7,2)) end dt_inv_estorno,
    cast(r.forma_pagto as bigint) cod_inv_forma_pago,
    cast(r.tipo_cta as bigint) cod_inv_tipo_cta,
    cast(r.moeda_cta as bigint) cod_inv_moneda_cta,
    trim(r.nr_cta_tip) ds_inv_tipo_cta,
    cast(r.nr_cta_num as bigint) cod_inv_cta_num,
    (cast(r.qt_cot_rgt as bigint)/10000) fc_inv_qt_cot_rgt,
    trim(r.num_cancel) cod_inv_cancelacion,
    cast(r.moeda_cambio as bigint) cod_inv_moneda_cambio,
    (cast(r.vl_conv_pact as bigint)/100) fc_inv_conv_pact,
    cast(r.nr_oper as bigint) cod_inv_operacion,
    cast(r.cliente_ac as bigint) cod_inv_cliente_ac,
    NULL cod_inv_oper_orig,
    NULL cod_inv_resg_orig,
    NULL cod_inv_ctod_orig,
    NULL dt_inv_ult_reg,
    NULL dt_inv_bloqueio,
    NULL cod_inv_bloqueio,
    NULL cod_inv_dias_ut_dec,
    NULL fc_inv_cota_base,
    NULL fc_inv_apl_liq,
    NULL fc_inv_qt_cot_apl,
    NULL fc_inv_sd_cot_atu,
    NULL fc_inv_cota_liq_d0,
    NULL fc_inv_cotacao_pact,
    NULL dt_inv_comprom,
    0 flag_inv_apl_transf,
    cast(r.nr_rgt as bigint) cod_inv_nr_rgt,
    cast(r.nr_cert_dest as bigint) cod_inv_nr_cert_dest,
    case when CONCAT(SUBSTRING(r.dtpagto,1,4),'-',SUBSTRING(r.dtpagto,5,2),'-',SUBSTRING(r.dtpagto,7,2)) in ('9999-12-31', '0001-01-01','0000-00-00') then NULL
         else CONCAT(SUBSTRING(r.dtpagto,1,4),'-',SUBSTRING(r.dtpagto,5,2),'-',SUBSTRING(r.dtpagto,7,2)) end dt_inv_dtpagto,
    (cast(r.perc_comis as bigint)/100) fc_inv_perc_comis,
    (cast(r.val_rgt_brt as bigint)/100) fc_inv_val_rgt_brt,
    (cast(r.val_rgt_liq as bigint)/100) fc_inv_val_rgt_liq,
    (cast(r.val_cust_rgt as bigint)/100) fc_inv_val_cust_rgt,
    (cast(r.val_cota_rgt as bigint)/100000000) fc_inv_val_cota_rgt,
    (cast(r.cotacao_indi as bigint)/100000000) fc_inv_cotacao_indi,
    cast(r.liqu_rgt as bigint) cod_inv_liqu_rgt,
    'RESCATE' ds_inv_tipo_oper,
    CONCAT(SUBSTRING(r.dtinput,1,4),'-',SUBSTRING(r.dtinput,5,2),'-',SUBSTRING(r.dtinput,7,2)) partition_date,
    row_number() over (partition by r.cod_cliente, r.cod_fdo, r.certificado, r.nr_oper order by r.partition_date desc ) as row_num
    from bi_corp_staging.fm_rescates r
    where r.partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones_hist') }}',30)
      and r.partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones_hist') }}') a
where a.row_num = 1;

-- Creo TMP_SUSCRIPCION_RESCATE_FONDOS
CREATE TEMPORARY TABLE IF NOT EXISTS TMP_SUSCRIPCION_RESCATE_FONDOS as
select
ts.cod_inv_fondo,
ts.cod_inv_cuenta,
ts.cod_inv_certificado,
ts.cod_inv_fondo_orig_dest,
ts.cod_inv_cli_orig_dest,
ts.cod_inv_canal_liq,
ts.cod_inv_agte,
ts.cod_inv_canal,
ts.cod_inv_agte_dg,
ts.cod_inv_canal_dg,
ts.cod_inv_oper_dg,
ts.cod_inv_agte_cd,
ts.cod_inv_canal_cd,
ts.cod_inv_oper_cd,
ts.ts_inv_input,
ts.dt_inv_solic,
ts.dt_inv_conver,
ts.dt_inv_estorno,
ts.cod_inv_forma_pago,
ts.cod_inv_tipo_cta,
ts.cod_inv_moneda_cta,
ts.ds_inv_tipo_cta,
ts.cod_inv_cta_num,
ts.fc_inv_qt_cot_rgt,
ts.cod_inv_cancelacion,
ts.cod_inv_moneda_cambio,
ts.fc_inv_conv_pact,
ts.cod_inv_operacion,
ts.cod_inv_cliente_ac,
ts.cod_inv_oper_orig,
ts.cod_inv_resg_orig,
ts.cod_inv_ctod_orig,
ts.dt_inv_ult_reg,
ts.dt_inv_bloqueio,
ts.cod_inv_bloqueio,
ts.cod_inv_dias_ut_dec,
ts.fc_inv_cota_base,
ts.fc_inv_apl_liq,
ts.fc_inv_qt_cot_apl,
ts.fc_inv_sd_cot_atu,
ts.fc_inv_cota_liq_d0,
ts.fc_inv_cotacao_pact,
ts.dt_inv_comprom,
ts.flag_inv_apl_transf,
ts.cod_inv_nr_rgt,
ts.cod_inv_nr_cert_dest,
ts.dt_inv_dtpagto,
ts.fc_inv_perc_comis,
ts.fc_inv_val_rgt_brt,
ts.fc_inv_val_rgt_liq,
ts.fc_inv_val_cust_rgt,
ts.fc_inv_val_cota_rgt,
ts.fc_inv_cotacao_indi,
ts.cod_inv_liqu_rgt,
ts.ds_inv_tipo_oper,
ts.partition_date
from TEMP_SUSCRIPCION ts
UNION ALL
select
tr.cod_inv_fondo,
tr.cod_inv_cuenta,
tr.cod_inv_certificado,
tr.cod_inv_fondo_orig_dest,
tr.cod_inv_cli_orig_dest,
tr.cod_inv_canal_liq,
tr.cod_inv_agte,
tr.cod_inv_canal,
tr.cod_inv_agte_dg,
tr.cod_inv_canal_dg,
tr.cod_inv_oper_dg,
tr.cod_inv_agte_cd,
tr.cod_inv_canal_cd,
tr.cod_inv_oper_cd,
tr.ts_inv_input,
tr.dt_inv_solic,
tr.dt_inv_conver,
tr.dt_inv_estorno,
tr.cod_inv_forma_pago,
tr.cod_inv_tipo_cta,
tr.cod_inv_moneda_cta,
tr.ds_inv_tipo_cta,
tr.cod_inv_cta_num,
tr.fc_inv_qt_cot_rgt,
tr.cod_inv_cancelacion,
tr.cod_inv_moneda_cambio,
tr.fc_inv_conv_pact,
tr.cod_inv_operacion,
tr.cod_inv_cliente_ac,
tr.cod_inv_oper_orig,
tr.cod_inv_resg_orig,
tr.cod_inv_ctod_orig,
tr.dt_inv_ult_reg,
tr.dt_inv_bloqueio,
tr.cod_inv_bloqueio,
tr.cod_inv_dias_ut_dec,
tr.fc_inv_cota_base,
tr.fc_inv_apl_liq,
tr.fc_inv_qt_cot_apl,
tr.fc_inv_sd_cot_atu,
tr.fc_inv_cota_liq_d0,
tr.fc_inv_cotacao_pact,
tr.dt_inv_comprom,
tr.flag_inv_apl_transf,
tr.cod_inv_nr_rgt,
tr.cod_inv_nr_cert_dest,
tr.dt_inv_dtpagto,
tr.fc_inv_perc_comis,
tr.fc_inv_val_rgt_brt,
tr.fc_inv_val_rgt_liq,
tr.fc_inv_val_cust_rgt,
tr.fc_inv_val_cota_rgt,
tr.fc_inv_cotacao_indi,
tr.cod_inv_liqu_rgt,
tr.ds_inv_tipo_oper,
tr.partition_date
from TEMP_RESCATE tr;

--- Inserto en Tabla Final
insert overwrite table bi_corp_common.bt_inv_suscripcion_rescate_fondos
partition(partition_date)
select
tso.cod_inv_sucursal_operativa,
f.cod_inv_cuenta,
t.nup cod_per_nup,
f.ds_inv_tipo_oper,
f.cod_inv_fondo,
df.ds_inv_fondo,
df.cod_inv_moneda,
df.cod_inv_isin,
df.cod_inv_cnv,
df.cod_inv_custodia,
f.cod_inv_certificado,
f.cod_inv_fondo_orig_dest,
f.cod_inv_cli_orig_dest,
f.cod_inv_canal_liq,
f.cod_inv_agte,
f.cod_inv_canal,
c.ds_inv_canal,
c.cod_inv_tipo_canal,
c.flag_inv_bp_canal,
coalesce(f.fc_inv_val_rgt_brt, f.fc_inv_apl_liq) fc_inv_monto_oper,
coalesce(f.fc_inv_qt_cot_rgt, f.fc_inv_qt_cot_apl) fc_inv_cuotas_oper,
coalesce(f.fc_inv_val_cota_rgt, f.fc_inv_cota_liq_d0) fc_inv_cotiz_oper,
f.cod_inv_agte_dg,
f.cod_inv_canal_dg,
f.cod_inv_oper_dg,
cdg.ds_inv_canal ds_inv_canal_dg,
f.cod_inv_agte_cd,
f.cod_inv_canal_cd,
f.cod_inv_oper_cd,
ccd.ds_inv_canal ds_inv_canal_cd,
f.ts_inv_input,
f.dt_inv_solic,
f.dt_inv_conver,
f.dt_inv_estorno,
f.cod_inv_forma_pago,
f.cod_inv_tipo_cta,
f.cod_inv_moneda_cta,
f.ds_inv_tipo_cta,
f.cod_inv_cta_num,
f.cod_inv_cancelacion,
f.cod_inv_moneda_cambio,
f.fc_inv_conv_pact,
f.cod_inv_operacion,
f.cod_inv_cliente_ac,
f.cod_inv_oper_orig,
f.cod_inv_resg_orig,
f.cod_inv_ctod_orig,
f.dt_inv_ult_reg,
f.dt_inv_bloqueio,
f.cod_inv_bloqueio,
f.cod_inv_dias_ut_dec,
f.fc_inv_cota_base,
f.fc_inv_sd_cot_atu,
f.fc_inv_cotacao_pact,
f.dt_inv_comprom,
f.flag_inv_apl_transf,
f.cod_inv_nr_rgt,
f.cod_inv_nr_cert_dest,
f.dt_inv_dtpagto,
f.fc_inv_perc_comis,
f.fc_inv_val_rgt_liq,
f.fc_inv_val_cust_rgt,
f.fc_inv_cotacao_indi,
f.cod_inv_liqu_rgt,
df.cod_inv_formato_fondo,
df.int_inv_carencia,
df.cod_inv_tipo_fondo,
f.partition_date
from TMP_SUSCRIPCION_RESCATE_FONDOS f
left join TEMP_NUPS t
on (f.cod_inv_cuenta = t.cod_cuenta)
left join bi_corp_common.dim_inv_codigos_fondos df
on (cast(df.cod_inv_fondo_depositaria as bigint) = f.cod_inv_fondo and
    df.partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones_hist') }}')
left join TEMP_SUC_OPERATIVA tso
on (f.cod_inv_cuenta = tso.cod_inv_cuenta)
left join bi_corp_common.dim_inv_canales_fondos c
on (f.cod_inv_canal = c.cod_inv_canal and f.cod_inv_agte = c.cod_inv_agente_colocador and
    c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones_hist') }}')
left join bi_corp_common.dim_inv_canales_fondos cdg
on (f.cod_inv_canal_dg = cdg.cod_inv_canal and f.cod_inv_agte_dg = cdg.cod_inv_agente_colocador and
    cdg.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones_hist') }}')
left join bi_corp_common.dim_inv_canales_fondos ccd
on (f.cod_inv_canal_cd = ccd.cod_inv_canal and f.cod_inv_agte_cd = ccd.cod_inv_agente_colocador and
    ccd.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones_hist') }}')
where f.partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones_hist') }}',7) and f.partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones_hist') }}';