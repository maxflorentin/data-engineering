set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

-- Calculo la maxima particion PEDT042
CREATE TEMPORARY TABLE tmp_maxpart042 AS
select max(partition_date) AS partition_date
from bi_corp_Staging.malpe_ptb_pedt042
where partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones_hist') }}',7)
  and partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones_hist') }}'
  and pecdgent = '0072' and pecodpro = '60' and pecodmon = 'ARS';

-- Elimino los  duplicados de PEDT042
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_SUC_OPERATIVA as
SELECT distinct
    cast(p.pesucope as int) cod_inv_sucursal_operativa,
    cast(p.penumcon as int) cod_inv_cuenta
FROM bi_corp_staging.malpe_ptb_pedt042 p
INNER JOIN tmp_maxpart042 mx42 on (p.partition_date=mx42.partition_date)
WHERE p.pecdgent = '0072' -- (Santander)
    AND p.pecodpro = '60'   -- (Producto Fondos)
    AND p.pecodmon = 'ARS';


insert overwrite table bi_corp_common.stk_inv_fondos
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones_hist') }}')
select
    cod_inv_sucursal_operativa as cod_inv_sucursal_operativa,
    cast(s.cta as int) as cod_inv_cuenta,
    cast(s.penumper as int) as cod_per_nup,
    s.razon as ds_inv_razon_social,
    cast(s.cod_fondo as int) as cod_inv_fondo,
    f.ds_inv_fondo as ds_inv_fondo,
    cast(s.saldo_cuotas as bigint)/10000 as fc_inv_cuotapartes,
    cast(s.sal_cuot_bloq as bigint)/10000 as fc_inv_cuotapartes_bloq,
    cast(s.impo_moneda_origen as bigint)/(cast(s.saldo_cuotas as bigint)/100) as fc_inv_cotiz_fondo,
    f.cod_inv_moneda as cod_inv_moneda,
    cast(s.impo_moneda_local as bigint)/100 as fc_inv_impo_moneda_local,
    cast(s.impo_moneda_origen as bigint)/100 as fc_inv_impo_moneda_origen,
    cast(s.cotiz_me as bigint)/100 as fc_inv_cotiz_moneda,
    f.cod_inv_id_sociedad_gerente as cod_inv_id_sociedad_gerente,
    f.cod_inv_fondo_gerente as cod_inv_fondo_gerente,
    f.cod_inv_fondo_clase as cod_inv_fondo_clase,
    f.cod_inv_isin as cod_inv_isin,
    f.cod_inv_cnv as cod_inv_cnv,
    f.cod_inv_custodia as cod_inv_custodia,
    f.cod_inv_formato_fondo as cod_inv_formato_fondo,
    f.int_inv_carencia as int_inv_carencia,
    f.cod_inv_tipo_fondo as cod_inv_tipo_fondo
from bi_corp_staging.fm_saldos s
left join bi_corp_common.dim_inv_codigos_fondos f on f.cod_inv_fondo_depositaria = cast(s.cod_fondo as int) and f.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones_hist') }}'
left join TEMP_SUC_OPERATIVA t on t.cod_inv_cuenta = cast(s.cta as int)
where s.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones_hist') }}';