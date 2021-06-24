set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

-- Calculo la maxima particion PEDT042
DROP TABLE IF EXISTS tmp_maxpart042;
CREATE TEMPORARY TABLE tmp_maxpart042 AS
select max(partition_date) AS partition_date
from bi_corp_Staging.malpe_ptb_pedt042
where partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones') }}',7)
  and partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones') }}'
  and pecdgent = '0072' and pecodpro = '60' and pecodmon = 'ARS';

-- Elimino los  duplicados de PEDT042
DROP TABLE IF EXISTS TEMP_SUC_OPERATIVA;
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_SUC_OPERATIVA as
SELECT distinct
    cast(p.pesucope as int) cod_inv_sucursal_operativa,
    cast(p.penumcon as int) cod_inv_cuenta
FROM bi_corp_staging.malpe_ptb_pedt042 p
INNER JOIN tmp_maxpart042 mx42 on (p.partition_date=mx42.partition_date)
WHERE p.pecdgent = '0072' -- (Santander)
    AND p.pecodpro = '60'   -- (Producto Fondos)
    AND p.pecodmon = 'ARS';

--------------Calculo la maxima particion PEDT008
DROP TABLE IF EXISTS tmp_maxpart008;
CREATE TEMPORARY TABLE tmp_maxpart008 AS
select max(partition_date) AS partition_date
  from bi_corp_Staging.malpe_pedt008
 where partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones') }}',7)
  and partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones') }}'
   and pecodpro = '60' and pecalpar = 'TI'
   and pecdgent = '0072';

DROP TABLE IF EXISTS tmp_pedt008;
CREATE TEMPORARY TABLE tmp_pedt008 AS
select cast(contrato as bigint) contrato,
       cast(sucursal as bigint) sucursal,
       cast(nup as bigint) nup
  from (
        select p.penumcon contrato,p.pecodofi sucursal ,p.penumper nup,ROW_NUMBER () OVER ( partition by P.penumcon, P.pecodofi, P.pecodpro, P.pecodsub ORDER BY P.peordpar ) as orden
        from bi_corp_Staging.malpe_pedt008 p
        INNER JOIN tmp_maxpart008 mx8 on p.partition_date=mx8.partition_date
        where pecodpro = '60' and pecalpar = 'TI'
          and pecdgent = '0072'
       ) x
 where orden = 1;

insert overwrite table bi_corp_common.stk_inv_titulos
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones') }}')
select t.cod_inv_sucursal_operativa as cod_inv_sucursal_operativa,
       cast(securities_contract_code as int) cod_inv_cuenta,
       p.nup cod_per_nup,
       case when trim(assetid)  = 'null' then NULL else trim(assetid) end cod_inv_especie,
       de.ds_inv_tipo_especie,
       de.ds_inv_especie,
       de.cod_inv_moneda_emision,
       cast(nominals as decimal(25,7)) cod_inv_cantidad,
       cast(asset_price as decimal(25,7)) cod_inv_precio,
       (cast(nominals as decimal(25,7)) * cast(asset_price as decimal(25,7))) cod_inv_importe,
       de.cod_inv_periodicidad_pago,
       de.cod_inv_producto,
       case when trim(holding_status)  = 'null' then NULL else trim(holding_status) end cod_inv_estado_tenencia,
       case when trim(place_holding)  = 'null' then NULL else trim(place_holding) end cod_inv_lugar_titulos
from bi_corp_staging.rio347_balance b
left join bi_corp_common.dim_inv_especies_titulos de
on (trim(b.assetid) = de.cod_inv_especie and
    de.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones') }}')
left join TEMP_SUC_OPERATIVA t on (t.cod_inv_cuenta = cast(b.securities_contract_code as int))
left join tmp_pedt008 p on (p.contrato = cast(b.securities_contract_code as int))
where b.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones') }}';