set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS max_wasdo12;
CREATE TEMPORARY TABLE max_wasdo12 AS
SELECT max(w.partition_date) AS partition_date
FROM bi_corp_staging.afma_wasdo12 w
WHERE w.partition_date > date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_AFMA_Updates-Daily') }}',7)
and w.partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_AFMA_Updates-Daily') }}';

DROP TABLE IF EXISTS wasdo12_stock_tmp;
create temporary table wasdo12_stock_tmp as
SELECT w12.wasdo12_tipo_cuenta,
w12.wasdo12_nro_cuenta,
w12.wasdo12_f_pago_aa1,
w12.wasdo12_f_pago_aa2,
w12.wasdo12_f_pago_mm,
w12.wasdo12_f_pago_dd,
w12.wasdo12_nro_seq,
w12.wasdo12_importe,
w12.wasdo12_empresa,
w12.wasdo12_suc_recaud,
w12.wasdo12_origen_mov,
w12.wasdo12_moneda,
w12.wasdo12_nro_comprobante,
w12.wasdo12_estado,
w12.wasdo12_estado_fecha,
w12.wasdo12_cartera,
w12.wasdo12_ident_canal,
w12.wasdo12_ident_fecha,
w12.wasdo12_ident_nro,
w12.wasdo12_nro_rel_simul,
w12.wasdo12_ind_pago_efectivo,
w12.wasdo12_ind_boleto_online,
w12.wasdo12_ind_pago_boleto,
w12.partition_date
FROM bi_corp_staging.afma_wasdo12 w12
INNER JOIN max_wasdo12 mw ON mw.partition_date = w12.partition_date
WHERE w12.partition_date > date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_AFMA_Updates-Daily') }}',7)
and w12.partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_AFMA_Updates-Daily') }}'

union all

SELECT wasdo12_tipo_cuenta,
wasdo12_nro_cuenta,
wasdo12_f_pago_aa1,
wasdo12_f_pago_aa2,
wasdo12_f_pago_mm,
wasdo12_f_pago_dd,
wasdo12_nro_seq,
wasdo12_importe,
wasdo12_empresa,
wasdo12_suc_recaud,
wasdo12_origen_mov,
wasdo12_moneda,
wasdo12_nro_comprobante,
wasdo12_estado,
wasdo12_estado_fecha,
wasdo12_cartera,
wasdo12_ident_canal,
wasdo12_ident_fecha,
wasdo12_ident_nro,
wasdo12_nro_rel_simul,
wasdo12_ind_pago_efectivo,
wasdo12_ind_boleto_online,
wasdo12_ind_pago_boleto,
partition_date
FROM bi_corp_staging.afma_wasdo12_stock
WHERE '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_AFMA_Updates-Daily') }}'='2020-10-15' AND partition_date='2020-10-15'
and wasdo12_nro_seq is not null;

DROP TABLE IF EXISTS wasdo12_update_tmp;
create TEMPORARY table wasdo12_update_tmp as
SELECT * FROM bi_corp_staging.afma_wasdo12_updates where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_AFMA_Updates-Daily') }}';

INSERT OVERWRITE TABLE bi_corp_staging.afma_wasdo12
PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_AFMA_Updates-Daily') }}')
SELECT
    COALESCE(b.wasdo12_tipo_cuenta,a.wasdo12_tipo_cuenta)         as tipo_cuenta,
    COALESCE(b.wasdo12_nro_cuenta,a.wasdo12_nro_cuenta)         as nro_cuenta,
    COALESCE(b.wasdo12_f_pago_aa1,a.wasdo12_f_pago_aa1)         as f_pago_aa1,
    COALESCE(b.wasdo12_f_pago_aa2,a.wasdo12_f_pago_aa2)         as f_pago_aa2,
    COALESCE(b.wasdo12_f_pago_mm,a.wasdo12_f_pago_mm)         as f_pago_mm,
    COALESCE(b.wasdo12_f_pago_dd,a.wasdo12_f_pago_dd)         as f_pago_dd,
    COALESCE(b.wasdo12_nro_seq,a.wasdo12_nro_seq)         as nro_seq,
    COALESCE(b.wasdo12_importe,a.wasdo12_importe)         as importe,
    COALESCE(b.wasdo12_empresa,a.wasdo12_empresa)         as empresa,
    COALESCE(b.wasdo12_suc_recaud,a.wasdo12_suc_recaud)         as suc_recaud,
    COALESCE(b.wasdo12_origen_mov,a.wasdo12_origen_mov)         as origen_mov,
    COALESCE(b.wasdo12_moneda,a.wasdo12_moneda)         as moneda,
    COALESCE(b.wasdo12_nro_comprobante,a.wasdo12_nro_comprobante)         as nro_comprobante,
    COALESCE(b.wasdo12_estado,a.wasdo12_estado)         as estado,
    COALESCE(b.wasdo12_estado_fecha,a.wasdo12_estado_fecha)         as estado_fecha,
    COALESCE(b.wasdo12_cartera,a.wasdo12_cartera)         as cartera,
    COALESCE(b.wasdo12_ident_canal,a.wasdo12_ident_canal)         as ident_canal,
    COALESCE(b.wasdo12_ident_fecha,a.wasdo12_ident_fecha)         as ident_fecha,
    COALESCE(b.wasdo12_ident_nro,a.wasdo12_ident_nro)         as ident_nro,
    COALESCE(b.wasdo12_nro_rel_simul,a.wasdo12_nro_rel_simul)         as nro_rel_simul,
    COALESCE(b.wasdo12_ind_pago_efectivo,a.wasdo12_ind_pago_efectivo)         as ind_pago_efectivo,
    COALESCE(b.wasdo12_ind_boleto_online,a.wasdo12_ind_boleto_online)         as ind_boleto_online,
    COALESCE(b.wasdo12_ind_pago_boleto,a.wasdo12_ind_pago_boleto)         as ind_pago_boleto
from wasdo12_stock_tmp a
full outer join wasdo12_update_tmp b on
(a.wasdo12_tipo_cuenta = b.wasdo12_tipo_cuenta
and a.wasdo12_nro_cuenta = b.wasdo12_nro_cuenta
and a.wasdo12_f_pago_aa1 = b.wasdo12_f_pago_aa1
and a.wasdo12_f_pago_aa2 = b.wasdo12_f_pago_aa2
and a.wasdo12_f_pago_mm = b.wasdo12_f_pago_mm
and a.wasdo12_f_pago_dd = b.wasdo12_f_pago_dd
and a.wasdo12_nro_seq = b.wasdo12_nro_seq);