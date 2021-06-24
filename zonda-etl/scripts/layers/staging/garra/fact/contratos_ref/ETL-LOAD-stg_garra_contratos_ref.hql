set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS max_garra_contratos_ref;
CREATE TEMPORARY TABLE max_garra_contratos_ref AS
SELECT max(cf.partition_date) AS partition_date
FROM bi_corp_staging.garra_contratos_ref cf
WHERE cf.partition_date > date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_GARRA_Updates-Daily') }}',7)
and cf.partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_GARRA_Updates-Daily') }}';


DROP TABLE IF EXISTS contratos_ref_stock_tmp;
create temporary table contratos_ref_stock_tmp as
SELECT 086_cod_entidad,
086_cod_centro,
086_num_contrato,
086_cod_producto,
086_cod_subprodu,
086_cod_divisa,
086_cod_centrod,
086_num_contratd,
086_cod_productd,
086_cod_subprodd,
086_cod_divisad,
086_imp_refnacdo,
086_imp_refnacdl,
086_imp_cbiomorg,
086_fec_refinanc,
086_cod_reesctr,
086_ind_antconint,
086_imp_anticipo,
086_cod_marca_seg_esp,
086_cod_marca,
086_cod_submarca,
086_fec_primer_imp,
086_num_persona,
086_ws_marca_reestruc,
cr.partition_date
FROM bi_corp_staging.garra_contratos_ref cr
INNER JOIN max_garra_contratos_ref mcr ON mcr.partition_date = cr.partition_date
WHERE cr.partition_date > date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_GARRA_Updates-Daily') }}',7)
and cr.partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_GARRA_Updates-Daily') }}'

union all

SELECT trim(s.086_entidad) as 086_cod_entidad,
trim(s.086_centro) as 086_cod_centro,
trim(s.086_contrato) as 086_num_contrato,
trim(s.086_producto) as 086_cod_producto,
trim(s.086_subproducto) as 086_cod_subprodu,
trim(s.086_divisa) as 086_cod_divisa,
trim(s.086_centrod) as 086_cod_centrod,
trim(s.086_contratod) as 086_num_contratd,
trim(s.086_productod) as 086_cod_productd,
trim(s.086_subproductod) as 086_cod_subprodd,
trim(s.086_divisad) as 086_cod_divisad,
cast(s.086_imp_refnacdo as decimal(13,4)) as 086_imp_refnacdo,
cast(s.086_imp_refnacdl as decimal(13,4)) as 086_imp_refnacdl,
cast(s.086_imp_cbiomorg as decimal(13,4)) as 086_imp_cbiomorg,
trim(s.086_fec_refinan) as 086_fec_refinanc,
trim(s.086_tipo_restructuracion) as 086_cod_reesctr,
trim(s.086_int_completo) as 086_ind_antconint,
cast(s.086_imp_intereses as decimal(11,2)) as 086_imp_anticipo,
trim(s.086_cod_marca_seg_esp) as 086_cod_marca_seg_esp,
trim(s.086_marca) as 086_cod_marca,
trim(s.086_submarca) as 086_cod_submarca,
trim(s.086_fec_sit_irr) as 086_fec_primer_imp,
trim(s.086_nup) as 086_num_persona,
trim(s.086_marca_reestruc) as 086_ws_marca_reestruc,
s.partition_date
FROM bi_corp_staging.garra_contratos_ref_stock s
WHERE '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_GARRA_Updates-Daily') }}'='2020-10-02' AND s.partition_date='2020-10-01';

DROP TABLE IF EXISTS contratos_ref_update_tmp;
create TEMPORARY table contratos_ref_update_tmp as
SELECT * FROM bi_corp_staging.garra_contratos_ref_updates where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_GARRA_Updates-Daily') }}';

INSERT OVERWRITE TABLE bi_corp_staging.garra_contratos_ref
PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_GARRA_Updates-Daily') }}')
SELECT
COALESCE(b.086_cod_entidad,a.086_cod_entidad) as 086_cod_entidad,
COALESCE(b.086_cod_centro,a.086_cod_centro) as 086_cod_centro,
COALESCE(b.086_num_contrato,a.086_num_contrato) as 086_num_contrato,
COALESCE(b.086_cod_producto,a.086_cod_producto) as 086_cod_producto,
COALESCE(b.086_cod_subprodu,a.086_cod_subprodu) as 086_cod_subprodu,
COALESCE(b.086_cod_divisa,a.086_cod_divisa) as 086_cod_divisa,
COALESCE(b.086_cod_centrod,a.086_cod_centrod) as 086_cod_centrod,
COALESCE(b.086_num_contratd,a.086_num_contratd) as 086_num_contratd,
COALESCE(b.086_cod_productd,a.086_cod_productd) as 086_cod_productd,
COALESCE(b.086_cod_subprodd,a.086_cod_subprodd) as 086_cod_subprodd,
COALESCE(b.086_cod_divisad,a.086_cod_divisad) as 086_cod_divisad,
COALESCE(b.086_imp_refnacdo,a.086_imp_refnacdo) as 086_imp_refnacdo,
COALESCE(b.086_imp_refnacdl,a.086_imp_refnacdl) as 086_imp_refnacdl,
COALESCE(b.086_imp_cbiomorg,a.086_imp_cbiomorg) as 086_imp_cbiomorg,
COALESCE(b.086_fec_refinanc,a.086_fec_refinanc) as 086_fec_refinanc,
COALESCE(b.086_cod_reesctr,a.086_cod_reesctr) as 086_cod_reesctr,
COALESCE(b.086_ind_antconint,a.086_ind_antconint) as 086_ind_antconint,
COALESCE(b.086_imp_anticipo,a.086_imp_anticipo) as 086_imp_anticipo,
COALESCE(b.086_cod_marca_seg_esp,a.086_cod_marca_seg_esp) as 086_cod_marca_seg_esp,
COALESCE(b.086_cod_marca,a.086_cod_marca) as 086_cod_marca,
COALESCE(b.086_cod_submarca,a.086_cod_submarca) as 086_cod_submarca,
COALESCE(b.086_fec_primer_imp,a.086_fec_primer_imp) as 086_fec_primer_imp,
COALESCE(b.086_num_persona,a.086_num_persona) as 086_num_persona,
COALESCE(b.086_ws_marca_reestruc,a.086_ws_marca_reestruc) as 086_ws_marca_reestruc
from contratos_ref_stock_tmp a
full outer join contratos_ref_update_tmp b on
a.086_cod_entidad = b.086_cod_entidad
AND a.086_cod_centro = b.086_cod_centro
AND a.086_num_contrato = b.086_num_contrato
AND a.086_cod_producto = b.086_cod_producto
AND a.086_cod_subprodu = b.086_cod_subprodu
AND a.086_cod_divisa = b.086_cod_divisa
AND a.086_cod_centrod = b.086_cod_centrod
AND a.086_num_contratd = b.086_num_contratd
AND a.086_cod_productd = b.086_cod_productd
AND a.086_cod_subprodd = b.086_cod_subprodd
AND a.086_cod_divisad = b.086_cod_divisad;
