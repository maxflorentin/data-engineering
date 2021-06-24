"
SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;


INSERT OVERWRITE TABLE bi_corp_common.dim_suc_sucursales
    PARTITION (partition_date)
SELECT
cod_centro cod_suc_sucursal,
desc_comp_centro_op des_suc_sucursal,
TRANSLATE(concat (TRIM(direc_comp_centro_op), '- ', cod_postal_centro_op , '- ' , cod_localidad, '�', '#')) des_suc_localidad,
tipo_centro cod_suc_tiposucursal,
fec_alta_centro dt_suc_fechaalta,
fec_baja_centro dt_suc_fechabaja,
partition_date
from bi_corp_staging.tcdt050
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}'


"