"
SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Estos son de altair solamente

INSERT OVERWRITE TABLE bi_corp_common.dim_pro_producto
    PARTITION (partition_date)
SELECT
concat(substr(tcdt.gen_clave,1,2),'_',substr(tcdt.gen_clave,3,4)) cod_pro_claveproducto,
substr(tcdt.gen_clave,1,2) as cod_pro_producto,
substr(tcdt.gen_clave,3,4) as cod_pro_subproducto,
trim(substr(tcdt.gen_Datos,20,40)) as ds_pro_producto,
tcdt.partition_date partition_date
from bi_Corp_staging.tcdtgen tcdt
where gentabla = '0309' and partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}'
order by 1,2

"