"
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.stk_per_telefonos
PARTITION(partition_date)
SELECT
CAST(malpe_pedt023.penumper AS BIGINT) cod_per_nup
, CAST(trim(malpe_pedt023.pesectel) AS INT) ds_per_secuencia
, CAST(trim(malpe_pedt023.pesecdom) AS INT) ds_per_sec_dom
, CAST(trim(malpe_pedt023.petiptel) AS INT) cod_per_tipo_telefono
, CAST(trim(malpe_pedt023.peclatel) AS INT) cod_per_clase_telefono
, trim(malpe_pedt023.pepretel) ds_per_cod_area
, concat(trim(malpe_pedt023.pecartel),trim(malpe_pedt023.penumtel)) ds_per_nro_telefono
, trim(malpe_pedt023.penumint) ds_per_interno
, trim(malpe_pedt023.pecodcom) ds_per_compania
, trim(malpe_pedt023.fuente) ds_per_fuente
, nvl(CAST(trim(malpe_pedt023.secuencia) AS INT), 0) ds_per_sec_fuente
, trim(malpe_pedt023.estado) ds_per_estado
, nvl(CAST(trim(malpe_pedt023.pepriori) AS INT), 0) ds_per_semaforo
, malpe_pedt023.pefecalt dt_per_fecha_alta
, to_date(malpe_pedt023.pehstamp) dt_per_fecha_modificacion
, malpe_pedt023.partition_date partition_date
FROM bi_corp_staging.malpe_pedt023
WHERE malpe_pedt023.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_PERSONAS-Common-Julio') }}'
"