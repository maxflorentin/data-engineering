set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

-- tabla temporal que genera el perímetro de elementos a mapear en la normalizacion.
CREATE TEMPORARY TABLE perimetro_contratos AS
SELECT distinct 'Tactico-Mitre' source,     tm.contrato as id_cto_source, 
                '' cred_cod_entidad,        '' cred_cod_centro,          '' cred_num_cuenta, 
                '' cred_cod_producto,       '' cred_cod_subprodu_altair, '' mmff_cod_especie, 
                '' mmff_cod_disponibilidad, '' mmff_cod_sis_origen,      '' mmff_cod_cartera, 
                '' mmff_cod_operacion,      '' mmff_cod_pata,            '' blc_sociedad, 
                '' blc_cargabal,            '' blc_sociedad_eliminacion
from bi_corp_bdr.tactico_mitre_contratos tm;

CREATE TEMPORARY TABLE max_id_cto AS
SELECT COALESCE(MAX(cast(id_cto_bdr as BIGINT)), 0) as max_id_cto
FROM bi_corp_bdr.test_normalizacion_id_contratos_history;

INSERT INTO TABLE bi_corp_bdr.test_normalizacion_id_contratos_history
SELECT lpad(CAST(y.max_id_cto AS BIGINT) + z.rownum, 9, '0') as id_cto_bdr,
       z.source,                   z.id_cto_source,    z.cred_cod_entidad,
       z.cred_cod_centro,          z.cred_num_cuenta,  z.cred_cod_producto,
       z.cred_cod_subprodu_altair, z.mmff_cod_especie, z.mmff_cod_disponibilidad,
       z.mmff_cod_sis_origen,      z.mmff_cod_cartera, z.mmff_cod_operacion,
       z.mmff_cod_pata,            z.blc_sociedad,     z.blc_cargabal,
       z.blc_sociedad_eliminacion, current_date as inserted_date
FROM (
  SELECT x.*, ROW_NUMBER() OVER() AS rownum
  FROM (
    SELECT p.*
    FROM perimetro_contratos p
      LEFT JOIN bi_corp_bdr.test_normalizacion_id_contratos_history h 
        ON h.id_cto_source = p.id_cto_source
    WHERE h.id_cto_source is null
  ) x
) z, max_id_cto y;

INSERT OVERWRITE TABLE bi_corp_bdr.test_normalizacion_id_contratos
PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tactico_Mitre') }}')
--Los que ya estaban en la normalizacion este mes
SELECT n.id_cto_bdr,              n.source,                   n.id_cto_source,
       n.cred_cod_entidad,        n.cred_cod_centro,          n.cred_num_cuenta,
       n.cred_cod_producto,       n.cred_cod_subprodu_altair, n.mmff_cod_especie,
       n.mmff_cod_disponibilidad, n.mmff_cod_sis_origen,      n.mmff_cod_cartera,
       n.mmff_cod_operacion,      n.mmff_cod_pata,            n.blc_sociedad,
       n.blc_cargabal,            n.blc_sociedad_eliminacion
FROM bi_corp_bdr.test_normalizacion_id_contratos n
WHERE n.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tactico_Mitre') }}'
  and n.source != 'Tactico-Mitre'
UNION ALL
--Los nuevos correspondientes a Tactico Mitre
SELECT hs.id_cto_bdr,              hs.source,                   hs.id_cto_source,
       hs.cred_cod_entidad,        hs.cred_cod_centro,          hs.cred_num_cuenta,
       hs.cred_cod_producto,       hs.cred_cod_subprodu_altair, hs.mmff_cod_especie,
       hs.mmff_cod_disponibilidad, hs.mmff_cod_sis_origen,      hs.mmff_cod_cartera,
       hs.mmff_cod_operacion,      hs.mmff_cod_pata,            hs.blc_sociedad,
       hs.blc_cargabal,            hs.blc_sociedad_eliminacion
FROM (
  SELECT DISTINCT pc.id_cto_source 
  FROM perimetro_contratos pc
) ds
  LEFT JOIN bi_corp_bdr.test_normalizacion_id_contratos_history hs 
    ON hs.id_cto_source = ds.id_cto_source;