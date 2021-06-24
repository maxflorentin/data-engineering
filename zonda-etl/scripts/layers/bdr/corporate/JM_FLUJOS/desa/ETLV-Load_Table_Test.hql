set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_bdr.test_jm_flujos
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_flujos') }}')
SELECT r9746_feoperac, r9746_s1emp  , r9746_contra1 , r9746_fecmvto,
       r9746_clasmvto, r9746_importe, r9746_salonbal, 'Productivo' as origen
from bi_corp_bdr.jm_flujos
where partition_date = '2020-12';

---------------------------------------------------
---------------------------------------------------

CREATE TEMPORARY TABLE bi_corp_bdr.ctos_cancelados as
-- CONTRATOS CANCELADOS EN EL MES
SELECT cmap.id_cto_bdr, min(cast(date_add(core.PEFECEST, -1) as string)) as ult_act, min(core.PEFECEST) as fec_cancelacion
  FROM bi_corp_staging.malpe_pedt042 core
INNER JOIN bi_corp_bdr.normalizacion_id_contratos cmap
      ON core.pecdgent = cmap.cred_cod_entidad
     AND core.pecodofi = cmap.cred_cod_centro
     AND core.penumcon = cmap.cred_num_cuenta
     AND core.pecodpro = cmap.cred_cod_producto
     AND trim(core.pecodsub) = cmap.cred_cod_subprodu_altair
     AND cmap.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_flujos') }}'
LEFT JOIN bi_corp_bdr.jm_trz_cont_ren ren
      ON ren.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_flujos') }}'
     AND ren.g7025_cont_ant = cmap.id_cto_bdr
     AND ren.G7025_MOTRENU not in ('00020','00026', '00029', '00030') --Código de renumeraciones que se den de alta! (Para covid) Motivo de renumeración
 WHERE core.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt042', dag_id='BDR_flujos') }}'
   AND core.PEESTOPE = 'C'
   AND ren.g7025_cont_ant is null
   AND concat(substr(core.PEFECEST,1,4),substr(core.PEFECEST,6,2)) = '{{ ti.xcom_pull(task_ids='InputConfig', key='month', dag_id='BDR_flujos') }}'
group by cmap.id_cto_bdr;

---------------------------------------------------
---------------------------------------------------

WITH canceladosPrimerDia AS ( --solo me quedo con los cancelados el 1/12
  SELECT c.id_cto_bdr
	FROM bi_corp_bdr.ctos_cancelados c
	WHERE c.fec_cancelacion = '2020-12-01' --'${primerDiaDeEsteMes}'
),
flujosMesAnterior AS ( --todos los flujos de NOVIEMBRE ordenados por fecha de movimiento
	SELECT row_number() over(partition by fl.r9746_contra1 order by fl.r9746_fecmvto desc) AS orden, fl.*
	FROM bi_corp_bdr.jm_flujos fl
	WHERE fl.partition_date = '2020-11' --'${mesAnterior}'
),
ultimosFlujosNormalesMesAnterior AS ( --de c/ contrato solo me quedo con el movimiento mas actual y NORMAL
	SELECT *
	FROM flujosMesAnterior fl
	WHERE orden = 1
		AND fl.r9746_clasmvto = '00009' --'${normal}'
),
diferencia AS ( --los que fueron cancelados el 1/12 y a su vez su ultimo flujo de NOVIEMBRE fue NORMAL
	SELECT b.*
	FROM canceladosPrimerDia a 
		INNER JOIN ultimosFlujosNormalesMesAnterior b
			ON a.id_cto_bdr = b.r9746_contra1
)
insert INTO table bi_corp_bdr.test_jm_flujos
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_flujos') }}')
SELECT r9746_feoperac,r9746_s1emp,r9746_contra1,r9746_fecmvto,
       r9746_clasmvto,r9746_importe,r9746_salonbal,'Desarrollo' as origen
FROM diferencia;