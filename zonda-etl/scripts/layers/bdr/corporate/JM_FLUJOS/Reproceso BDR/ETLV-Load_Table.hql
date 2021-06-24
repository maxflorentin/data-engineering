set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

CREATE TEMPORARY TABLE bi_corp_bdr.contratos_utp AS
-- PERSONAS CON CONTRATOS CON FECHAS UTP EN EL PERIODO
SELECT DISTINCT lpad(bb.num_persona, 9, '0') as num_persona
  FROM
       (
        SELECT DISTINCT cg.num_persona
          FROM bi_corp_bdr.jm_contr_otros a
        INNER JOIN bi_corp_bdr.normalizacion_id_contratos b
               ON a.e0623_contra1 = b.id_cto_bdr
              AND b.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
        INNER JOIN santander_business_risk_arda.contratos_garra cg
               ON cg.cod_entidad = b.cred_cod_entidad
              AND cg.cod_centro = b.cred_cod_centro
              AND cg.num_cuenta = b.cred_num_cuenta
              AND cg.cod_producto = b.cred_cod_producto
              AND cg.cod_subprodu = b.cred_cod_subprodu_altair
        WHERE a.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
          AND ( e0623_finiutct not in ('0001-01-01', '9999-12-31') or  e0623_ffinutct not in ('0001-01-01', '9999-12-31') )
          AND cg.data_date_part between      '{{ ti.xcom_pull(task_ids='InputConfig', key='first_working_day_arda', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
                                         and '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
       ) bb;


CREATE TEMPORARY TABLE bi_corp_bdr.clientes_utp AS
-- PERSONAS CON FECHAS UTP EN EL PERIODO
SELECT DISTINCT g4093_idnumcli as num_persona
  FROM bi_corp_bdr.jm_client_bii
 WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
   AND (g4093_finiutcl not in ('0001-01-01', '9999-12-31') or g4093_ffinutcl not in ('0001-01-01', '9999-12-31'));


CREATE TEMPORARY TABLE bi_corp_bdr.flujos_cierre_ant AS
-- PERSONAS QUE EN EL FINAL DEL PERIODO ANTERIOR TIENEN UN MOVIMIENTO 9
SELECT DISTINCT lpad(cg.num_persona, 9, '0') as num_persona
  FROM bi_corp_bdr.jm_flujos fl
INNER JOIN (
            SELECT r9746_contra1, max(r9746_fecmvto) as ult_mov
              FROM bi_corp_bdr.jm_flujos
             WHERE PARTITION_DATE = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
            GROUP BY r9746_contra1
           ) fl_fil
        ON fl_fil.r9746_contra1 = fl.r9746_contra1
       and fl_fil.ult_mov = fl.r9746_fecmvto

INNER JOIN bi_corp_bdr.normalizacion_id_contratos nc
        ON nc.id_cto_bdr = fl.r9746_contra1
       AND nc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'

INNER JOIN santander_business_risk_arda.contratos_garra cg
        ON cg.cod_entidad = nc.cred_cod_entidad
       AND cg.cod_centro = nc.cred_cod_centro
       AND cg.num_cuenta = nc.cred_num_cuenta
       AND cg.cod_producto = nc.cred_cod_producto
       AND cg.cod_subprodu = nc.cred_cod_subprodu_altair
       AND cg.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_last_working_day_arda', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'

 WHERE fl.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
   AND fl.r9746_clasmvto = '00009';

CREATE TEMPORARY TABLE bi_corp_bdr.final_utp AS
-- TODAS LAS PERSONAS QUE DEBEN INCORPORARSE A FLUJOS
SELECT distinct tt.num_persona
  FROM
      (
-- PERSONAS CON FECHAS UTP EN EL PERIODO
       SELECT num_persona FROM bi_corp_bdr.clientes_utp
       UNION ALL
-- PERSONAS CON CONTRATOS CON FECHAS UTP EN EL PERIODO
       SELECT num_persona FROM bi_corp_bdr.contratos_utp
       UNION ALL
       SELECT num_persona from bi_corp_bdr.flujos_cierre_ant
       UNION ALL
-- PERSONAS CON CONTRATOS CON IMPORTE IRREGULAR > 0
       select lpad(num_persona, 9, '0') as num_persona
       from santander_business_risk_arda.contratos_garra
       where data_date_part between     '{{ ti.xcom_pull(task_ids='InputConfig', key='first_working_day_arda', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
                                    and '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
         and imp_irregular_total_moneda_local > 0
      ) tt;


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
     AND cmap.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
LEFT JOIN bi_corp_bdr.jm_trz_cont_ren ren
      ON ren.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
     AND ren.g7025_cont_ant = cmap.id_cto_bdr
     AND ren.G7025_MOTRENU not in ('00020','00060', '00061', '00062','00063','00064','00065','00066','00067') --Código de renumeraciones que se den de alta! (Para covid) Motivo de renumeración
 WHERE core.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt042', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
   AND core.PEESTOPE = 'C'
   AND ren.g7025_cont_ant is null
   AND concat(substr(core.PEFECEST,1,4),substr(core.PEFECEST,6,2)) = '{{ ti.xcom_pull(task_ids='InputConfig', key='month', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
group by cmap.id_cto_bdr;


CREATE TEMPORARY TABLE bi_corp_bdr.ctos_fallidos as
-- CONTRATOS CASTIGADOS
SELECT cod_entidad, cod_centro, num_cuenta, cod_producto, cod_subprodu_altair, cod_marca, min(fec_castigo) fec_castigo
  FROM santander_business_risk_arda.contratos
 WHERE data_date_part between     '{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day_arda', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
                              and '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
   AND cod_marca = 'FA'
GROUP BY cod_entidad, cod_centro, num_cuenta, cod_producto, cod_subprodu_altair, cod_marca;


CREATE TEMPORARY TABLE bi_corp_bdr.baremos as
-- BAREMOS DE EMPRESA
SELECT empresa, cod_baremo_local, cod_baremo_global
  FROM bi_corp_bdr.map_baremos_global_local
 WHERE cod_negocio_local = '1'
   AND '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
                      between fecha_desde and fecha_hasta;


CREATE TEMPORARY TABLE bi_corp_bdr.cto_perimetro as
--PERIMETROS DE CONTRATOS SEGUN NORMALIZACION
SELECT m.id_cto_bdr, m.id_cto_source, m.cred_cod_entidad as cod_entidad, m.cred_cod_centro as cod_centro,
       m.cred_num_cuenta as num_cuenta, m.cred_cod_producto as cod_producto, m.cred_cod_subprodu_altair as cod_subprodu
  FROM bi_corp_bdr.normalizacion_id_contratos m
 WHERE m.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
   AND m.source = 'credito';


insert overwrite table bi_corp_bdr.jm_flujos
-- INSERT DE MOVIMIENTOS EN FLUJOS NORMALES (9) O MARCAS DE FALLIDO (4)
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
SELECT
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' AS R9746_FEOPERAC,
lpad(nvl(ble.cod_baremo_global,'0'),5,'0') as R9746_S1EMP,
lpad(ncto.id_cto_bdr,9,"0") as R9746_CONTRA1,
to_date(from_unixtime(UNIX_TIMESTAMP(core_fimp.data_date_part,"yyyyMMdd"))) AS R9746_FECMVTO,
CASE WHEN confa.cod_marca = 'FA' AND confa.fec_castigo = to_date(from_unixtime(UNIX_TIMESTAMP(core_fimp.data_date_part,"yyyyMMdd")))
      THEN lpad("4",5,"0")
     ELSE lpad("9",5,"0") END as R9746_CLASMVTO,
concat('-',lpad(regexp_replace(format_number(SUM(IMP_IRREGULAR_TOTAL_MONEDA_LOCAL), 2),"\\,|\\.",""),16,"0")) as R9746_IMPORTE,
concat('-',lpad(regexp_replace(format_number(SUM(IMP_RIESGO_DIVISA_LOCAL_DEL_CONTRATO), 2),"\\,|\\.",""),16,"0")) as R9746_SALONBAL

-- FUENTE DE CONTRATOS DE FLUJO
  FROM santander_business_risk_arda.contratos_garra core_fimp

-- TODAS LAS PERONAS QUE DEBEN INFORMARSE EN FLUJOS
INNER JOIN bi_corp_bdr.final_utp filt_fimp
     on lpad(core_fimp.num_persona, 9, '0') = filt_fimp.num_persona

-- PERIMETROS DE CONTRATOS SEGUN NORMALIZACION
INNER JOIN bi_corp_bdr.cto_perimetro ncto
        ON ncto.cod_entidad = core_fimp.cod_entidad
       AND ncto.cod_centro = core_fimp.cod_centro
       AND ncto.num_cuenta = core_fimp.num_cuenta
       AND ncto.cod_producto = core_fimp.cod_producto
       AND ncto.cod_subprodu = core_fimp.cod_subprodu

-- CONTRATOS FALLIDOS
LEFT JOIN bi_corp_bdr.ctos_fallidos confa
       ON confa.cod_entidad = core_fimp.cod_entidad
      AND confa.cod_centro = core_fimp.cod_centro
      AND confa.num_cuenta = core_fimp.num_cuenta
      AND confa.cod_producto = core_fimp.cod_producto
      AND confa.cod_subprodu_altair = core_fimp.cod_subprodu

-- CONTRATOS CANCELADOS
LEFT JOIN bi_corp_bdr.ctos_cancelados cancelados
       ON cancelados.id_cto_bdr = ncto.id_cto_bdr

-- BAREMO DE ENTIDADES
left join bi_corp_bdr.baremos ble
on ble.cod_baremo_local = core_fimp.cod_entidad

--CONDICIONES GENERALES DEL INSERT
where core_fimp.data_date_part between '{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day_arda', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' and nvl(regexp_replace(cancelados.ult_act, '-', ''), '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
and (confa.fec_castigo is null or confa.fec_castigo = '9999-12-31' or confa.fec_castigo = '0001-01-01' or regexp_replace(substring(confa.fec_castigo, 1, 7), '-', '') = '{{ ti.xcom_pull(task_ids='InputConfig', key='month', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
group by '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}',
lpad(nvl(ble.cod_baremo_global,'0'),5,'0'),
lpad(ncto.id_cto_bdr,9,"0"),
to_date(from_unixtime(UNIX_TIMESTAMP(core_fimp.data_date_part,"yyyyMMdd"))),
CASE WHEN confa.cod_marca = 'FA' AND confa.fec_castigo = to_date(from_unixtime(UNIX_TIMESTAMP(core_fimp.data_date_part,"yyyyMMdd")))
      THEN lpad("4",5,"0")
     ELSE lpad("9",5,"0") END;


insert INTO table bi_corp_bdr.jm_flujos
-- INSERT DE MOVIMIENTOS EN FLUJOS CANCELADOS (8)
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
SELECT
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' AS R9746_FEOPERAC,
fl.r9746_s1emp as R9746_S1EMP,
fl.r9746_contra1 as R9746_CONTRA1,
cancelados.fec_cancelacion AS R9746_FECMVTO,
lpad("8",5,"0") as R9746_CLASMVTO,
concat('-',lpad(regexp_replace(format_number(SUM(0), 2),"\\,|\\.",""),16,"0")) as R9746_IMPORTE,
concat('-',lpad(regexp_replace(format_number(SUM(0), 2),"\\,|\\.",""),16,"0")) as R9746_SALONBAL

-- FUENTE DE CONTRATOS DE FLUJO
  FROM bi_corp_bdr.jm_flujos fl

-- NORMALIZACION DE CONTRATOS
INNER JOIN bi_corp_bdr.cto_perimetro ncto
        ON ncto.id_cto_bdr = fl.r9746_contra1

-- CONTRATOS CANCELADOS
INNER JOIN bi_corp_bdr.ctos_cancelados cancelados
        ON cancelados.id_cto_bdr = ncto.id_cto_bdr

-- CONTRATOS FALLIDOS
LEFT JOIN bi_corp_bdr.ctos_fallidos confa
       ON confa.cod_entidad = ncto.cod_entidad
      AND confa.cod_centro = ncto.cod_centro
      AND confa.num_cuenta = ncto.num_cuenta
      AND confa.cod_producto = ncto.cod_producto
      AND confa.cod_subprodu_altair = ncto.cod_subprodu

--CONDICIONES GENERALES DEL INSERT
WHERE fl.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
  AND confa.cod_marca is null
group by '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}',
fl.r9746_s1emp,
fl.r9746_contra1,
cancelados.fec_cancelacion,
lpad("8",5,"0");

-- INSERT DE MOVIMIENTOS QUE SE CANCELAN (8) EL PRIMER DIA DEL MES
WITH canceladosPrimerDia AS ( --solo me quedo con los cancelados el 1/12
  SELECT c.id_cto_bdr
	FROM bi_corp_bdr.ctos_cancelados c
	WHERE c.fec_cancelacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day_arda', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' --'${primerDiaDeEsteMes}'
),
flujosMesAnterior AS ( --todos los flujos de NOVIEMBRE ordenados por fecha de movimiento
	SELECT row_number() over(partition by fl.r9746_contra1 order by fl.r9746_fecmvto desc) AS orden, fl.*
	FROM bi_corp_bdr.jm_flujos fl
	WHERE fl.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' --'${mesAnterior}'
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
insert INTO table bi_corp_bdr.jm_flujos
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
SELECT r9746_feoperac,r9746_s1emp,r9746_contra1,r9746_fecmvto,
       r9746_clasmvto,r9746_importe,r9746_salonbal
FROM diferencia;