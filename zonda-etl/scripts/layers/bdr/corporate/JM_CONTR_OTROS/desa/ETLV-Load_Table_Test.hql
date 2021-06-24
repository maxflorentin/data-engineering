set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

CREATE TEMPORARY TABLE bi_corp_bdr.contr_otros_area_negocio
    (
        id_cto_bdr STRING,
        unnv       STRING,
        unnts      STRING
    );

with
 tabla as (
               select * from (
                              select row_number() over(partition by c.id_cto_bdr,rn.segmentation_code order by rn.global_value asc) as orden,
                                     c.id_cto_bdr , rn.segmentation_code , rn.global_value
                                from bi_corp_bdr.perim_contratos_bis c
                              inner join bi_corp_common.rosetta_nkey_hist nk
                                  on c.id_cto_bdr = nk.native_key
                                 and c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
                                 and nk.partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_rosetta_nkey_hist', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
                                 and nk.domain_code ='00004'
                              inner join bi_corp_common.rosetta_nkey_hist nk2
                                  on nk2.master_key = nk.master_key
                                 and nk2.partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_rosetta_nkey_hist', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
                                 and nk2.domain_code = '00005'
                              inner join bi_corp_common.rosetta_rnkd_hist rn
                                  on rn.native_key = nk2.native_key
                                 and rn.partition_date=  '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_rosetta_rnkd_hist', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
                                 and rn.domain_code = '00005'
                                 and rn.global_value != 'AN04020000'
                               where rn.segmentation_code in ( '00001', '00004')
                                 and trim(rn.global_value) != '998'
                              order by c.id_cto_bdr
                              ) a
                where orden = 1
            ),
 baremos as (
             select t.id_cto_bdr, t.segmentation_code, bl.cod_baremo_local, bgl.cod_baremo_global
               from tabla t
             left join bi_corp_bdr.baremos_local bl
                 on bl.cod_negocio_local in ('86','116')
                and t.global_value = bl.cod_baremo_alfanumerico_local
             left join bi_corp_bdr.map_baremos_global_local bgl
                 on bl.cod_negocio_local = cast(bgl.cod_negocio_local as string)
                and bl.cod_baremo_local = bgl.cod_baremo_local
             )
insert overwrite table bi_corp_bdr.contr_otros_area_negocio
select b1.id_cto_bdr,
       lpad(b4.cod_baremo_local,5,'0'),
       lpad(b1.cod_baremo_global,5,'0')
from baremos b1 inner join baremos b4
	on b1.id_cto_bdr = b4.id_cto_bdr
	    and b1.segmentation_code = '00001'
	    and b4.segmentation_code = '00004';

CREATE TEMPORARY TABLE bi_corp_bdr.contr_otros_sit_gestion
  (idf_cto_supra_s_divisa STRING,
   situacion_gestion STRING,
   utp STRING,
   actualiza_utp STRING,
   fin_utp STRING,
   orden_dup STRING
   );


with core_garra as (

    select
            m.id_cto_source as idf_cto_supra_s_divisa,
            cg.cod_marca,
            cg.cod_submarca,
            cg.dias_de_impago

    FROM santander_business_risk_arda.contratos_garra cg

    INNER JOIN bi_corp_bdr.perim_contratos_bis m
           ON m.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
          AND concat_ws("_", cg.cod_entidad, cg.cod_centro, cg.num_cuenta, cg.cod_producto, cg.cod_subprodu) = m.id_cto_source
    WHERE cg.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_contratos_garra', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'

    group by
            m.id_cto_source,
            cg.cod_marca,
            cg.cod_submarca,
            cg.dias_de_impago
),

core_refi as (

    select
            m.id_cto_source as idf_cto_supra_s_divisa,
            cr.num_cuenta,
            cr.filler2,
            cr.dudosidad

    FROM (SELECT bb.cod_entidad, bb.cod_centro, bb.num_cuenta, bb.cod_producto, bb.cod_subprodu, bb.cod_atribuido_a_segmento_especial as filler2,
                 min(to_date(from_unixtime(UNIX_TIMESTAMP(bb.data_date_part, 'yyyyMMdd')))) dudosidad,
                 max(to_date(from_unixtime(UNIX_TIMESTAMP(bb.data_date_part, 'yyyyMMdd')))) dudosidad_max
            FROM santander_business_risk_arda.contratos_garra bb
          INNER JOIN (SELECT cod_entidad, cod_centro, num_cuenta, cod_producto, cod_subprodu, cod_divisa
                        FROM santander_business_risk_arda.contratos_garra
                       WHERE data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_contratos_garra', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
                         AND cod_atribuido_a_segmento_especial = 'DU'
                         AND cod_producto IN ('40','41','42','71')
                      GROUP BY cod_entidad, cod_centro, num_cuenta, cod_producto, cod_subprodu, cod_divisa) c
                     ON c.cod_entidad = bb.cod_entidad
                    AND c.cod_centro = bb.cod_centro
                    AND c.num_cuenta = bb.num_cuenta
                    AND c.cod_producto = bb.cod_producto
                    AND c.cod_subprodu = bb.cod_subprodu
                    AND c.cod_divisa = bb.cod_divisa
          WHERE bb.cod_atribuido_a_segmento_especial = 'DU'
            AND bb.data_date_part BETWEEN     '{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day_arda', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
                                          AND '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day_arda',  dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
          GROUP BY bb.cod_entidad, bb.cod_centro, bb.num_cuenta, bb.cod_producto, bb.cod_subprodu, cod_atribuido_a_segmento_especial
          ) cr

    INNER JOIN bi_corp_bdr.perim_contratos_bis m
           ON m.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
          AND concat_ws("_", cr.cod_entidad, cr.cod_centro, cr.num_cuenta, cr.cod_producto, cr.cod_subprodu) = m.id_cto_source

    group by
            m.id_cto_source,
            cr.num_cuenta,
            cr.filler2,
            cr.dudosidad
),

core_refdu as (

    select
            findu.idf_cto_supra_s_divisa,
            findu.fin_du

    FROM (SELECT concat_ws('_', refi.cod_entidad,refi.cod_centro,refi.num_cuenta,refi.cod_producto,refi.cod_subprodu) AS idf_cto_supra_s_divisa,
                 MAX(to_date(from_unixtime(UNIX_TIMESTAMP(refi.data_date_part, 'yyyyMMdd')))) as fin_du

            FROM bi_corp_bdr.jm_contr_otros contr

          INNER JOIN bi_corp_bdr.normalizacion_id_contratos b
                     ON b.id_cto_bdr = contr.e0623_contra1
                    and b.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'

          INNER JOIN santander_business_risk_arda.contratos_garra refi
                     ON refi.cod_entidad = b.cred_cod_entidad
                    AND refi.cod_centro = b.cred_cod_centro
                    AND refi.num_cuenta = b.cred_num_cuenta
                    AND refi.cod_producto = b.cred_cod_producto
                    AND refi.cod_subprodu = b.cred_cod_subprodu_altair
                    AND refi.cod_atribuido_a_segmento_especial = 'DU'
                    AND refi.cod_producto IN ('40','41','42','71')
                    AND refi.data_date_part BETWEEN     '{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day_arda', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
                                                    AND '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day_arda',  dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
           LEFT JOIN santander_business_risk_arda.contratos_garra fil
                     ON refi.cod_entidad = fil.cod_entidad
                    AND refi.cod_centro = fil.cod_centro
                    AND refi.num_cuenta = fil.num_cuenta
                    AND refi.cod_producto = fil.cod_producto
                    AND refi.cod_subprodu = fil.cod_subprodu
                    AND refi.cod_divisa = fil.cod_divisa
                    AND fil.cod_atribuido_a_segmento_especial = 'DU'
                    AND fil.cod_producto IN ('40','41','42','71')
                    AND fil.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_contratos_garra', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
          WHERE contr.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
            AND contr.e0623_gest_sit = '00024'
            AND fil.cod_entidad is null
         GROUP BY concat_ws('_', refi.cod_entidad,refi.cod_centro,refi.num_cuenta,refi.cod_producto,refi.cod_subprodu)
         ) findu

    INNER JOIN  bi_corp_bdr.perim_contratos_bis m
           ON m.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
          AND findu.idf_cto_supra_s_divisa = m.id_cto_source

    group by
            findu.idf_cto_supra_s_divisa,
            findu.fin_du
)

insert overwrite table bi_corp_bdr.contr_otros_sit_gestion

Select tb.idf_cto_supra_s_divisa,
       tb.situacion_gestion,
       tb.utp,
       tb.actualiza_utp,
       case when tb.fin_utp = '0001-01-01' then '9999-12-31' else tb.fin_utp end as fin_utp,
       row_number() over(partition by tb.idf_cto_supra_s_divisa order by tb.Prioridad_situacion_gestion asc) as orden_dup
from (
    SELECT bq.idf_cto_supra_s_divisa, bq.situacion_gestion, bq.Prioridad_situacion_gestion,
            MIN(CASE WHEN ((cgf.cod_marca = 'DE' AND cgf.cod_submarca = 'PAS') OR (cgf.cod_marca = 'DE' AND cgf.cod_submarca = 'GEN')) AND bq.situacion_gestion = '00005'
                      THEN to_date(from_unixtime(UNIX_TIMESTAMP(cgf.data_date_part,'yyyyMMdd')))
                     WHEN (cgf.cod_marca = 'PR' AND (cgf.cod_submarca = 'COH' OR cgf.cod_submarca = 'CON' OR cgf.cod_submarca = 'GEN')) AND bq.situacion_gestion = '00007'
                      THEN to_date(from_unixtime(UNIX_TIMESTAMP(cgf.data_date_part,'yyyyMMdd')))
                     WHEN (cgf.cod_marca = 'CO' AND (cgf.cod_submarca = 'COH' OR cgf.cod_submarca = 'CON' OR cgf.cod_submarca = 'GEN')) AND bq.situacion_gestion = '00008'
                      THEN to_date(from_unixtime(UNIX_TIMESTAMP(cgf.data_date_part,'yyyyMMdd')))
                     WHEN (cgf.cod_marca = 'MO' AND cgf.cod_submarca = 'CON') AND bq.situacion_gestion = '00009'
                      THEN to_date(from_unixtime(UNIX_TIMESTAMP(cgf.data_date_part,'yyyyMMdd')))
                     WHEN (cgf.cod_marca = 'FA' AND cgf.cod_submarca = 'FA') AND bq.situacion_gestion = '00013'
                      THEN to_date(from_unixtime(UNIX_TIMESTAMP(cgf.data_date_part,'yyyyMMdd')))
                     WHEN bq.situacion_gestion = '00024' THEN bq.dudosidad
                     WHEN bq.situacion_gestion IN ('00005','00007','00008','00009','00013','00018','00024','00025') THEN '9999-12-31'
                     ELSE '9999-12-31' END) AS utp,
            MIN(CASE WHEN ((cgf.cod_marca = 'DE' AND cgf.cod_submarca = 'PAS') OR (cgf.cod_marca = 'DE' AND cgf.cod_submarca = 'GEN')) AND bq.situacion_gestion = '00005'
                     THEN to_date(from_unixtime(UNIX_TIMESTAMP(cgf.data_date_part,'yyyyMMdd')))
                    WHEN ((cgf.cod_marca = 'PR' AND (cgf.cod_submarca = 'COH' OR cgf.cod_submarca = 'CON' OR cgf.cod_submarca = 'GEN')) ) AND bq.situacion_gestion = '00007'
                     THEN to_date(from_unixtime(UNIX_TIMESTAMP(cgf.data_date_part,'yyyyMMdd')))
                    WHEN ((cgf.cod_marca = 'CO' AND (cgf.cod_submarca = 'COH' OR cgf.cod_submarca = 'CON' OR cgf.cod_submarca = 'GEN'))) AND bq.situacion_gestion = '00008'
                     THEN to_date(from_unixtime(UNIX_TIMESTAMP(cgf.data_date_part,'yyyyMMdd')))
                    WHEN (cgf.cod_marca = 'MO' AND cgf.cod_submarca = 'CON') AND bq.situacion_gestion = '00009'
                     THEN to_date(from_unixtime(UNIX_TIMESTAMP(cgf.data_date_part,'yyyyMMdd')))
                    WHEN (cgf.cod_marca = 'FA' AND cgf.cod_submarca = 'FA') AND bq.situacion_gestion = '00013'
                     THEN to_date(from_unixtime(UNIX_TIMESTAMP(cgf.data_date_part,'yyyyMMdd')))
                    WHEN bq.situacion_gestion = '00024' THEN bq.dudosidad
                    WHEN bq.situacion_gestion IN ('00005','00007','00008','00009','00013','00018','00024','00025') THEN '9999-12-31'
                    ELSE '9999-12-31' END) AS actualiza_utp,
            MAX(CASE WHEN bq.fin_du IS NOT NULL THEN bq.fin_du
                    WHEN (((cgf.cod_marca = 'DE' AND cgf.cod_submarca = 'PAS') OR (cgf.cod_marca = 'DE' AND cgf.cod_submarca = 'GEN'))
                      OR (cgf.cod_marca = 'PR' AND (cgf.cod_submarca = 'COH' OR cgf.cod_submarca = 'CON' OR cgf.cod_submarca = 'GEN'))
                      OR (cgf.cod_marca = 'CO' AND (cgf.cod_submarca = 'COH' OR cgf.cod_submarca = 'CON' OR cgf.cod_submarca = 'GEN'))
                      OR (cgf.cod_marca = 'MO' AND cgf.cod_submarca = 'CON')
                      OR (cgf.cod_marca = 'FA' AND cgf.cod_submarca = 'FA')) AND bq.situacion_gestion NOT IN ('00005','00007','00008','00009','00013','00018','00024','00025')
                     THEN to_date(from_unixtime(UNIX_TIMESTAMP(cgf.data_date_part,'yyyyMMdd')))
                    ELSE '0001-01-01' END) AS fin_utp
     FROM santander_business_risk_arda.contratos_garra cgf
     inner JOIN
        (select
                garr.idf_cto_supra_s_divisa,
                CASE WHEN ref.num_cuenta IS NOT NULL THEN lpad(24, 5, '0')
                     WHEN (garr.cod_marca = 'DE' AND garr.cod_submarca = 'PAS') OR (garr.cod_marca = 'DE' AND garr.cod_submarca = 'GEN') THEN lpad(5, 5, '0')
                     WHEN (garr.cod_marca = 'PR' AND (garr.cod_submarca = 'COH' OR garr.cod_submarca = 'CON' OR garr.cod_submarca = 'GEN')) THEN lpad(7, 5, '0')
                     WHEN (garr.cod_marca = 'CO' AND (garr.cod_submarca = 'COH' OR garr.cod_submarca = 'CON' OR garr.cod_submarca = 'GEN')) THEN lpad(8, 5, '0')
                     WHEN (garr.cod_marca = 'MO' AND garr.cod_submarca = 'CON') THEN lpad(9, 5, '0')
                     WHEN (garr.cod_marca = 'FA' AND garr.cod_submarca = 'FA') THEN lpad(13, 5, '0')
                     WHEN garr.dias_de_impago = 0 THEN lpad(1, 5, '0')
                     WHEN garr.dias_de_impago <= 30 THEN lpad(2, 5, '0')
                     WHEN garr.dias_de_impago <= 60 THEN lpad(3, 5, '0')
                     WHEN garr.dias_de_impago <= 90 THEN lpad(4, 5, '0')
                     WHEN garr.dias_de_impago > 90 THEN lpad(15, 5, '0')
                     ELSE '99999' END AS situacion_gestion,
                CASE WHEN ref.num_cuenta IS NOT NULL THEN 1
                     WHEN (garr.cod_marca = 'DE' AND garr.cod_submarca = 'PAS') OR (garr.cod_marca = 'DE' AND garr.cod_submarca = 'GEN') THEN 2
                     WHEN (garr.cod_marca = 'PR' AND (garr.cod_submarca = 'COH' OR garr.cod_submarca = 'CON' OR garr.cod_submarca = 'GEN')) THEN 3
                     WHEN (garr.cod_marca = 'CO' AND (garr.cod_submarca = 'COH' OR garr.cod_submarca = 'CON' OR garr.cod_submarca = 'GEN')) THEN 4
                     WHEN (garr.cod_marca = 'MO' AND garr.cod_submarca = 'CON') THEN 5
                     WHEN (garr.cod_marca = 'FA' AND garr.cod_submarca = 'FA') THEN 6
                     WHEN garr.dias_de_impago = 0 THEN 7
                     WHEN garr.dias_de_impago <= 30 THEN 8
                     WHEN garr.dias_de_impago <= 60 THEN 9
                     WHEN garr.dias_de_impago <= 90 THEN 10
                     WHEN garr.dias_de_impago > 90 THEN 11
                     ELSE 12 END AS Prioridad_situacion_gestion,
                garr.cod_marca,
                garr.cod_submarca,
                ref.filler2,
                ref.dudosidad,
                fdu.fin_du

          from core_garra garr

         LEFT JOIN core_refi ref
               ON ref.idf_cto_supra_s_divisa = garr.idf_cto_supra_s_divisa

         LEFT JOIN core_refdu fdu
               on fdu.idf_cto_supra_s_divisa = garr.idf_cto_supra_s_divisa

        ) bq
            ON bq.idf_cto_supra_s_divisa = concat_ws("_", cgf.cod_entidad,cgf.cod_centro,cgf.num_cuenta,cgf.cod_producto,cgf.cod_subprodu)
     WHERE cgf.data_date_part BETWEEN     '{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day_arda', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
                                      AND '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day_arda',  dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
     GROUP BY bq.idf_cto_supra_s_divisa, bq.situacion_gestion, Prioridad_situacion_gestion
    ) tb;

CREATE TEMPORARY TABLE bi_corp_bdr.contr_otros_tarjetas
  (id_cto_source STRING,
   estado_tarjeta STRING,
   limite_oculto STRING,
   forma_pago STRING,
   importe_concedido STRING
   );

insert overwrite table bi_corp_bdr.contr_otros_tarjetas
SELECT distinct
a.id_cto_source,
CASE WHEN coalesce(tca.cod_est_tarj_tit, tcm.cod_est_tarj_tit, tcv.cod_est_tarj_tit) = '20' THEN '00001'
     WHEN coalesce(tca.cod_est_tarj_tit, tcm.cod_est_tarj_tit, tcv.cod_est_tarj_tit) in ('22', '29') THEN '00002'
     ELSE '99999' END
     AS estado_tarjeta,
CASE WHEN locate('.', (coalesce(tcv.imp_lim_sombra, tca.imp_lim_sombra, tcm.imp_lim_sombra))) = 0 AND (coalesce(tcv.imp_lim_sombra, tca.imp_lim_sombra, tcm.imp_lim_sombra)) >= 0
       THEN regexp_replace(concat('+', lpad(concat((nvl(coalesce(tcv.imp_lim_sombra, tca.imp_lim_sombra, tcm.imp_lim_sombra), 0)), '00'), 16, '0')), '\\.', '')
     WHEN (coalesce(tcv.imp_lim_sombra, tca.imp_lim_sombra, tcm.imp_lim_sombra)) >= 0 AND length(substr((coalesce(tcv.imp_lim_sombra, tca.imp_lim_sombra, tcm.imp_lim_sombra)), locate('.', (coalesce(tcv.imp_lim_sombra, tca.imp_lim_sombra, tcm.imp_lim_sombra))), length((coalesce(tcv.imp_lim_sombra, tca.imp_lim_sombra, tcm.imp_lim_sombra))))) = 2
       THEN regexp_replace(concat('+', lpad(concat((nvl(coalesce(tcv.imp_lim_sombra, tca.imp_lim_sombra, tcm.imp_lim_sombra), 0)), '0'), 17, '0')), '\\.', '')
     WHEN (coalesce(tcv.imp_lim_sombra, tca.imp_lim_sombra, tcm.imp_lim_sombra)) >= 0
       THEN regexp_replace(concat('+', lpad((nvl(coalesce(tcv.imp_lim_sombra, tca.imp_lim_sombra, tcm.imp_lim_sombra), 0)), 17, '0')), '\\.', '')
     WHEN coalesce(tcv.imp_lim_sombra, tca.imp_lim_sombra, tcm.imp_lim_sombra) IS NULL
       THEN '+0000000000000000'
     WHEN coalesce(tcv.imp_lim_sombra, tca.imp_lim_sombra, tcm.imp_lim_sombra) = 0
       THEN '+0000000000000000'
     WHEN locate('.', (coalesce(tcv.imp_lim_sombra, tca.imp_lim_sombra, tcm.imp_lim_sombra))) = 0 AND (coalesce(tcv.imp_lim_sombra, tca.imp_lim_sombra, tcm.imp_lim_sombra)) < 0
       THEN regexp_replace(concat('-', lpad(concat(nvl(coalesce(tcv.imp_lim_sombra, tca.imp_lim_sombra, tcm.imp_lim_sombra), 0), '00'), 16, '0')), '\\.', '')
     WHEN (coalesce(tcv.imp_lim_sombra, tca.imp_lim_sombra, tcm.imp_lim_sombra)) < 0 AND length(substr((coalesce(tcv.imp_lim_sombra, tca.imp_lim_sombra, tcm.imp_lim_sombra)), locate('.', (coalesce(tcv.imp_lim_sombra, tca.imp_lim_sombra, tcm.imp_lim_sombra))), length((coalesce(tcv.imp_lim_sombra, tca.imp_lim_sombra, tcm.imp_lim_sombra))))) = 2
       THEN regexp_replace(concat('-', lpad(concat(nvl(coalesce(tcv.imp_lim_sombra, tca.imp_lim_sombra, tcm.imp_lim_sombra), 0), '0'), 17, '0')), '\\.', '')
     ELSE regexp_replace(concat('-', lpad((nvl(coalesce(tcv.imp_lim_sombra, tca.imp_lim_sombra, tcm.imp_lim_sombra), 0)), 17, '0')), '\\.', '') END
     AS limite_oculto,
CASE WHEN nvl(coalesce(tcv.cod_forma_pago, tca.cod_forma_pago, tcm.cod_forma_pago), 0) = 1 THEN lpad(1, 5, '0')
     WHEN nvl(coalesce(tcv.cod_forma_pago, tca.cod_forma_pago, tcm.cod_forma_pago), 0) = 2 THEN lpad(5, 5, '0')
     WHEN nvl(coalesce(tcv.cod_forma_pago, tca.cod_forma_pago, tcm.cod_forma_pago), 0) = 3 THEN lpad(2, 5, '0')
     WHEN nvl(coalesce(tcv.cod_forma_pago, tca.cod_forma_pago, tcm.cod_forma_pago), 0) = 4 THEN lpad(5, 5, '0')
     WHEN nvl(coalesce(tcv.cod_forma_pago, tca.cod_forma_pago, tcm.cod_forma_pago), 0) = 5 THEN lpad(2, 5, '0')
     ELSE '00000' END
     AS forma_pago,
CASE WHEN locate('.', (coalesce(tcv.imp_lim_financ, tca.imp_lim_financ, tcm.imp_lim_financ))) = 0 AND (coalesce(tcv.imp_lim_financ, tca.imp_lim_financ, tcm.imp_lim_financ)) >= 0
       THEN regexp_replace(concat('+', lpad(concat((nvl(coalesce(tcv.imp_lim_financ, tca.imp_lim_financ, tcm.imp_lim_financ), 0)), '00'), 16, '0')), '\\.', '')
    WHEN (coalesce(tcv.imp_lim_financ, tca.imp_lim_financ, tcm.imp_lim_financ)) >= 0 AND length(substr((coalesce(tcv.imp_lim_financ, tca.imp_lim_financ, tcm.imp_lim_financ)), locate('.', (coalesce(tcv.imp_lim_financ, tca.imp_lim_financ, tcm.imp_lim_financ))), length((coalesce(tcv.imp_lim_financ, tca.imp_lim_financ, tcm.imp_lim_financ))))) = 2
       THEN regexp_replace(concat('+', lpad(concat((nvl(coalesce(tcv.imp_lim_financ, tca.imp_lim_financ, tcm.imp_lim_financ), 0)), '0'), 17, '0')), '\\.', '')
    WHEN (coalesce(tcv.imp_lim_financ, tca.imp_lim_financ, tcm.imp_lim_financ)) >= 0
       THEN regexp_replace(concat('+', lpad((nvl(coalesce(tcv.imp_lim_financ, tca.imp_lim_financ, tcm.imp_lim_financ), 0)), 17, '0')), '\\.', '')
    WHEN coalesce(tcv.imp_lim_financ, tca.imp_lim_financ, tcm.imp_lim_financ) IS NULL
       THEN '+0000000000000000'
    WHEN coalesce(tcv.imp_lim_financ, tca.imp_lim_financ, tcm.imp_lim_financ) = 0
       THEN '+0000000000000000'
    WHEN locate('.', (coalesce(tcv.imp_lim_financ, tca.imp_lim_financ, tcm.imp_lim_financ))) = 0 AND (coalesce(tcv.imp_lim_financ, tca.imp_lim_financ, tcm.imp_lim_financ)) < 0
       THEN regexp_replace(concat('-', lpad(concat(nvl(coalesce(tcv.imp_lim_financ, tca.imp_lim_financ, tcm.imp_lim_financ), 0), '00'), 16, '0')), '\\.', '')
    WHEN (coalesce(tcv.imp_lim_financ, tca.imp_lim_financ, tcm.imp_lim_financ)) < 0 AND length(substr((coalesce(tcv.imp_lim_financ, tca.imp_lim_financ, tcm.imp_lim_financ)), locate('.', (coalesce(tcv.imp_lim_financ, tca.imp_lim_financ, tcm.imp_lim_financ))), length((coalesce(tcv.imp_lim_financ, tca.imp_lim_financ, tcm.imp_lim_financ))))) = 2
       THEN regexp_replace(concat('-', lpad(concat(nvl(coalesce(tcv.imp_lim_financ, tca.imp_lim_financ, tcm.imp_lim_financ), 0), '0'), 17, '0')), '\\.', '')
    ELSE regexp_replace(concat('-', lpad((nvl(coalesce(tcv.imp_lim_financ, tca.imp_lim_financ, tcm.imp_lim_financ), 0)), 17, '0')), '\\.', '') END
    AS importe_concedido
FROM bi_corp_bdr.perim_contratos_bis a

LEFT JOIN santander_business_risk_arda.ifrs9_tarjetas_saldos_visa tcv
          ON a.id_cto_source = concat_ws('_','0072',tcv.cod_centro,tcv.num_cuenta,tcv.cod_producto,tcv.cod_subprodu)
         AND tcv.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_ifrs9_tarjetas_saldos_visa', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'

LEFT JOIN santander_business_risk_arda.ifrs9_tarjetas_saldos_amex tca
        ON a.id_cto_source = concat_ws('_','0072',tca.cod_centro,tca.num_cuenta,tca.cod_producto,tca.cod_subprodu)
       AND tca.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_ifrs9_tarjetas_saldos_amex', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'

LEFT JOIN santander_business_risk_arda.ifrs9_tarjetas_saldos_master tcm
        ON a.id_cto_source = concat_ws('_','0072',tcm.cod_centro,tcm.num_cuenta,tcm.cod_producto,tcm.cod_subprodu)
       AND tcm.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_ifrs9_tarjetas_saldos_master', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'

WHERE a.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
  and (   concat(tcm.cod_centro,tcm.num_cuenta,tcm.cod_producto,tcm.cod_subprodu) is not null
       or concat(tca.cod_centro,tca.num_cuenta,tca.cod_producto,tca.cod_subprodu) is not null
       or concat(tcv.cod_centro,tcv.num_cuenta,tcv.cod_producto,tcv.cod_subprodu) is not null);

with core_contratos as (
select  prim.id_cto_source, prim.id_cto_bdr, prim.cod_producto, prim.cod_subprodu
      , nvl(dvto.num_dias_para_vcmto, 0) as num_dias_para_vcmto
      , nvl(lcre.imp_lim_credito_ml, 0) as imp_lim_credito_ml
      , max(nvl(datmax.mes_inact_tdc, 0)) as mes_inact_tdc
      , max(nvl(datmax.num_dia_demora, 0)) as num_dia_demora
      , nvl(tint.tasa_int, 0.0000) as tasa_int

FROM (
            select m.id_cto_bdr, m.id_cto_source, m.cred_cod_entidad as cod_entidad, m.cred_cod_centro as cod_centro,
                   m.cred_num_cuenta as num_cuenta, m.cred_cod_producto as cod_producto, m.cred_cod_subprodu_altair as cod_subprodu
            from bi_corp_bdr.jm_contr_bis cb
            INNER JOIN bi_corp_bdr.normalizacion_id_contratos m
                   on m.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
                  and m.id_cto_bdr = cb.G4095_CONTRA1
                  AND m.source = 'credito'
            where cb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
           ) prim

left join santander_business_risk_arda.contratos dvto
               on dvto.cod_entidad = prim.cod_entidad
              and dvto.cod_centro = prim.cod_centro
              and dvto.num_cuenta = prim.num_cuenta
              and dvto.cod_producto = prim.cod_producto
              and dvto.cod_subprodu_altair = prim.cod_subprodu
--              and dvto.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
              and dvto.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_contratos', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
              and dvto.num_dias_para_vcmto is not null

left join santander_business_risk_arda.contratos lcre
               on lcre.cod_entidad = prim.cod_entidad
              and lcre.cod_centro = prim.cod_centro
              and lcre.num_cuenta = prim.num_cuenta
              and lcre.cod_producto = prim.cod_producto
              and lcre.cod_subprodu_altair = prim.cod_subprodu
--              and lcre.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
              and lcre.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_contratos', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
              and lcre.imp_lim_credito_ml is not null
              and lcre.imp_lim_credito_ml != 0

left join santander_business_risk_arda.contratos datmax
               on datmax.cod_entidad = prim.cod_entidad
              and datmax.cod_centro = prim.cod_centro
              and datmax.num_cuenta = prim.num_cuenta
              and datmax.cod_producto = prim.cod_producto
              and datmax.cod_subprodu_altair = prim.cod_subprodu
--              and datmax.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
              and datmax.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_contratos', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'

left join santander_business_risk_arda.contratos tint
               on tint.cod_entidad = prim.cod_entidad
              and tint.cod_centro = prim.cod_centro
              and tint.num_cuenta = prim.num_cuenta
              and tint.cod_producto = prim.cod_producto
              and tint.cod_subprodu_altair = prim.cod_subprodu
--              and tint.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
              and tint.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_contratos', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
              and tint.tasa_int is not null
              and tint.tasa_int != 0

group by   prim.id_cto_source, prim.id_cto_bdr, prim.cod_producto, prim.cod_subprodu
         , nvl(dvto.num_dias_para_vcmto, 0)
         , nvl(lcre.imp_lim_credito_ml,0)
         , nvl(tint.tasa_int, 0.0000)

)


insert overwrite table bi_corp_bdr.test_jm_contr_otros
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}')

SELECT DISTINCT
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' AS E0623_FEOPERAC,
23100 AS E0623_S1EMP,
lpad(a.id_cto_bdr, 9, '0') AS E0623_CONTRA1,
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' AS E0623_FEC_MES,
case when concat(lpad(nvl(a.num_dias_para_vcmto, 0), 11, '0'),'000000') = '00000000000000000'
      then '00000999999000000'
      else concat(lpad(nvl(a.num_dias_para_vcmto, 0), 11, '0'),'000000')
end  AS E0623_VCTO_RES,
lpad(0, 17, '0') AS E0623_VCTO_PON,
lpad(nvl(vbl.cod_baremo_local,'0'), 5, '0') AS E0623_IDSUBPRD,
lpad(0, 5, '0') AS  E0623_TIP_LIQU,
lpad(0, 17, '0') AS E0623_LIQ_PZO,
lpad(0, 5, '0') AS  E0623_TIP_AMOR,
lpad(0, 17, '0') AS E0623_AMOR_PZO,
lpad(0, 5, '0') AS  E0623_AMOR_SIS,
lpad(0, 5, '0') AS  E0623_CTB_SITU,
nvl(temp_sit.situacion_gestion, '99999') AS E0623_GEST_SIT,
nvl(temp_sit.situacion_gestion, '99999') AS E0623_GES2_SIT,
lpad(0, 9, '0') AS  E0623_ATAEMAX,
lpad(0, 9, '0') AS  E0623_TIP_INT,
case when dac.limite_credito is not null then
            concat('+', lpad(regexp_replace(format_number(cast(regexp_replace(dac.limite_credito,'\\,','.') as double), 2) ,'\\,|\\.|\\-|\\+',''),16,'0'))
      else
           lpad(nvl(cot.importe_concedido,0),17,'0')
      end AS E0623_IMP1LIMI,
case when dac.disponible is not null then
            concat('+', lpad(regexp_replace(format_number(cast(regexp_replace(dac.disponible,'\\,','.') as double), 2) ,'\\,|\\.|\\-|\\+',''),16,'0'))
      else
      coalesce(
         CASE WHEN locate('.', (a.imp_lim_credito_ml)) = 0 AND (a.imp_lim_credito_ml) >= 0
                 THEN regexp_replace(concat('+', lpad(concat((nvl(a.imp_lim_credito_ml, 0)), '00'), 16, '0')), '\\.', '')
              WHEN (a.imp_lim_credito_ml) >= 0 AND length(substr((a.imp_lim_credito_ml), locate('.', (a.imp_lim_credito_ml)), length((a.imp_lim_credito_ml)))) = 2
                 THEN regexp_replace(concat('+', lpad(concat((nvl(a.imp_lim_credito_ml, 0)), '0'), 17, '0')), '\\.', '')
              WHEN (a.imp_lim_credito_ml) >= 0
                 THEN regexp_replace(concat('+', lpad((nvl(a.imp_lim_credito_ml, 0)), 17, '0')), '\\.', '')
              WHEN a.imp_lim_credito_ml IS NULL
                 THEN '+0000000000000000'
              WHEN a.imp_lim_credito_ml = 0
                 THEN '+0000000000000000'
              WHEN locate('.', (a.imp_lim_credito_ml)) = 0 AND (a.imp_lim_credito_ml) < 0
                 THEN regexp_replace(concat('-', lpad(concat(nvl(a.imp_lim_credito_ml, 0), '00'), 16, '0')), '\\.', '')
              WHEN (a.imp_lim_credito_ml) < 0 AND length(substr((a.imp_lim_credito_ml), locate('.', (a.imp_lim_credito_ml)), length((a.imp_lim_credito_ml)))) = 2
                 THEN regexp_replace(concat('-', lpad(concat(nvl(a.imp_lim_credito_ml, 0), '0'), 17, '0')), '\\.', '')
              ELSE regexp_replace(concat('-', lpad((nvl(a.imp_lim_credito_ml, 0)), 17, '0')), '\\.', '') END
               )
      end AS E0623_ALIMACT,
--if(nvl(a.imp_lim_credito_ml, 0) >= 0, concat('+', lpad(nvl(a.imp_lim_credito_ml, 0), 16, '0')), concat('-', lpad(nvl(a.imp_lim_credito_ml, 0), 16, '0'))) AS E0623_ALIMACT,
concat('+', lpad(0, 16, '0')) AS    E0623_IMPORTH,
lpad(0, 5, '0') AS E0623_INV_NEGO,
concat('+', lpad(0, 16, '0')) AS    E0623_IPROVISI,
concat('+', lpad(0, 16, '0')) AS    E0623_IPROVIS1,
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' AS E0623_FECULTMO,
lpad(nvl(cot.estado_tarjeta,0),5,'0') AS E0623_ESTADTRJ,
lpad(a.mes_inact_tdc, 17, '0') AS E0623_INACTRJ,
22150 AS E0623_UNNT,
lpad(nvl(an.unnts,'0'), 5, '0') AS E0623_UNNTS,
lpad(nvl(an.unnv,'0'), 5, '0') AS E0623_UNNV,
lpad(0, 5, '0') AS E0623_UNNVS,
' ' AS  E0623_RGOSUB,
'0001-01-01' AS E0623_FECINCAR,
'0001-01-01' AS E0623_FECFICAR,
'N' AS E0623_INTNEG,
lpad(0, 5, '0') AS E0623_MTVALTA,
' ' AS  E0623_INDSUBRO,
lpad(0, 5, '0') AS  E0623_TIP_INTE,
lpad(0, 9, '0') AS  E0623_DIFERNCI,
case when nvl(b.importe_ifrs9,0) >= 0
           then concat('+', lpad(regexp_replace(format_number(cast(regexp_replace(nvl(b.importe_ifrs9,0) ,"\\,",".") as double), 2),"\\,|\\.|\\+",""),16,"0"))
           else concat('-', lpad(regexp_replace(format_number(cast(regexp_replace(nvl(b.importe_ifrs9,0) ,"\\,",".") as double), 2),"\\,|\\.|\\-",""),16,"0"))
     end  AS E0623_IMPRTCUO,
'N' AS  E0623_INDSEGUR,
'N' AS  E0623_AMORTPAR,
'9999-12-31' AS E0623_FECIMPAG,
concat('-', lpad(0, 16, '0')) AS    E0623_IMPPRIMP,
'9999-12-31' AS E0623_FHPRIMPG,
concat('-', lpad(0, 16, '0')) AS    E0623_IMPIMPNR,
lpad(0, 5, '0') AS E0623_ESTPRINM,
lpad(0, 5, '0') AS  E0623_EXCLCTO,
lpad(0, 9, '0') AS  E0623_CUOTIMPA,
lpad(nvl(cot.limite_oculto,0),17,'0') AS E0623_LIMOCULT,
lpad(0, 5, '0') AS E0623_CODIMPHI,
' ' AS  E0623_INDEUCON,
lpad(0, 5, '0') AS E0623_TIPCAREN,
concat('+', lpad(0, 16, '0')) AS E0623_CUOTPRES,
' ' AS E0623_IBUYSELL,
lpad(0, 9, '0') AS  E0623_SUTIPINT,
lpad(0, 9, '0') AS  E0623_TETIPINT,
lpad(0, 5, '0') AS  E0623_TIPSUELO,
lpad(0, 5, '0') AS  E0623_TIPTECHO,
lpad(0, 5, '0') AS  E0623_PLREVTIN,
'9999-12-31' AS E0623_FECCUOTA,
lpad(nvl(concat(a.num_dia_demora,'00'), 0), 9, '0') AS E0623_NUDIAATR,
lpad(0, 5, '0') AS  E0623_SEGPLLIM,
concat('+', lpad(0, 16, '0')) AS    E0623_VOLTRANS,
' ' AS  E0623_MARCAUTI,
lpad(0, 5, '0') AS  E0623_TOPDEALE,
'9999-12-31' AS E0623_FECREPRE,
23100 AS E0623_EMPREPOR,
concat('+', lpad(0, 16, '0')) AS    E0623_IMPCUIMP,
lpad(regexp_replace(format_number(cast(regexp_replace(nvl(a.tasa_int,0) ,"\\,",".") as double), 6),"\\,|\\.",""),9,"0") AS E0623_TIPINEFE,
lpad(nvl(cot.forma_pago,0),5,'0') AS E0623_FORPGACT,
CASE WHEN co_ant.E0623_CONTRA1 IS NULL AND temp_sit.situacion_gestion IN ('00005','00007','00008','00009','00013','00024') THEN temp_sit.utp
     WHEN co_ant.E0623_CONTRA1 IS NULL AND temp_sit.situacion_gestion NOT IN ('00005','00007','00008','00009','00013','00024') THEN '9999-12-31'
     WHEN temp_sit.situacion_gestion = co_ant.E0623_GEST_SIT THEN co_ant.E0623_FINIUTCT
     WHEN temp_sit.situacion_gestion <> co_ant.E0623_GEST_SIT AND temp_sit.situacion_gestion IN ('00005','00007','00008','00009','00013','00024') THEN temp_sit.actualiza_utp
     WHEN temp_sit.situacion_gestion <> co_ant.E0623_GEST_SIT AND temp_sit.situacion_gestion NOT IN ('00005','00007','00008','00009','00013','00024') THEN '9999-12-31'
     ELSE '9999-12-31'
     END AS E0623_FINIUTCT,
CASE WHEN co_ant.E0623_CONTRA1 IS NULL AND temp_sit.situacion_gestion IN ('00005','00007','00008','00009','00013','00024') THEN '9999-12-31'
     WHEN co_ant.E0623_CONTRA1 IS NULL AND temp_sit.situacion_gestion NOT IN ('00005','00007','00008','00009','00013','00024') THEN '9999-12-31'
     WHEN temp_sit.situacion_gestion = co_ant.E0623_GEST_SIT THEN '9999-12-31'
     WHEN temp_sit.situacion_gestion <> co_ant.E0623_GEST_SIT AND temp_sit.situacion_gestion IN ('00005','00007','00008','00009','00013','00024') THEN '9999-12-31'
     WHEN temp_sit.situacion_gestion <> co_ant.E0623_GEST_SIT AND temp_sit.situacion_gestion NOT IN ('00005','00007','00008','00009','00013','00024') AND co_ant.E0623_GEST_SIT NOT IN ('00005','00007','00008','00009','00013','00024') THEN '9999-12-31'
     WHEN temp_sit.situacion_gestion <> co_ant.E0623_GEST_SIT AND temp_sit.situacion_gestion NOT IN ('00005','00007','00008','00009','00013','00024') AND temp_sit.fin_utp <> '9999-12-31' THEN temp_sit.fin_utp
     WHEN temp_sit.situacion_gestion <> co_ant.E0623_GEST_SIT AND temp_sit.situacion_gestion NOT IN ('00005','00007','00008','00009','00013','00024') AND temp_sit.fin_utp  = '9999-12-31' THEN '{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
     ELSE '9999-12-31'
     END AS E0623_FFINUTCT

FROM core_contratos a

LEFT JOIN (
            select cod_entidad, cod_centro, num_cuenta, cod_producto, cod_subprodu,
                    sum(imp_capital+imp_aju_capital+imp_int_devengados) as importe_ifrs9
              FROM santander_business_risk_arda.ifrs9_contratos
--             where data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
             where data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_ifrs9_contratos', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
            group by cod_entidad, cod_centro, num_cuenta, cod_producto, cod_subprodu
           ) b
               ON a.id_cto_source = concat_ws('_', b.cod_entidad,b.cod_centro,b.num_cuenta,b.cod_producto,b.cod_subprodu)

LEFT JOIN bi_corp_bdr.v_baremos_local vbl
           ON TRIM(vbl.cod_baremo_alfanumerico_local) = TRIM(concat(a.cod_producto, a.cod_subprodu))
          and vbl.cod_negocio_local = '3'

LEFT JOIN bi_corp_bdr.jm_contr_otros co_ant
           ON co_ant.E0623_CONTRA1 = a.id_cto_bdr
          AND co_ant.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'

LEFT JOIN bi_corp_bdr.contr_otros_sit_gestion temp_sit
           ON temp_sit.idf_cto_supra_s_divisa = a.id_cto_source
          AND temp_sit.orden_dup = '1'

LEFT JOIN bi_corp_bdr.contr_otros_tarjetas cot
            ON cot.id_cto_source = a.id_cto_source

LEFT JOIN bi_corp_bdr.datos_arda_contratos dac
            ON dac.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
           AND dac.id_cto_bdr = a.id_cto_bdr

LEFT JOIN bi_corp_bdr.contr_otros_area_negocio an
            ON an.id_cto_bdr = a.id_cto_bdr
;


--MMFF Contratos Otros Datos

with ficheros_rtra as (
                       select distinct *
                         from
                             (select dealstamp, Buysell
                                from bi_corp_staging.rtra_regulatorio
                               where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
                                 and cpty != 'CLIENTE FICTICIO ARGENTINA'
                              union all
                              select dealstamp, Buysell
                                from bi_corp_staging.rtra_economico
                               where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
                                 and cpty != 'CLIENTE FICTICIO ARGENTINA') a
                       )
insert into table bi_corp_bdr.test_jm_contr_otros
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}')
select DISTINCT
       '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' AS E0623_FEOPERAC,
       lpad('23100',5,"0") as E0623_S1EMP,
       lpad(nc.id_cto_bdr,9,"0") as E0623_CONTRA1,
       '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' as E0623_FEC_MES,
       case when nvl(
                     datediff(to_date(from_unixtime(UNIX_TIMESTAMP(cast(nvl(maturity, '31/12/9999') as varchar(10)),"dd/MM/yyyy"))),
                              to_date(from_unixtime(UNIX_TIMESTAMP(cast( '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' as varchar(10)),"yyyy-MM-dd"))))
                    ,0) >= 0
            then 
               (
                  case when lpad(concat(nvl(
                                  datediff(to_date(from_unixtime(UNIX_TIMESTAMP(cast(nvl(maturity, '31/12/9999') as varchar(10)),"dd/MM/yyyy"))),
                                           to_date(from_unixtime(UNIX_TIMESTAMP(cast( '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' as varchar(10)),"yyyy-MM-dd"))))
                                  ,0),'000000'),
                       17,'0') = '00000000000000000'
                     then '00000999999000000'
                     else 
                        lpad(concat(nvl(
                                  datediff(to_date(from_unixtime(UNIX_TIMESTAMP(cast(nvl(maturity, '31/12/9999') as varchar(10)),"dd/MM/yyyy"))),
                                           to_date(from_unixtime(UNIX_TIMESTAMP(cast( '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' as varchar(10)),"yyyy-MM-dd"))))
                                  ,0),'000000'),
                       17,'0')
                  end
               )
            else
               (
                  case when 
                                lpad(concat(regexp_replace(datediff(to_date(from_unixtime(UNIX_TIMESTAMP(cast(nvl(maturity, '31/12/9999') as varchar(10)),"dd/MM/yyyy"))),
                                                                    to_date(from_unixtime(UNIX_TIMESTAMP(cast( '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' as varchar(10)),"yyyy-MM-dd")))),
                                                           "\\-",""),
                                            '000000'),
                                     16,'0') = '0000000000000000'
                  then  '0000999999000000'
                  else 
                     concat( '-',
                                lpad(concat(regexp_replace(datediff(to_date(from_unixtime(UNIX_TIMESTAMP(cast(nvl(maturity, '31/12/9999') as varchar(10)),"dd/MM/yyyy"))),
                                                                    to_date(from_unixtime(UNIX_TIMESTAMP(cast( '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' as varchar(10)),"yyyy-MM-dd")))),
                                                           "\\-",""),
                                            '000000'),
                                     16,'0')
                     )
                  end
               )
                    
              end as E0623_VCTO_RES,
       lpad(0, 17, '0')  as E0623_VCTO_PON,
       lpad(61026,5,'0')  as E0623_IDSUBPRD,
       lpad(0, 5, '0') AS  E0623_TIP_LIQU,
       lpad(0, 17, '0') AS E0623_LIQ_PZO,
       lpad(0, 5, '0') AS  E0623_TIP_AMOR,
       lpad(0, 17, '0') AS E0623_AMOR_PZO,
       lpad(0, 5, '0') AS  E0623_AMOR_SIS,
       lpad(0, 5, '0') AS  E0623_CTB_SITU,
       lpad(0, 5, '0') AS  E0623_GEST_SIT,
       lpad(0, 5, '0') AS  E0623_GES2_SIT,
       lpad(0, 9, '0') AS  E0623_ATAEMAX,
       lpad(0, 9, '0') AS  E0623_TIP_INT,
       concat('+', lpad(0, 16, '0')) AS E0623_IMP1LIMI,
       concat('+', lpad(0, 16, '0')) AS E0623_ALIMACT,
       concat('+', lpad(0, 16, '0')) AS E0623_IMPORTH,
       lpad(
            case when trim(cargabal) = '1314119E' then '1'
                 when trim(cargabal) = '130541E'  then '1'
                 when trim(cargabal) = '131103E'  then '3'
                 when trim(cargabal) = '130241E'  then '3'
                 when trim(cargabal) = '131111E'  then '3'
                 else '0' end
            , 5, '0') AS E0623_INV_NEGO,
       concat('+', lpad(0, 16, '0')) AS    E0623_IPROVISI,
       concat('+', lpad(0, 16, '0')) AS    E0623_IPROVIS1,
       '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' AS E0623_FECULTMO,
       lpad(0, 5, '0') AS E0623_ESTADTRJ,
       lpad(0, 17, '0') AS E0623_INACTRJ,
       lpad(0, 5, '0') AS E0623_UNNT,
       lpad(0, 5, '0') AS E0623_UNNTS,
       lpad(0, 5, '0') AS E0623_UNNV,
       lpad(0, 5, '0') AS E0623_UNNVS,
       ' ' AS  E0623_RGOSUB,
       '0001-01-01' AS E0623_FECINCAR,
       '0001-01-01' AS E0623_FECFICAR,
       ' ' as E0623_INTNEG,
       lpad(0, 5, '0') AS E0623_MTVALTA,
       ' ' AS  E0623_INDSUBRO,
       lpad(0, 5, '0') AS  E0623_TIP_INTE,
       lpad(0, 9, '0') AS  E0623_DIFERNCI,
       concat('+', lpad(0, 16, '0')) AS E0623_IMPRTCUO,
       ' ' AS E0623_INDSEGUR,
       ' ' AS E0623_AMORTPAR,
       '9999-12-31' AS E0623_FECIMPAG,
       concat('-', lpad(0, 16, '0')) AS    E0623_IMPPRIMP,
       '9999-12-31' AS E0623_FHPRIMPG,
       concat('-', lpad(0, 16, '0')) AS    E0623_IMPIMPNR,
       lpad(0, 5, '0') AS E0623_ESTPRINM,
       lpad(0, 5, '0') AS  E0623_EXCLCTO,
       lpad(0, 9, '0') AS  E0623_CUOTIMPA,
       concat('+', lpad(0, 16, '0')) AS E0623_LIMOCULT,
       lpad(0, 5, '0') AS E0623_CODIMPHI,
       ' ' AS  E0623_INDEUCON,
       lpad(0, 5, '0') AS E0623_TIPCAREN,
       concat('+', lpad(0, 16, '0')) AS E0623_CUOTPRES,
       nvl(rtra.Buysell,' ')  as E0623_IBUYSELL,
       lpad(0, 9, '0') AS  E0623_SUTIPINT,
       lpad(0, 9, '0') AS  E0623_TETIPINT,
       lpad(0, 5, '0') AS  E0623_TIPSUELO,
       lpad(0, 5, '0') AS  E0623_TIPTECHO,
       lpad(0, 5, '0') AS  E0623_PLREVTIN,
       '9999-12-31' AS E0623_FECCUOTA,
       lpad(0, 9, '0') AS E0623_NUDIAATR,
       lpad(0, 5, '0') AS  E0623_SEGPLLIM,
       concat('+', lpad(0, 16, '0')) AS    E0623_VOLTRANS,
       ' ' AS  E0623_MARCAUTI,
       lpad(0, 5, '0') AS  E0623_TOPDEALE,
       '9999-12-31' AS E0623_FECREPRE,
       "23100" as E0623_EMPREPOR,
       concat('+', lpad(0, 16, '0')) AS    E0623_IMPCUIMP,
       lpad(0, 9, '0') AS E0623_TIPINEFE,
       lpad(0, 5, '0') AS E0623_FORPGACT,
       '0001-01-01' AS E0623_FINIUTCT,
       '0001-01-01' AS E0623_FFINUTCT
  from bi_corp_staging.mmff_tactico_especie tac
 inner join bi_corp_bdr.normalizacion_id_contratos nc
            on nc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
           and nc.source = 'mmff-tactico'
           and nc.id_cto_source = concat_ws('_', trim(tac.cuenta),trim(tac.isin))
 left join ficheros_rtra rtra
            on rtra.dealstamp = concat(tac.especie,'-I')
where tac.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_mmff_tactico_especie', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}';


--Tactico Corresponsales
insert into table bi_corp_bdr.test_jm_contr_otros
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}')
select 
       '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' AS E0623_FEOPERAC
       ,lpad('23100',5,"0") as E0623_S1EMP
       ,lpad(nct.id_cto_bdr,9,"0") as E0623_CONTRA1 --Normalización de contrato
       ,'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' as E0623_FEC_MES
       ,case 
               when lpad(concat(nvl(
                        datediff(   to_date(from_unixtime(UNIX_TIMESTAMP(cast( concat(SUBSTRING('{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}',1,8), '05') as varchar(10)),"yyyy-MM-dd"))), 
                                    to_date(from_unixtime(UNIX_TIMESTAMP(cast( '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' as varchar(10)),"yyyy-MM-dd")))
                     )                        
                     ,0),'000000'),17,'0') = '00000000000000000'
               then '00000999999000000'
               else lpad(concat(nvl(
                        datediff(   to_date(from_unixtime(UNIX_TIMESTAMP(cast( concat(SUBSTRING('{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}',1,8), '05') as varchar(10)),"yyyy-MM-dd"))), 
                                    to_date(from_unixtime(UNIX_TIMESTAMP(cast( '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' as varchar(10)),"yyyy-MM-dd")))
                     )  
                     ,0),'000000'),17,'0')
         end  as E0623_VCTO_RES --Valor por defecto: Los días que queden hasta el penultimo día del mes siguiente
       ,lpad(0, 17, '0')  as E0623_VCTO_PON
       ,lpad(61001,5,'0')  as E0623_IDSUBPRD
       ,lpad(0, 5, '0') AS  E0623_TIP_LIQU
       ,lpad(0, 17, '0') AS E0623_LIQ_PZO
       ,lpad(0, 5, '0') AS  E0623_TIP_AMOR
       ,lpad(0, 17, '0') AS E0623_AMOR_PZO
       ,lpad(0, 5, '0') AS  E0623_AMOR_SIS
       ,lpad(0, 5, '0') AS  E0623_CTB_SITU
       ,lpad(99999, 5, '0') AS  E0623_GEST_SIT
       ,lpad(99999, 5, '0') AS  E0623_GES2_SIT
       ,lpad(0, 9, '0') AS  E0623_ATAEMAX
       ,lpad(0, 9, '0') AS  E0623_TIP_INT
       ,concat('+', lpad(0, 16, '0')) AS E0623_IMP1LIMI
       ,concat('+', lpad(0, 16, '0')) AS E0623_ALIMACT
       ,concat('+', lpad(0, 16, '0')) AS E0623_IMPORTH
       ,lpad(0, 5, '0') AS E0623_INV_NEGO
       ,concat('+', lpad(0, 16, '0')) AS    E0623_IPROVISI
       ,concat('+', lpad(0, 16, '0')) AS    E0623_IPROVIS1
       ,'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' AS E0623_FECULTMO
       ,lpad(0, 5, '0') AS E0623_ESTADTRJ
       ,lpad(0, 17, '0') AS E0623_INACTRJ
       ,lpad(0, 5, '0') AS E0623_UNNT
       ,lpad(0, 5, '0') AS E0623_UNNTS
       ,lpad(0, 5, '0') AS E0623_UNNV
       ,lpad(0, 5, '0') AS E0623_UNNVS
       ,' ' AS  E0623_RGOSUB
       ,'0001-01-01' AS E0623_FECINCAR
       ,'0001-01-01' AS E0623_FECFICAR
       ,' ' as E0623_INTNEG
       ,lpad(0, 5, '0') AS E0623_MTVALTA
       ,' ' AS  E0623_INDSUBRO
       ,lpad(0, 5, '0') AS  E0623_TIP_INTE
       ,lpad(0, 9, '0') AS  E0623_DIFERNCI
       ,concat('+', lpad(0, 16, '0')) AS E0623_IMPRTCUO
       ,' ' AS E0623_INDSEGUR
       ,' ' AS E0623_AMORTPAR
       ,'9999-12-31' AS E0623_FECIMPAG
       ,concat('-', lpad(0, 16, '0')) AS    E0623_IMPPRIMP
       ,'9999-12-31' AS E0623_FHPRIMPG
       ,concat('-', lpad(0, 16, '0')) AS    E0623_IMPIMPNR
       ,lpad(0, 5, '0') AS E0623_ESTPRINM
       ,lpad(0, 5, '0') AS  E0623_EXCLCTO
       ,lpad(0, 9, '0') AS  E0623_CUOTIMPA
       ,concat('+', lpad(0, 16, '0')) AS E0623_LIMOCULT
       ,lpad(0, 5, '0') AS E0623_CODIMPHI
       ,' ' AS  E0623_INDEUCON
       ,lpad(0, 5, '0') AS E0623_TIPCAREN
       ,concat('+', lpad(0, 16, '0')) AS E0623_CUOTPRES
       ,' '  as E0623_IBUYSELL
       ,lpad(0, 9, '0') AS  E0623_SUTIPINT
       ,lpad(0, 9, '0') AS  E0623_TETIPINT
       ,lpad(0, 5, '0') AS  E0623_TIPSUELO
       ,lpad(0, 5, '0') AS  E0623_TIPTECHO
       ,lpad(0, 5, '0') AS  E0623_PLREVTIN
       ,'9999-12-31' AS E0623_FECCUOTA
       ,lpad(0, 9, '0') AS E0623_NUDIAATR
       ,lpad(0, 5, '0') AS  E0623_SEGPLLIM
       ,concat('+', lpad(0, 16, '0')) AS    E0623_VOLTRANS
       ,' ' AS  E0623_MARCAUTI
       ,lpad(0, 5, '0') AS  E0623_TOPDEALE
       ,'9999-12-31' AS E0623_FECREPRE
       ,"23100" as E0623_EMPREPOR
       ,concat('+', lpad(0, 16, '0')) AS    E0623_IMPCUIMP
       ,lpad(0, 9, '0') AS E0623_TIPINEFE
       ,lpad(0, 5, '0') AS E0623_FORPGACT
       ,'0001-01-01' AS E0623_FINIUTCT
       ,'0001-01-01' AS E0623_FFINUTCT
from 
    (select *
       from bi_corp_staging.corresponsales crp
      where crp.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_corresponsales', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
    ) crp
left join 
    (select * 
        from bi_corp_bdr.normalizacion_id_contratos 
        where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
        and source = 'corresponsales-tactico' 
    ) nct
    on concat_ws('_', trim(crp.nup), trim(crp.moneda), trim(crp.rubro_altair)) = trim(nct.id_cto_source);