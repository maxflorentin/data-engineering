set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.stk_cys_contratos
partition(partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_GarraMoria-Daily') }}')

SELECT
cast(w.waguxdex_num_persona as bigint) as cod_per_nup,
cast(w.waguxdex_cod_centro as int) as cod_suc_sucursal,
cast(w.waguxdex_num_contrato as bigint) as cod_nro_cuenta,
w.waguxdex_cod_producto as cod_prod_producto,
w.waguxdex_cod_subprodu as cod_prod_subproducto,
w.waguxdex_cod_divisa as cod_div_divisa,
case when to_date(w.waguxdex_fec_inisitir) in ('9999-12-31', '0001-01-01') then NULL else to_date(w.waguxdex_fec_inisitir) end as dt_cys_fechasituacionirregular,
case when to_date(w.waguxdex_fec_dudosida) in ('9999-12-31', '0001-01-01') then NULL else to_date(w.waguxdex_fec_dudosida) end as dt_cys_fechadudosidad,
w.waguxdex_cod_marca as cod_cys_marca,
w.waguxdex_cod_smarcgsi as cod_cys_submarca,
w.waguxdex_imp_riesmolo as fc_cys_importeriesgo,
w.waguxdex_imp_irremolo as fc_cys_importeirregular,
w.waguxdex_imp_castmolo as fc_cys_importecastigo,
w.waguxdex_imp_pendmolo as fc_cys_importependiente,
w.waguxdex_imp_quitmolo as fc_cys_importequita,
w.waguxdex_num_posicion as ds_cys_numposicion,
case when to_date(w.waguxdex_fecha_altareg) in ('9999-12-31', '0001-01-01') then NULL else to_date(w.waguxdex_fecha_altareg) end as dt_cys_fechaaltaregistro,
case when to_date(w.waguxdex_fec_marcsubm) in ('9999-12-31', '0001-01-01') then NULL else to_date(w.waguxdex_fec_marcsubm) end as dt_cys_fechamarcasubmit,
case when to_date(w.waguxdex_fec_alt_prod) in ('9999-12-31', '0001-01-01') then NULL else to_date(w.waguxdex_fec_alt_prod) end as dt_cys_fechaltaproducto,
w.waguxdex_imp_cap_venc as fc_cys_importecapvencido,
w.waguxdex_imp_int_venc as fc_cys_importeintvencido,
w.waguxdex_imp_otr_tot as fc_cys_importeotros,
w.waguxdex_imp_cap_a_venc as fc_cys_importecapvencer,
case when to_date(w.waguxdex_fec_vto_prod) in ('9999-12-31', '0001-01-01') then NULL else to_date(w.waguxdex_fec_vto_prod) end as dt_cys_fechavencproducto,
w.waguxdex_nro_cuo_act as ds_num_cuotaactual,
w.waguxdex_nro_cuo_tot as ds_num_cuotatotal,
w.waguxdex_cod_amort_cap as cod_cys_amortizacion,
case when w.waguxdex_cod_destino = '' then NULL else w.waguxdex_cod_destino end as cod_cys_destinofondos,
case when to_date(w.waguxdex_fec_cambio_nor) in ('9999-12-31', '0001-01-01') then NULL else to_date(w.waguxdex_fec_cambio_nor) end as dt_cys_fechacambioclasificacionreestructuracion,
case when w.waguxdex_motivo_riesgo_sub_est = '' then NULL else w.waguxdex_motivo_riesgo_sub_est end as ds_cys_motivoriesgosubestado,
case when w.waguxdex_ind_arrastre = '' then NULL else w.waguxdex_ind_arrastre end as ds_cys_indicaarrastre,
case when to_date(w.waguxdex_fec_inigra) in ('9999-12-31', '0001-01-01') then NULL else to_date(w.waguxdex_fec_inigra) end as dt_cys_fechainiciogracia,
case when to_date(w.waguxdex_fec_fingra) in ('9999-12-31', '0001-01-01') then NULL else to_date(w.waguxdex_fec_fingra) end as dt_cys_fechafingracia,
case when w.waguxdex_plz_plazo = '' then NULL else w.waguxdex_plz_plazo end as fc_cys_plazo,
w.waguxdex_con_diaimpag as fc_cys_diasatraso,
w.waguxdex_nro_cuotaco as fc_cys_cuotaspagasconsecutivas,
case when w.waguxdex_cla_garantia = '' then NULL else w.waguxdex_cla_garantia end as ds_cys_clasegarantia,
case when w.waguxdex_ind_normaliz = '' then NULL else w.waguxdex_ind_normaliz end as ds_cys_normalizado,
w.waguxdex_imp_inter_ord as fc_cys_interesesordinarios,
case when w.waguxdex_tip_reestruct = '' then NULL else w.waguxdex_tip_reestruct end as ds_cys_tiporeestructuracion,
case when w.waguxdex_cod_marca_subjetiva = '' then NULL else w.waguxdex_cod_marca_subjetiva end as flag_cys_marcasubjetiva,
case when w.waguxdex_ind_proc_nor = '' then NULL else w.waguxdex_ind_proc_nor end as flag_cys_procesonormalizacion,
case when w.waguxdex_ind_incumplim = '' then NULL else w.waguxdex_ind_incumplim end as flag_cys_marcaincumplimiento,
case when to_date(w.waguxdex_fec_incumplim) in ('9999-12-31', '0001-01-01') then NULL else to_date(w.waguxdex_fec_incumplim) end  as dt_cys_fechaincumplimiento,
case when w.waguxdex_cod_atrib_seg_esp = '' then NULL else w.waguxdex_cod_atrib_seg_esp end as ds_cys_clasificacionreestructuracion,
case s.segmento when 'INDIVIDUOS' then p.categoria_particulares else p.categoria_carterizados end as ds_cys_categoriaproducto,
trim(s.segmento_control) as ds_cys_descripcionsegmento,
case when coalesce(w.waguxdex_con_diaimpag,0) = 0 then '0' when w.waguxdex_con_diaimpag >= 1 AND w.waguxdex_con_diaimpag <= 30 then '1-30' when w.waguxdex_con_diaimpag >= 30 AND w.waguxdex_con_diaimpag <= 60 then '31-60' when w.waguxdex_con_diaimpag >= 61 AND w.waguxdex_con_diaimpag <= 90 then '61-90' when w.waguxdex_con_diaimpag >= 91 AND w.waguxdex_con_diaimpag <= 120 then '91-120' when w.waguxdex_con_diaimpag >= 121 AND w.waguxdex_con_diaimpag<= 150 then '121-150' when w.waguxdex_con_diaimpag>= 151 AND w.waguxdex_con_diaimpag<= 180 then '151-180' else 'MAS DE 180' end as ds_cys_bucket,
trim(s.ramo) as cod_cys_segmento,
trim(s.subsegmento) as cod_cys_subsegmento
FROM bi_corp_staging.garra_wagucdex w
LEFT JOIN bi_corp_staging.malpe_ptb_pedt030 p30 on w.waguxdex_num_persona = p30.penumper and p30.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_GarraMoria-Daily') }}'
LEFT JOIN bi_corp_staging.risksql5_segmentos s ON trim(p30.pesegcla) = trim(s.ramo) and s.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_GarraMoria-Daily') }}'
LEFT JOIN bi_corp_staging.risksql5_productos p ON trim(w.waguxdex_cod_producto) = trim(p.codigo_producto) and trim(w.waguxdex_cod_subprodu) = trim(p.codigo_subproducto)  and p.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_GarraMoria-Daily') }}'
WHERE w.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_GarraMoria-Daily') }}'
;