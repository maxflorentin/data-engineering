set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

CREATE TEMPORARY TABLE libradores AS
SELECT bco_girado, suc_girada, cod_postal, nro_cheque, cta_girada, nro_cuit
FROM (
    select fir.bco_girado, fir.suc_girada, fir.cod_postal, fir.nro_cheque, fir.cta_girada, fir.nro_cuit, ROW_NUMBER () OVER ( partition by fir.bco_girado, fir.suc_girada, fir.cod_postal, fir.nro_cheque, fir.cta_girada order by fir.tim_alta asc ) as rownum
    from bi_corp_staging.malzx_alzxufir  fir
    where fir.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}'
) x
WHERE rownum = 1
;


insert overwrite table bi_corp_common.stk_chq_cesionados
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}')

SELECT DISTINCT
lpad(chq.bco_girado, 4, '0') as cod_chq_entidad,
cast(chq.nro_cheque as bigint) as cod_chq_nrocheque,
cast(chq.cta_girada as bigint) as cod_chq_cuentacheque,
cast(lpad(chq.suc_girada, 4, '0') as bigint) as cod_chq_sucursalgirada,
cast(trim(chq.cod_postal) as int) as cod_chq_codigopostal,
cast(concat('000', substr(cab.cta_cliente,4)) as bigint) as cod_chq_cuentacorriente,
cast(null as string) as cod_chq_paquete,
cast(null as string) as cod_chq_tipopaquete,
cast(cab.cod_nup as bigint) as cod_per_nup,
p30.pesegcla as cod_per_segmentoduro,
p30.pesegcal as cod_chq_agrupacionsegmento,
cast(null as string) as cod_chq_subsegmento,
cast(lpad(chq.suc_lote, 4, '0') as bigint) as cod_suc_sucursal,
chq.cod_producto as cod_chq_producto,
chq.cod_subproducto as cod_chq_subproducto,
trim(pro.des_corta) as ds_chq_producto,
cg.cod_producto as cod_chq_productocontrato,
cg.cod_subprodu as cod_chq_subproductocontrato,
cab.cod_divisa as cod_chq_divisa,
trim(fir.nro_cuit) as ds_chq_nrocuit,
case when to_date(cg.fec_alta_registro) in ('9999-12-31', '0001-01-01') then cast(null as string) else to_date(cg.fec_alta_registro) end as dt_chq_fechaaltaregistro,
case when to_date(cg.fec_alta_producto) in ('9999-12-31', '0001-01-01') then cast(null as string) else to_date(cg.fec_alta_producto) end as dt_chq_fechaaltaproducto,
case when to_date(cg.fec_vencimiento_del_producto) in ('9999-12-31', '0001-01-01') then cast(null as string) else to_date(cg.fec_vencimiento_del_producto) end as dt_chq_fechavtoproducto,
cast(null as string) as dt_chq_fechaaltamarcaemerix,
cast(null as string) as dt_chq_fechaaltasubmarcaemerix,
case when cg.fec_incumplim in ('9999-12-31', '0001-01-01') then NULL else to_date(cg.fec_incumplim) end as dt_chq_fechasituacionirregular,
cg.cod_marca as cod_chq_marca,
cg.cod_submarca as cod_chq_submarca,
cast(null as int) as flag_chq_procesojudicial,
cast(null as string) as cod_chq_marcaemerix,
cast(null as string) as cod_chq_submarcaemerix,
cast(null as int) as flag_chq_contratociti,
chq.cod_programa as cod_chq_programa,
chq.nro_lote as cod_chq_nrolote,
case when to_date(cab.fec_lectura) in ('9999-12-31', '0001-01-01') then cast(null as string) else to_date(cab.fec_lectura) end as dt_chq_fechalectura,
case when to_date(chq.fec_presentacion) in ('9999-12-31', '0001-01-01') then cast(null as string) else to_date(chq.fec_presentacion) end as dt_chq_fechapresentacion,
case when to_date(chq.fec_valor) in ('9999-12-31', '0001-01-01') then cast(null as string) else to_date(chq.fec_valor) end as dt_chq_fechavalor,
case when to_date(chq.fec_vencimiento) in ('9999-12-31', '0001-01-01') then cast(null as string) else to_date(chq.fec_vencimiento) end as dt_chq_fechavto,
case when to_date(chq.fec_compensacion) in ('9999-12-31', '0001-01-01') then cast(null as string) else to_date(chq.fec_compensacion) end as dt_chq_fechacompensacion,
case when to_date(chq.ult_fec_diferimien) in ('9999-12-31', '0001-01-01') then cast(null as string) else to_date(chq.ult_fec_diferimien) end as dt_chq_ultimafechadiferimiento,
case when to_date(chq.fec_cambio_est) in ('9999-12-31', '0001-01-01') then cast(null as string) else to_date(chq.fec_cambio_est) end as dt_chq_fechacambioest,
case when to_date(chq.fec_egreso_stock) in ('9999-12-31', '0001-01-01') then cast(null as string) else to_date(chq.fec_egreso_stock) end as dt_chq_fechaegresostock,
case when to_date(cab.fec_proceso) in ('9999-12-31', '0001-01-01') then cast(null as string) else to_date(cab.fec_proceso) end as dt_chq_fechaproceso,
case when to_date(cab.fec_imputacion) in ('9999-12-31', '0001-01-01') then cast(null as string) else to_date(cab.fec_imputacion) end as dt_chq_fechaimputacion,
case when to_date(cab.fec_recepcion) in ('9999-12-31', '0001-01-01') then cast(null as string) else to_date(cab.fec_recepcion) end as dt_chq_fecharecepcion,
case chq.mar_tsa_esp_uni when ' ' then cast(null as string) else chq.mar_bonif_imp_tsa end as cod_chq_marstaespuni,
chq.cod_canje_cheq as cod_chq_canjecheque,
chq.cod_canje_clte as cod_chq_canjeclte,
chq.cod_evento as cod_chq_evento,
chq.est_cheque as cod_chq_estcheque,
case ddf.dest_fondo when '     ' then cast(null as string) else ddf.dest_fondo end as cod_chq_destinofondos,
trim(substring(t.gen_datos, 1, 40)) as ds_chq_destinofondos,
case chq.mar_bonif_imp_tsa when ' ' then cast(null as string) else chq.mar_bonif_imp_tsa end as cod_chq_marbonifimptsa,
cab.cod_canal cod_chq_canal,
cg.imp_riesgo_divisa_local_del_contrato as fc_chq_importeriesgo,
cg.imp_irregular_total_moneda_local as fc_chq_importeirregular,
coalesce(cast(trim(concat(substr(regexp_replace(chq.imp_cheque, "^0+", ''),1,length(regexp_replace(chq.imp_cheque, "^0+", ''))-2),'.',substr(regexp_replace(chq.imp_cheque, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_importecheque,
cg.imp_capital_a_vencer as fc_chq_importecapitalavencer,
cg.dias_de_impago as fc_chq_diasatraso,
coalesce(cast(trim(concat(substr(regexp_replace(chq.tsa_comision_cobr, "^0+", ''),1,length(regexp_replace(chq.tsa_comision_cobr, "^0+", ''))-2),'.',substr(regexp_replace(chq.tsa_comision_cobr, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_tsacomisioncobro,
coalesce(cast(trim(concat(substr(regexp_replace(chq.tsa_comision_def, "^0+", ''),1,length(regexp_replace(chq.tsa_comision_def, "^0+", ''))-2),'.',substr(regexp_replace(chq.tsa_comision_def, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_tsacomisiondef,
coalesce(cast(trim(concat(substr(regexp_replace(chq.tsa_comision_esp, "^0+", ''),1,length(regexp_replace(chq.tsa_comision_esp, "^0+", ''))-2),'.',substr(regexp_replace(chq.tsa_comision_esp, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_tsacomisionesp,
coalesce(cast(trim(concat(substr(regexp_replace(chq.tsa_comision_bon, "^0+", ''),1,length(regexp_replace(chq.tsa_comision_bon, "^0+", ''))-2),'.',substr(regexp_replace(chq.tsa_comision_bon, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_tsacomisionbon,
coalesce(cast(trim(concat(substr(regexp_replace(chq.porc_bonificacion, "^0+", ''),1,length(regexp_replace(chq.porc_bonificacion, "^0+", ''))-2),'.',substr(regexp_replace(chq.porc_bonificacion, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_porcentajebonificacion,
coalesce(cast(trim(concat(substr(regexp_replace(chq.tsa_fija_com, "^0+", ''),1,length(regexp_replace(chq.tsa_fija_com, "^0+", ''))-2),'.',substr(regexp_replace(chq.tsa_fija_com, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_tsafijacom,
coalesce(cast(trim(concat(substr(regexp_replace(chq.tsa_cost_corresp, "^0+", ''),1,length(regexp_replace(chq.tsa_cost_corresp, "^0+", ''))-2),'.',substr(regexp_replace(chq.tsa_cost_corresp, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_tsacostcorresp,
coalesce(cast(trim(concat(substr(regexp_replace(chq.cost_corresponsal, "^0+", ''),1,length(regexp_replace(chq.cost_corresponsal, "^0+", ''))-2),'.',substr(regexp_replace(chq.cost_corresponsal, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_costcorresponsal,
coalesce(cast(trim(concat(substr(regexp_replace(chq.spr_corresponsal, "^0+", ''),1,length(regexp_replace(chq.spr_corresponsal, "^0+", ''))-2),'.',substr(regexp_replace(chq.spr_corresponsal, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_sprcorresponsal,
coalesce(cast(trim(concat(substr(regexp_replace(chq.tsa_cost_financ, "^0+", ''),1,length(regexp_replace(chq.tsa_cost_financ, "^0+", ''))-2),'.',substr(regexp_replace(chq.tsa_cost_financ, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_tsacostfinanc,
coalesce(cast(trim(concat(substr(regexp_replace(chq.cost_financiero, "^0+", ''),1,length(regexp_replace(chq.cost_financiero, "^0+", ''))-2),'.',substr(regexp_replace(chq.cost_financiero, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_costofinanciero,
coalesce(cast(trim(concat(substr(regexp_replace(chq.spr_financiero, "^0+", ''),1,length(regexp_replace(chq.spr_financiero, "^0+", ''))-2),'.',substr(regexp_replace(chq.spr_financiero, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_sprfinanciero,
coalesce(cast(trim(concat(substr(regexp_replace(chq.tsa_cost_transacc, "^0+", ''),1,length(regexp_replace(chq.tsa_cost_transacc, "^0+", ''))-2),'.',substr(regexp_replace(chq.tsa_cost_transacc, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_tsacosttransaccional,
coalesce(cast(trim(concat(substr(regexp_replace(chq.cost_transaccional, "^0+", ''),1,length(regexp_replace(chq.cost_transaccional, "^0+", ''))-2),'.',substr(regexp_replace(chq.cost_transaccional, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_costtransaccional,
coalesce(cast(trim(concat(substr(regexp_replace(chq.spr_transaccional, "^0+", ''),1,length(regexp_replace(chq.spr_transaccional, "^0+", ''))-2),'.',substr(regexp_replace(chq.spr_transaccional, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_sprtransaccional,
coalesce(cast(trim(concat(substr(regexp_replace(chq.cost_minimo_corr, "^0+", ''),1,length(regexp_replace(chq.cost_minimo_corr, "^0+", ''))-2),'.',substr(regexp_replace(chq.cost_minimo_corr, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_costominimocorr,
coalesce(cast(trim(concat(substr(regexp_replace(chq.carg_fijo_adm_plan, "^0+", ''),1,length(regexp_replace(chq.carg_fijo_adm_plan, "^0+", ''))-2),'.',substr(regexp_replace(chq.carg_fijo_adm_plan, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_cargofijoadmplan,
coalesce(cast(trim(concat(substr(regexp_replace(chq.carg_fijo_adm_cobr, "^0+", ''),1,length(regexp_replace(chq.carg_fijo_adm_cobr, "^0+", ''))-2),'.',substr(regexp_replace(chq.carg_fijo_adm_cobr, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_cargofijoadmcobro,
coalesce(cast(trim(concat(substr(regexp_replace(chq.mon_fijo_corr, "^0+", ''),1,length(regexp_replace(chq.mon_fijo_corr, "^0+", ''))-2),'.',substr(regexp_replace(chq.mon_fijo_corr, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_montofijocorr,
coalesce(cast(trim(concat(substr(regexp_replace(chq.int_devengados, "^0+", ''),1,length(regexp_replace(chq.int_devengados, "^0+", ''))-2),'.',substr(regexp_replace(chq.int_devengados, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_interesesdevengados,
coalesce(cast(trim(concat(substr(regexp_replace(chq.int_devengados_tot, "^0+", ''),1,length(regexp_replace(chq.int_devengados_tot, "^0+", ''))-2),'.',substr(regexp_replace(chq.int_devengados_tot, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_interesesdevengadostotales,
coalesce(cast(trim(concat(substr(regexp_replace(chq.tsa_fija_int, "^0+", ''),1,length(regexp_replace(chq.tsa_fija_int, "^0+", ''))-2),'.',substr(regexp_replace(chq.tsa_fija_int, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_tsafijaint,
coalesce(cast(trim(concat(substr(regexp_replace(chq.tsa_especial_int, "^0+", ''),1,length(regexp_replace(chq.tsa_especial_int, "^0+", ''))-2),'.',substr(regexp_replace(chq.tsa_especial_int, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_tsaespecialint,
coalesce(cast(trim(concat(substr(regexp_replace(chq.tsa_interes, "^0+", ''),1,length(regexp_replace(chq.tsa_interes, "^0+", ''))-2),'.',substr(regexp_replace(chq.tsa_interes, "^0+", ''),-2)))as decimal(19, 4)),0) / 100 as fc_chq_tsainteres,
coalesce(cast(trim(concat(substr(regexp_replace(chq.imp_comision_cobr, "^0+", ''),1,length(regexp_replace(chq.imp_comision_cobr, "^0+", ''))-2),'.',substr(regexp_replace(chq.imp_comision_cobr, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_importecomisioncobro,
coalesce(cast(trim(concat(substr(regexp_replace(chq.imp_interes_tot, "^0+", ''),1,length(regexp_replace(chq.imp_interes_tot, "^0+", ''))-2),'.',substr(regexp_replace(chq.imp_interes_tot, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_importeinterestotal,
cast(null as decimal(15,2)) as fc_chq_disponibletotal
FROM bi_corp_staging.malzx_alzxucab cab
INNER JOIN bi_corp_staging.malzx_alzxuchq chq on chq.suc_lote = cab.suc_lote and chq.nro_lote = cab.nro_lote and chq.fec_lectura = cab.fec_lectura and chq.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}'
LEFT JOIN bi_corp_staging.malzx_alzxupro pro on pro.cod_producto = cab.cod_producto and pro.cod_subproducto = cab.cod_subproducto and pro.cod_programa = cab.cod_programa and pro.cod_divisa = cab.cod_divisa and pro.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}'
LEFT JOIN bi_corp_staging.malzx_alzxuddf ddf on ddf.suc_lote = cab.suc_lote and ddf.nro_lote = cab.nro_lote and ddf.fecha = cab.fec_lectura and ddf.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}'
LEFT JOIN libradores fir on fir.bco_girado = chq.bco_girado and fir.suc_girada = chq.suc_girada and fir.cod_postal = chq.cod_postal and fir.nro_cheque = chq.nro_cheque and fir.cta_girada = chq.cta_girada
LEFT JOIN santander_business_risk_arda.contratos_garra cg on concat(chq.bco_girado,chq.nro_cheque,substr(chq.cod_producto,-1,1)) = cg.num_cuenta and chq.cod_nup = cg.num_persona and cg.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date_filter', dag_id='LOAD_CMN_Cheques') }}'
LEFT JOIN bi_corp_staging.malpe_pedt030 p30 on cab.cod_nup = p30.penumper and p30.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}'
-- LEFT JOIN bi_corp_risk.segmentos s on s.segmento=trim(p30.pesegcla)
LEFT JOIN bi_corp_staging.tcdtgen t on trim(ddf.dest_fondo) = trim(t.gen_clave) and trim(t.gentabla) = '0301' and t.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}'
WHERE cab.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}'
AND chq.fec_lectura <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}'
AND chq.fec_egreso_stock >= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}'
;