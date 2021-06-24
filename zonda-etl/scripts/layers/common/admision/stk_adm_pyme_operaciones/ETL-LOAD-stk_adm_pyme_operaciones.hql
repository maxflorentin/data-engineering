set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS tmp_maxpart_personas;
CREATE TEMPORARY TABLE tmp_maxpart_personas AS
select
    max(partition_date) AS partition_date
from bi_corp_common.stk_per_personas
where partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}',7)
  and partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'
  and cod_per_segmentoduro is not null;

DROP TABLE IF EXISTS personas_segmentoduro;
CREATE TEMPORARY TABLE personas_segmentoduro AS
select
    p.cod_per_nup,
    p.cod_per_segmentoduro
from bi_corp_common.stk_per_personas p
INNER JOIN tmp_maxpart_personas maxp on (p.partition_date=maxp.partition_date);


DROP TABLE IF EXISTS estadomaxfecha;
CREATE TEMPORARY TABLE estadomaxfecha AS
select
    e.cod_adm_secu_f487,
    e.ds_adm_sector,
    max(dt_adm_fec_log) as max_fecha
from bi_corp_common.stk_adm_pyme_estadosoperacion e
where e.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'
group by e.cod_adm_secu_f487, e.ds_adm_sector;


DROP TABLE IF EXISTS estadosoperacionsimplificado;
CREATE TEMPORARY TABLE estadosoperacionsimplificado AS
select
    e.cod_adm_secu_f487,
    e.ds_adm_sector,
    e.ds_adm_estado,
    e.ds_adm_log,
    e.cod_adm_estado_accion,
    e.cod_adm_estado_origen,
    emaxf.max_fecha as dt_adm_fec_log
from bi_corp_common.stk_adm_pyme_estadosoperacion e
inner join estadomaxfecha emaxf on emaxf.cod_adm_secu_f487 = e.cod_adm_secu_f487 and emaxf.max_fecha = e.dt_adm_fec_log and emaxf.ds_adm_sector = e.ds_adm_sector
where e.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'
group by e.cod_adm_secu_f487, e.ds_adm_sector, e.ds_adm_estado, e.ds_adm_log, e.cod_adm_estado_accion, e.cod_adm_estado_origen, emaxf.max_fecha;


INSERT OVERWRITE TABLE bi_corp_common.stk_adm_pyme_operaciones
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}')
select distinct
    concat('F487', lpad(f.secu_f487, 16 , "0")) as cod_adm_tramite,
    cast(f.penumper as int) as cod_adm_penumper,
    cast(f.secu_f487 as int) as cod_adm_secu_f487,
    f.cod_producto as cod_prod_producto,
    cast(f.nro_prop as int) as int_adm_nro_prop,
    cast(f.secuencia as int) as int_adm_secuencia,
    f.cod_operacion as cod_adm_operacion,
    cast(f.monto as double) as fc_adm_monto,
    f.pecodmon as ds_adm_pecodmon,
    cast(f.plazo as int) as int_adm_plazo,
    f.unidad_plazo as ds_adm_unidad_plazo,
    f.vencimiento as dt_adm_vencimiento,
    cast(f.tasa as double) as dec_adm_tasa,
    f.tipo_tasa as ds_adm_tipo_tasa,
    cast(f.costo as double) as dec_adm_costo,
    f.origen_fondos as ds_adm_origen_fondos,
    f.destino_fondos as ds_adm_destino_fondos,
    f.clase_operacion as ds_adm_clase_operacion,
    f.fecha_acreditacion as dt_adm_fecha_acreditacion,
    cast(f.numero_cta_cte as int) as int_adm_numero_cta_cte,
    cast(f.plazo_repacto as int) as int_adm_plazo_repacto,
    f.unidad_plazo_repacto as ds_adm_unidad_plazo_repacto,
    f.sistema_amortizacion as ds_adm_sistema_amortizacion,
    f.comentarios as ds_adm_comentarios,
    f.observaciones as ds_adm_observaciones,
    f.dentro_limite as flag_adm_dentro_limite,
    f.estado_f487 as ds_adm_estado_f487,
    f.fecha_operacion as dt_adm_fecha_operacion,
    cast(f.deuda_actual_resto_grupo as double) as dec_adm_deuda_actual_resto_grupo,
    cast(f.maximo_debido_resto_grupo as double) as dec_adm_maximo_debido_resto_grupo,
    cast(f.id_prestamo_altair as int) as id_adm_prestamo_altair,
    f.cod_iva as cod_adm_iva,
    f.cod_iibb as cod_adm_iibb,
    f.agen_perc_iibb as flag_adm_agen_perc_iibb,
    cast(f.porc_reducc_iibb as double) as dec_adm_porc_reducc_iibb,
    f.fec_reducc_iibb as dt_adm_fec_reducc_iibb,
    cast(f.porc_reducc_sell as double) as dec_adm_porc_reducc_sell,
    f.fec_reducc_sell as dt_adm_fec_reducc_sell,
    f.pacto_fiscal as ds_adm_pacto_fiscal,
    f.cod_pacto_fiscal as cod_adm_pacto_fiscal,
    f.datos_seguros as ds_adm_datos_seguros,
    f.pefecalt as dt_adm_pefecalt,
    f.peusualt as ds_adm_peusualt,
    f.pefecmod as dt_adm_pefecmod,
    f.peusumod as ds_adm_peusumod,
    f.tasa_especial as ds_adm_tasa_especial,
    f.tipo_cta_cte as cod_tipo_cta_cte,
    cast(f.sucursal_cta_cte as int) as cod_adm_sucursal_cta_cte,
    f.mar_comunidad as flag_adm_mar_comunidad,
    f.cod_comunidad as cod_adm_comunidad,
    cast(f.mon_comunidad as double) as fc_mon_comunidad,
    f.operacion_especial as ds_adm_operacion_especial,
    cast(f.cant_cheques as int) as int_adm_cant_cheques,
    cast(f.porcent_cesion as double) as dec_adm_porcent_cesion,
    f.usu_liquidacion as ds_adm_usu_liquidacion,
    f.fecha_liquidacion as dt_adm_fecha_liquidacion,
    f.nro_acuerdo as ds_adm_nro_acuerdo,
    f.periodo_liq as ds_adm_periodo_liq,
    f.cod_estado_accion as cod_adm_estado_accion,
    f.cod_estado_origen as cod_adm_estado_origen,
    f.obs_ultimo_estado_stnd as ds_adm_obs_ultimo_estado_stnd,
    cast(f.mon_f487_original as double) as fc_adm_mon_f487_original,
    cast(f.plazo_f487_original as int) as int_adm_plazo_f487_original,
    cast(f.nro_comprobante as int) as int_adm_nro_comprobante,
    f.mar_export_cesion as flag_adm_mar_export_cesion,
    cast(f.tsa_sugerida as double) as dec_adm_tsa_sugerida,
    f.usu_liquidacion_net as ds_adm_usu_liquidacion_net,
    f.cod_usuario_net as cod_adm_usuario_net,
    cast(f.tasamaxima as double) as dec_adm_tasamaxima,
    f.seg_comercial_duro as ds_adm_seg_comercial_duro,
    f.mar_repauto as flag_adm_mar_repauto,
    cast(f.monto_anticipo as double) as fc_adm_monto_anticipo,
    f.pecodmon_anticipo as ds_adm_pecodmon_anticipo,
    f.mar_cubre_intereses as flag_adm_mar_cubre_intereses,
    cast(f.id_comite as int) as id_adm_comite,
    f.nro_op_sam as ds_adm_nro_op_sam,
    cast(f.secu_padre_f487 as int) as int_adm_secu_padre_f487,
    cast(f.comision as double) as dec_adm_comision,
    f.mar_doc_en_tramite as flag_adm_mar_doc_en_tramite,
    f.mar_contingencia as flag_adm_mar_contingencia,
    f.cod_sub_producto as cod_adm_sub_producto,
    f.cod_canal_sam as cod_adm_canal_sam,
    cast(f.tasa_cliente as double) as dec_adm_tasa_cliente,
    f.tipo_tasa_cliente as cod_adm_tipo_tasa_cliente,
    f.cod_cargo_aut_ultimo_estado as cod_adm_cargo_aut_ultimo_estado,
    f.nombre_archivo_bma as ds_adm_nombre_archivo_bma,
    f.cod_dentro_limite_ult_estado as cod_adm_dentro_limite_ult_estado,
    f.mar_waiver as flag_adm_mar_waiver,
    f.cod_periodointeres as cod_adm_periodointeres,
    f.mar_bma as flag_adm_mar_bma,
    cast(f.tipo_costo as int) as cod_adm_tipo_costo,
    f.mar_dentro_limite_ult_estado as flag_adm_mar_dentro_limite_ult_estado,
    f.mar_devolucion_ult_estado as flag_adm_mar_devolucion_ult_estado,
    cast(f.tipo_comision as int) as cod_adm_tipo_comision,
    f.inicio_cobro as int_adm_periodicidad_cobro,
    cast(f.periodicidad_cobro as int) as int_adm_periodicidad_cobro,
    f.primer_pago_intereses as dt_adm_primer_pago_intereses,
    cast(f.tipo_operacion as int) as cod_adm_tipo_operacion,
    cast(f.tasa_repacto as double) as dec_adm_tasa_repacto,
    f.mar_aprueba_tasa_especial as flag_adm_mar_aprueba_tasa_especial,
    cast(f.tipo_operacion_esp as int) as int_adm_tipo_operacion_esp,
    cast(f.cuit as bigint) as int_adm_cuit,
    f.razon_social as ds_adm_razon_social,
    f.obs_repacto as ds_adm_obs_repacto,
    f.fec_adelanto_cesion as dt_adm_fec_adelanto_cesion,
    f.mar_otrorango as ds_adm_mar_otrorango,
    s.cod_canal_origen as cod_adm_canal_origen,
    s.cod_analista_srs as cod_adm_analista_srs,
    s.cod_supervisor_srs as cod_adm_supervisor_srs,
    s.cod_analista_sup as cod_adm_analista_sup,
    s.cod_supervisor_sup as cod_adm_supervisor_sup,
    s.cod_resol_srs as cod_adm_resol_srs,
    s.obs_resol_srs as ds_adm_obs_resol_srs,
    s.cod_resol_sup as cod_adm_resol_sup,
    s.obs_resol_sup as ds_adm_obs_resol_sup,
    s.lote_fecha as ds_adm_lote_fecha,
    s.usu_codigo as ds_adm_usu_codigo,
    cast(s.lote_num as int) as int_adm_lote_num,
    s.obs_baja as ds_adm_obs_baja,
    s.mar_reutiliza_veraz as flag_adm_mar_reutiliza_veraz,
    s.fec_carga as dt_adm_fec_carga,
    s.fec_cierre as dt_adm_fec_cierre,
    s.fec_envio_sw as dt_adm_fec_envio_sw,
    s.fec_respuesta_sw as dt_adm_fec_respuesta_sw,
    s.fec_ini_resol_zg as dt_adm_fec_ini_resol_zg,
    s.fec_fin_resol_zg as dt_adm_fec_fin_resol_zg,
    s.fec_ini_resol_sup as dt_adm_fec_ini_resol_sup,
    s.fec_fin_resol_sup as dt_adm_fec_fin_resol_sup,
    s.fec_envio_veraz as dt_adm_fec_envio_veraz,
    s.fec_respuesta_veraz as dt_adm_fec_respuesta_veraz,
    s.fec_ini_resol_op as dt_adm_fec_ini_resol_op,
    s.fec_fin_resol_op as dt_adm_fec_fin_resol_op,
    s.tipo_acuerdo as ds_adm_tipo_acuerdo,
    s.mar_firma_obe as flag_adm_mar_firma_obe,
    s.mar_tipo_acuerdo as flag_adm_mar_tipo_acuerdo,
    cast(s.monto_original_acuerdo as double) as fc_adm_monto_original_acuerdo,
    cast(s.tasa_tea as double) as dec_adm_tasa_tea,
    cast(s.cft as double) as dec_adm_cft,
    cast(s.tsa_maxima as double) as dec_adm_tsa_maxima,
    cast(s.tasa_tea_max as double) as dec_adm_tasa_tea_max,
    cast(s.cft_max as double) as dec_adm_cft_max,
    cast(s.tsa_exceso as double) as dec_adm_tsa_exceso,
    cast(s.tasa_tea_exc as double) as dec_adm_tasa_tea_exc,
    cast(s.cft_exc as double) as dec_adm_cft_exc,
    s.val_mapa_riesgo as ds_adm_val_mapa_riesgo,
    s.val_fecha_mapa as ds_adm_val_fecha_mapa,
    cast(s.nro_tramite_paq as int) as int_adm_nro_tramite_paq,
    s.indicador_riesgo as ds_adm_indicador_riesgo,


    escoring.ds_adm_estado as ds_adm_estado_resolucion_scoring,
    escoring.ds_adm_log as ds_adm_log_scoring,
    escoring.dt_adm_fec_log as dt_adm_fec_resol_scoring,
    escoring.cod_adm_estado_accion as cod_adm_estado_scoring,
    escoring.cod_adm_estado_origen as cod_adm_estado_origen_scoring,

    ezg.ds_adm_estado as ds_adm_estado_resolucion_zona_gris,
    ezg.ds_adm_log as ds_adm_log_zona_gris,
    ezg.dt_adm_fec_log as dt_adm_fec_resol_zona_gris,
    ezg.cod_adm_estado_accion as cod_adm_estado_zona_gris,
    ezg.cod_adm_estado_origen as cod_adm_estado_origen_zona_gris,

    case
        when ezg.ds_adm_estado is not null then ezg.ds_adm_estado
        when escoring.ds_adm_estado is not  NULL then escoring.ds_adm_estado
        else ''
    end as ds_adm_estado_resolucion_scoring_zona_gris,

    case
        when ezg.ds_adm_log is not null then ezg.ds_adm_log
        when escoring.ds_adm_log is not  NULL then escoring.ds_adm_log
        else ''
    end as ds_adm_log_scoring_zona_gris,

    case
        when ezg.dt_adm_fec_log is not null then ezg.dt_adm_fec_log
        when escoring.dt_adm_fec_log is not  NULL then escoring.dt_adm_fec_log
        else ''
    end as dt_adm_fec_resol_scoring_zona_gris,

    case
        when ezg.cod_adm_estado_accion is not null then ezg.cod_adm_estado_accion
        when escoring.cod_adm_estado_accion is not  NULL then escoring.cod_adm_estado_accion
        else ''
    end as cod_adm_estado_scoring_zona_gris,

    case
        when ezg.cod_adm_estado_origen is not null then ezg.cod_adm_estado_origen
        when escoring.cod_adm_estado_origen is not  NULL then escoring.cod_adm_estado_origen
        else ''
    end as cod_adm_estado_origen_scoring_zona_gris,

    esupervision.ds_adm_estado as ds_adm_estado_resolucion_supervision,
    esupervision.ds_adm_log as ds_adm_log_supervision,
    esupervision.dt_adm_fec_log as dt_adm_fec_resol_supervision,
    esupervision.cod_adm_estado_accion as cod_adm_estado_supervision,
    esupervision.cod_adm_estado_origen as cod_adm_estado_origen_supervision,

    cast(pp.cod_per_nup as int) as cod_adm_nup,
    psd.cod_per_segmentoduro as cod_adm_segmentoduro,
    pp.ds_per_segmento as ds_adm_segmento,
    pp.ds_per_subsegmento as ds_adm_subsegmento,
    pp.cod_per_cuadrante as cod_adm_cuadrante,

    prop.cod_adm_origen as cod_adm_origen

from
    bi_corp_staging.sge_f487 f
left join bi_corp_staging.sge_stnd_datos_adicionales_f487 s on
    s.secu_f487 = f.secu_f487 and
    s.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'
left join estadosoperacionsimplificado escoring on
    escoring.cod_adm_secu_f487 = cast(f.secu_f487 as int) and
    lower(escoring.ds_adm_sector) like '%scoring%'
left join estadosoperacionsimplificado ezg on
    ezg.cod_adm_secu_f487 = cast(f.secu_f487 as int) and
    lower(ezg.ds_adm_sector) like '%zona gris%'
left join estadosoperacionsimplificado esupervision on
    esupervision.cod_adm_secu_f487 = cast(f.secu_f487 as int) and
    lower(esupervision.ds_adm_sector) like '%supervision%'
left join bi_corp_staging.sge_personas p on
    p.penumper = f.penumper and
    p.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'
left join bi_corp_common.stk_per_personas pp on
    pp.cod_per_nup = cast(p.nup as int) and
    pp.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'
left join
    bi_corp_common.stk_adm_propuestas prop on prop.cod_adm_nro_prop = cast(f.nro_prop as int) and
    prop.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'
left join
    personas_segmentoduro psd on psd.cod_per_nup =  cast(p.nup as int)
where f.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}';