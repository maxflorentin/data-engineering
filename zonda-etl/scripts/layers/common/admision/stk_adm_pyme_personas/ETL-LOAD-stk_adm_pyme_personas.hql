set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_pyme_personas
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}')
select distinct
    p.pecdgent as cod_adm_pecdgent,
    cast(p.penumper as int) as cod_adm_penumper,
    p.petipdoc as cod_adm_petipdoc,
    cast(p.penumdoc as int) as cod_adm_penumdoc,
    p.penomfan as cod_adm_penomfan,
    p.pepriape as ds_adm_pepriape,
    p.pesegape as cod_adm_pesegape,
    p.penomper as ds_adm_penomper,
    p.pefecing as dt_adm_pefecing,
    p.peestciv as flag_adm_peestciv,
    p.pesexper as flag_adm_pesexper,
    p.petipper as flag_adm_petipper,
    p.pefecnac as dt_adm_pefecnac,
    p.pefecini as dt_adm_pefecini,
    p.pecancap as cod_adm_pecancap,
    p.pepaiori as cod_adm_pepaiori,
    p.penacper as cod_adm_penacper,
    p.pepaires as cod_adm_pepaires,
    p.petipocu as cod_adm_petipocu,
    p.pecodact as cod_adm_pecodact,
    cast(p.petieres as int) as cod_adm_petieres,
    p.peforjur as cod_adm_peforjur,
    p.penatjur as cod_adm_penatjur,
    p.peestper as cod_adm_peestper,
    p.peconper as cod_adm_peconper,
    p.pecodemp as cod_adm_pecodemp,
    p.peindno2 as flag_adm_peindno2,
    p.peactriu as cod_adm_peactriu,
    p.pefecrev as dt_adm_pefecrev,
    p.peidioma as flag_adm_peidioma,
    p.peusualt as cod_adm_peusualt,
    p.pefecalt as dt_adm_pefecalt,
    p.peusumod as cod_adm_peusumod,
    p.petermod as cod_adm_petermod,
    p.psucmod as cod_adm_psucmod,
    p.pehstamp as cod_adm_pehstamp,
    p.penumgru as cod_adm_penumgru,
    cast(p.peuserid as int) as id_adm_peuser,
    cast(p.cuit_cliente as bigint) as int_adm_cuit_cliente,
    p.petipemp as cod_adm_petipemp,
    p.petieblc as flag_adm_petieblc,
    cast(p.pegfeve as int) as cod_adm_pegfeve,
    p.segmento as cod_adm_segmento,
    p.numgruant as cod_adm_numgruant,
    p.perescom as cod_adm_perescom,
    p.peobserv as ds_adm_peobserv,
    p.perubro as flag_adm_perubro,
    cast(p.nup as int) as cod_per_nup,
    p.pefecmod as dt_adm_pefecmod,
    cast(p.situacion as int) as cod_adm_situacion,
    cast(p.resp_comercial as int) as cod_adm_resp_comercial,
    cast(p.resp_riesgos as int) as cod_adm_resp_riesgos,
    p.cod_iva as cod_adm_iva,
    p.cod_iibb as cod_adm_iibb,
    p.actualizacion_host as flag_adm_actualizacion_host,
    p.mar_global as flag_adm_mar_global,
    cast(p.val_nivelger_item1 as int) as int_adm_val_nivelger_item1,
    cast(p.val_nivelger_item2 as int) as int_adm_val_nivelger_item2,
    cast(p.val_nivelger_item3 as int) as int_adm_val_nivelger_item3,
    cast(p.val_nivelger_item4 as int) as int_adm_val_nivelger_item4,
    cast(p.val_nivelger_item5 as int) as int_adm_val_nivelger_item5,
    cast(p.val_nivelger_item6 as int) as int_adm_val_nivelger_item6,
    cast(p.can_pers_ocupado as int) as int_adm_can_pers_ocupado,
    p.des_censo as ds_adm_censo,
    p.fec_carga_censo as dt_adm_fec_carga_censo,
    p.fec_venc_censo as dt_adm_fec_venc_censo,
    p.cod_actecon as cod_adm_actecon,
    p.pecodpai_nac as cod_adm_pecodpai_nac,
    cast(p.nro_solicitud_atril as int) as cod_adm_nro_solicitud_atril,
    p.fec_inactivacion as dt_adm_fec_inactivacion,
    p.cod_usu_inactivacion as cod_adm_usu_inactivacion,
    p.mar_uge as flag_adm_mar_uge,
    p.mar_afip_ok as flag_adm_mar_afip_ok,
    p.fecha_afip_ok as dt_adm_fecha_afip_ok,
    p.mar_contrato_marco as flag_adm_mar_contrato_marco,
    p.nivel_estudio as cod_adm_nivel_estudio,
    p.tipo_vivienda as flag_adm_tipo_vivienda,
    p.desc_actividad_bma as ds_adm_actividad_bma,
    cast(p.cod_actividad_santander as int) as cod_adm_actividad_santander,
    p.marca_resolucion_3337 as flag_adm_marca_resolucion_3337,
    p.marca_impuesto_endeudamiento as flag_adm_marca_impuesto_endeudamiento,
    cast(p.sucursal_cta_cte_bma as int) as cod_adm_sucursal_cta_cte_bma,
    cast(p.tipo_cta_cte_bma as int) as cod_adm_tipo_cta_cte_bma,
    cast(p.numero_cta_cte_bma as int) as cod_adm_numero_cta_cte_bma,
    cast(p.monto_facturacion as double) as fc_adm_monto_facturacion,
    p.com5603 as cod_adm_com5603,
    p.cod_act_bma as cod_adm_act_bma,
    cast(p.resp_riesgos_por_omision as int) as cod_adm_resp_riesgos_por_omision,
    p.segmento_por_omision as cod_adm_segmento_por_omision,
    p.segmento_riesgo as segmento_riesgo,
    j.cod_usuario as cod_adm_responsable_riesgos_legajo,
    u.nombre_usuario as ds_adm_responsable_riesgos_nombre,
    j2.cod_usuario as cod_adm_responsable_comercial_legajo,
    u2.nombre_usuario as ds_adm_responsable_comercial_nombre
from bi_corp_staging.sge_personas p
left join bi_corp_staging.sge_jerarquia_cargos j on j.id = p.resp_riesgos and j.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'
left join bi_corp_staging.sge_jerarquia_cargos j2 on j2.id = p.resp_comercial and j2.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'
left join bi_corp_staging.sge_usuarios u on u.cod_usuario = j.cod_usuario and u.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'
left join bi_corp_staging.sge_usuarios u2 on u2.cod_usuario = j2.cod_usuario and u2.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'
where p.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}';