SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_revision_rating_ficha
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
select
    cast(r.idficharevision as int) as id_adm_ficharevision,
    cast(r.idrevisionestado as int) as id_adm_revisionestado,
    dre.descripcion as ds_adm_revisionestado,

    cast(r.idcuestionarioree1 as int) as id_adm_cuestionarioree1,
    CASE
    WHEN r.idcuestionarioree1 = "1" THEN "Vacio"
    WHEN r.idcuestionarioree1 = "2" THEN "Deficiente"
    WHEN r.idcuestionarioree1 = "3" THEN "Conforme"
    WHEN r.idcuestionarioree1 = "4" THEN "Mejorar" END AS ds_adm_cuestionarioee1_productodemandamercado,

    cast(r.idtipoproducto as int) as id_adm_tipoproducto,
    CASE
    WHEN r.idtipoproducto = "1" THEN "Deficiente"
    WHEN r.idtipoproducto = "2" THEN "Conforme"
    WHEN r.idtipoproducto = "3" THEN "Mejorar" END AS ds_adm_tipoproducto,

    cast(r.idtipodemanda as int) as id_adm_tipodemanda,
    CASE
    WHEN r.idtipodemanda = "1" THEN "Deficiente"
    WHEN r.idtipodemanda = "2" THEN "Conforme"
    WHEN r.idtipodemanda = "3" THEN "Mejorar" END AS ds_adm_tipodemanda,

    cast(r.idtipomercado as int) as id_adm_tipomercado,
    CASE
    WHEN r.idtipomercado = "1" THEN "Deficiente"
    WHEN r.idtipomercado = "2" THEN "Conforme"
    WHEN r.idtipomercado = "3" THEN "Mejorar" END AS ds_adm_tipomercado,

    cast(r.idcuestionarioree2 as int) as id_adm_cuestionarioree2,
    CASE
    WHEN r.idcuestionarioree2 = "1" THEN "Vacio"
    WHEN r.idcuestionarioree2 = "2" THEN "Deficiente"
    WHEN r.idcuestionarioree2 = "3" THEN "Conforme"
    WHEN r.idcuestionarioree2 = "4" THEN "Mejorar" END AS ds_adm_cuestionarioree2_accionistasgerencia_completo,

    cast(r.idtipoaccionistagerencia1 as int) as id_adm_tipoaccionistagerencia1,
    CASE
    WHEN r.idtipoaccionistagerencia1 = "1" THEN "Deficiente"
    WHEN r.idtipoaccionistagerencia1 = "2" THEN "Conforme"
    WHEN r.idtipoaccionistagerencia1 = "3" THEN "Mejorar" END AS ds_adm_identificacionprecisaaccionariado,

    cast(r.idtipoaccionistagerencia2 as int) as id_adm_tipoaccionistagerencia2,
    CASE
    WHEN r.idtipoaccionistagerencia2 = "1" THEN "Deficiente"
    WHEN r.idtipoaccionistagerencia2 = "2" THEN "Conforme"
    WHEN r.idtipoaccionistagerencia2 = "3" THEN "Mejorar" END AS ds_adm_valoracionapoyoaccionistas,

    cast(r.idtipoaccionistagerencia3 as int) as id_adm_tipoaccionistagerencia3,
    CASE
    WHEN r.idtipoaccionistagerencia3 = "1" THEN "Deficiente"
    WHEN r.idtipoaccionistagerencia3 = "2" THEN "Conforme"
    WHEN r.idtipoaccionistagerencia3 = "3" THEN "Mejorar" END AS ds_adm_valoraciongerencia,

    cast(r.idtipoaccionistagerencia4 as int) as id_adm_tipoaccionistagerencia4,
    CASE
    WHEN r.idtipoaccionistagerencia4 = "1" THEN "Deficiente"
    WHEN r.idtipoaccionistagerencia4 = "2" THEN "Conforme"
    WHEN r.idtipoaccionistagerencia4 = "3" THEN "Mejorar" END AS ds_adm_conclusionaccionistagerencia,

    cast(r.idcuestionarioree3 as int) as id_adm_cuestionarioree3,
    CASE
    WHEN r.idcuestionarioree3 = "1" THEN "Vacio"
    WHEN r.idcuestionarioree3 = "2" THEN "Deficiente"
    WHEN r.idcuestionarioree3 = "3" THEN "Conforme"
    WHEN r.idcuestionarioree3 = "4" THEN "Mejorar" END AS ds_adm_cuestionarioree3_accesoalcredito_completo,

    cast(r.idtipoaccesoalcredito1 as int) as id_adm_tipoaccesoalcredito1,
    CASE
    WHEN r.idtipoaccesoalcredito1 = "1" THEN "Deficiente"
    WHEN r.idtipoaccesoalcredito1 = "2" THEN "Conforme"
    WHEN r.idtipoaccesoalcredito1 = "3" THEN "Mejorar" END AS ds_adm_detalledeudabancaria,

    cast(r.idtipoaccesoalcredito2 as int) as id_adm_tipoaccesoalcredito2,
    CASE
    WHEN r.idtipoaccesoalcredito2 = "1" THEN "Deficiente"
    WHEN r.idtipoaccesoalcredito2 = "2" THEN "Conforme"
    WHEN r.idtipoaccesoalcredito2 = "3" THEN "Mejorar" END AS ds_adm_detallerestodeuda,

    cast(r.idtipoaccesoalcredito3 as int) as id_adm_tipoaccesoalcredito3,
    CASE
    WHEN r.idtipoaccesoalcredito3 = "1" THEN "Deficiente"
    WHEN r.idtipoaccesoalcredito3 = "2" THEN "Conforme"
    WHEN r.idtipoaccesoalcredito3 = "3" THEN "Mejorar" END AS ds_adm_abanicobancario,

    cast(r.idtipoaccesoalcredito4 as int) as id_adm_tipoaccesoalcredito4,
    CASE
    WHEN r.idtipoaccesoalcredito4 = "1" THEN "Deficiente"
    WHEN r.idtipoaccesoalcredito4 = "2" THEN "Conforme"
    WHEN r.idtipoaccesoalcredito4 = "3" THEN "Mejorar" END AS ds_adm_garantias,

    cast(r.idtipoaccesoalcredito5 as int) as id_adm_tipoaccesoalcredito5,
    CASE
    WHEN r.idtipoaccesoalcredito5 = "1" THEN "Deficiente"
    WHEN r.idtipoaccesoalcredito5 = "2" THEN "Conforme"
    WHEN r.idtipoaccesoalcredito5 = "3" THEN "Mejorar" END AS ds_adm_conclusionaccesoalcredito,

    cast(r.idcuestionarioree4 as int) as id_adm_cuestionarioree4,
    CASE
    WHEN r.idcuestionarioree4 = "1" THEN "Vacio"
    WHEN r.idcuestionarioree4 = "2" THEN "Deficiente"
    WHEN r.idcuestionarioree4 = "3" THEN "Conforme"
    WHEN r.idcuestionarioree4 = "4" THEN "Mejorar" END AS ds_adm_cuestionarioree4_rentabilidadbeneficio_completo,

    cast(r.idtiporentabilidadbeneficio1 as int) as id_adm_tiporentabilidadbeneficio1,
    CASE
    WHEN r.idtiporentabilidadbeneficio1 = "1" THEN "Deficiente"
    WHEN r.idtiporentabilidadbeneficio1 = "2" THEN "Conforme"
    WHEN r.idtiporentabilidadbeneficio1 = "3" THEN "Mejorar" END AS ds_adm_explicacionevolucionventas,

    cast(r.idtiporentabilidadbeneficio2 as int) as id_adm_tiporentabilidadbeneficio2,
    CASE
    WHEN r.idtiporentabilidadbeneficio2 = "1" THEN "Deficiente"
    WHEN r.idtiporentabilidadbeneficio2 = "2" THEN "Conforme"
    WHEN r.idtiporentabilidadbeneficio2 = "3" THEN "Mejorar" END AS ds_adm_recurrenciabeneficio,

    cast(r.idtiporentabilidadbeneficio3 as int) as id_adm_tiporentabilidadbeneficio3,
    CASE
    WHEN r.idtiporentabilidadbeneficio3 = "1" THEN "Deficiente"
    WHEN r.idtiporentabilidadbeneficio3 = "2" THEN "Conforme"
    WHEN r.idtiporentabilidadbeneficio3 = "3" THEN "Mejorar" END AS ds_adm_previsiones,

    cast(r.idtiporentabilidadbeneficio4 as int) as id_adm_tiporentabilidadbeneficio4,
    CASE
    WHEN r.idtiporentabilidadbeneficio4 = "1" THEN "Deficiente"
    WHEN r.idtiporentabilidadbeneficio4 = "2" THEN "Conforme"
    WHEN r.idtiporentabilidadbeneficio4 = "3" THEN "Mejorar" END AS ds_adm_conclusionrentabilidadbeneficios,

    cast(r.idcuestionarioree5 as int) as id_adm_cuestionarioree5,
    CASE
    WHEN r.idcuestionarioree5 = "1" THEN "Vacio"
    WHEN r.idcuestionarioree5 = "2" THEN "Deficiente"
    WHEN r.idcuestionarioree5 = "3" THEN "Conforme"
    WHEN r.idcuestionarioree5 = "4" THEN "Mejorar" END AS ds_adm_cuestionarioree5_generacionrecursos_completo,

    cast(r.idtipogeneracionderecursos1 as int) as id_adm_tipogeneracionderecursos1,
    CASE
    WHEN r.idtipogeneracionderecursos1 = "1" THEN "Deficiente"
    WHEN r.idtipogeneracionderecursos1 = "2" THEN "Conforme"
    WHEN r.idtipogeneracionderecursos1 = "3" THEN "Mejorar" END AS ds_adm_serviciodeuda,

    cast(r.idtipogeneracionderecursos2 as int) as id_adm_tipogeneracionderecursos2,
    CASE
    WHEN r.idtipogeneracionderecursos2 = "1" THEN "Deficiente"
    WHEN r.idtipogeneracionderecursos2 = "2" THEN "Conforme"
    WHEN r.idtipogeneracionderecursos2 = "3" THEN "Mejorar" END AS ds_adm_politicadividendos,

    cast(r.idtipogeneracionderecursos3 as int) as id_adm_tipogeneracionderecursos3,
    CASE
    WHEN r.idtipogeneracionderecursos3 = "1" THEN "Deficiente"
    WHEN r.idtipogeneracionderecursos3 = "2" THEN "Conforme"
    WHEN r.idtipogeneracionderecursos3 = "3" THEN "Mejorar" END AS ds_adm_inversionactivosfijos,

    cast(r.idtipogeneracionderecursos4 as int) as id_adm_tipogeneracionderecursos4,
    CASE
    WHEN r.idtipogeneracionderecursos4 = "1" THEN "Deficiente"
    WHEN r.idtipogeneracionderecursos4 = "2" THEN "Conforme"
    WHEN r.idtipogeneracionderecursos4 = "3" THEN "Mejorar" END AS ds_adm_conclusiongeneracionfondos,

    cast(r.idcuestionarioree6 as int) as id_adm_cuestionarioree6,
    CASE
    WHEN r.idcuestionarioree6 = "1" THEN "Vacio"
    WHEN r.idcuestionarioree6 = "2" THEN "Deficiente"
    WHEN r.idcuestionarioree6 = "3" THEN "Conforme"
    WHEN r.idcuestionarioree6 = "4" THEN "Mejorar" END AS ds_adm_cuestionarioree6_solvencia_completo,

    cast(r.idtiposolvencia1 as int) as id_adm_tiposolvencia1,
    CASE
    WHEN r.idtiposolvencia1 = "1" THEN "Deficiente"
    WHEN r.idtiposolvencia1 = "2" THEN "Conforme"
    WHEN r.idtiposolvencia1 = "3" THEN "Mejorar" END AS ds_adm_adecuacionestructuracapital,

    cast(r.idtiposolvencia2 as int) as id_adm_tiposolvencia2,
    CASE
    WHEN r.idtiposolvencia2 = "1" THEN "Deficiente"
    WHEN r.idtiposolvencia2 = "2" THEN "Conforme"
    WHEN r.idtiposolvencia2 = "3" THEN "Mejorar" END AS ds_adm_adecuacionestructuracirculante,

    cast(r.idtiposolvencia3 as int) as id_adm_tiposolvencia3,
    CASE
    WHEN r.idtiposolvencia3 = "1" THEN "Deficiente"
    WHEN r.idtiposolvencia3 = "2" THEN "Conforme"
    WHEN r.idtiposolvencia3 = "3" THEN "Mejorar" END AS ds_adm_detalleinmueble,

    cast(r.idtiposolvencia4 as int) as id_adm_tiposolvencia4,
    CASE
    WHEN r.idtiposolvencia4 = "1" THEN "Deficiente"
    WHEN r.idtiposolvencia4 = "2" THEN "Conforme"
    WHEN r.idtiposolvencia4 = "3" THEN "Mejorar" END AS ds_adm_verificacionregistrables,

    cast(r.idtiposolvencia5 as int) as id_adm_tiposolvencia5,
    CASE
    WHEN r.idtiposolvencia5 = "1" THEN "Deficiente"
    WHEN r.idtiposolvencia5 = "2" THEN "Conforme"
    WHEN r.idtiposolvencia5 = "3" THEN "Mejorar" END AS ds_adm_conclusionsolvencia,

    cast(r.idtipoproyeccion as int) as id_adm_tipoproyeccion,
    CASE
    WHEN r.idtipoproyeccion = "1" THEN "Deficiente"
    WHEN r.idtipoproyeccion = "2" THEN "Conforme"
    WHEN r.idtipoproyeccion = "3" THEN "Mejorar" END AS ds_adm_tipoproyeccion,

    cast(r.idestadocomentarios as int) as id_adm_estadocomentarios,
    cast(r.idseguimiento as int) as id_adm_seguimiento,
    rrs.descripcion as ds_adm_seguimiento,
    cast(r.idestadosfinancieros as int) as id_adm_estadosfinancieros,
    ref.descripcion as ds_adm_estadosfinancieros,
    cast(r.idinformesectorial as int) as id_adm_informesectorial,
    cast(ris.id_rev_informesectorial as int) as int_adm_informesectorial,
    cast(r.idsistemaopcional as int) as id_adm_sistemaopcional,
    rso.descripcion as ds_adm_sistemaopcional,
    cast(r.idsistemaopcionalfeve as int) as id_adm_sistemaopcionalfeve,
    sof.descripcion as ds_adm_sistemaopcionalfeve,
    cast(r.idconclusion as int) as id_adm_conclusion,
    rrco.descripcion as ds_adm_conclusion,
    cast(r.ratingcomentadorta as int) as int_adm_ratingcomentadorta,
    cast(r.ratingrta as int) as int_adm_ratingrta,
    cast(r.calificacioninternarta as int) as int_adm_calificacioninternarta,
    cast(r.cuit as bigint) as int_adm_cuit,
    r.razonsocial as ds_adm_razonsocial,
    r.idzona as id_adm_zona,
    cast(r.ultimapropuesta as bigint) as cod_adm_ultimapropuesta,
    cast(r.idsucursal as int) as cod_adm_sucursal,
    cast(r.rating as decimal(16,2)) as dec_adm_rating,
    r.analista as ds_adm_analista,
    cast(r.calificacion as decimal(16,2)) as dec_adm_calificacion,
    r.ratingvigente as ds_adm_ratingvigente,
    cast(r.ratingcomentado as decimal(16,2)) as dec_adm_ratingcomentado,
    cast(r.calificacioninterna as decimal(16,2)) as dec_adm_calificacioninterna,
    r.regional as ds_adm_regional,
    r.cliente as ds_adm_cliente,
    r.revisorgys as ds_adm_revisorgys,
    r.analistarevisor as ds_adm_analistarevisor,
    r.comentario as ds_adm_comentario,
    r.comentariocorreccion as ds_adm_comentariocorreccion,
    cast(r.nup as int) as cod_per_nup,
    r.fechainicio as ts_adm_fechainicio,
    r.fechafinalizacion as ts_adm_fechafinalizacion,
    cast(r.cuestionarioreerta as int) as int_adm_cuestionarioreerta,
    r.valoracion as ds_adm_valoracion,
    cast(r.ratingresumido as int) as int_adm_ratingresumido,
    r.idzonarating as id_adm_zonarating,
    r.descripcionzonarating as ds_adm_descripcionzonarating
from bi_corp_staging.sge_revision_rating_ficha r

left join bi_corp_staging.sge_REV_RAT_ESTADO dre
on r.idrevisionestado = dre.id_rev_estado
and dre.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'

left join bi_corp_staging.sge_REV_RAT_SEGUIMIENTO rrs
on r.idseguimiento = rrs.id_rev_seguimiento
and rrs.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'

left join bi_corp_staging.sge_REV_RAT_ESTADO_FINANCIERO ref
on r.idestadosfinancieros = ref.id_rev_estadofinanciero
and ref.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'

left join bi_corp_staging.sge_REV_RAT_INFORME_SECTORIAL ris
on r.idinformesectorial = ris.id_rev_informesectorial
and ris.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'

left join bi_corp_staging.sge_REV_RAT_SISTEMA_OPCIONAL rso
on r.idsistemaopcional = rso.id_rev_sistemaopcional
and rso.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'

left join bi_corp_staging.sge_REV_RAT_SIST_OPCIONAL_FEVE sof
on r.idsistemaopcionalfeve = sof.id_rev_sistemaopcionalfeve
and sof.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'

left join bi_corp_staging.sge_REV_RAT_CONCLUSION rrco
on r.idconclusion = rrco.id_rev_conclusion
and rrco.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'

where r.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';

