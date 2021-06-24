set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 4000000;
set hive.merge.smallfiles.avgsize = 16000000;
set hive.exec.dynamic.partition.mode = nonstrict;
set mapred.job.queue.name = root.dataeng;

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.aux_garant_contratos_div (
    cod_entidad_bdr string,
    nup_cto_bdr string,
    id_cto_bdr string,
    cod_entidad string,
    num_persona string,
    id_cto_div string,
    pos_balance string,
    orden_x_producto int,
    Rating_Cliente decimal(19, 4),
    Rating_Pais_Cliente decimal(19, 4),
    segmento_cliente string,
    facturacion_total_cliente decimal(19, 4),
    deuda_total_cliente decimal(19, 4),
    RW_contrato decimal(19, 4),
    RW_cliente decimal(19, 4),
    RW_contraparte decimal(19, 4),
    factor_reductor decimal(19, 4),
    APR_SIN_MITIGAR decimal(19, 4),
    imp_deuda decimal(19, 4),
    orden_x_contrato int,
    orden_contrato int
) 
PARTITIONED BY (partition_date string) 
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/aux_garant_contratos_div';

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.aux_garant_calif_pais (
    pais string,
    codigo_iso_pais string,
    fitch_lt_f string,
    mdys_lt_fc string,
    sp_lt_fc string,
    cp_fitch string,
    cp_moodys string,
    cp_sp string,
    listado_ordenado array < string >,
    cal_unif_pais string
) 
PARTITIONED BY (partition_date string) 
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/aux_garant_calif_pais';

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.aux_garant_calif_empresa (
    alias_nup string,
    shortname string,
    FITCH_SU string,
    MOODYS_LT_RT string,
    SP_LT_FCUR_CREDIT_RT string,
    ce_fitch string,
    ce_moodys string,
    ce_sp string,
    listado_ordenado array < string >,
    cal_unif_empresa string
) 
PARTITIONED BY (partition_date string) 
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/aux_garant_calif_empresa';

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.aux_garant_garantias_especificas_div (
    cod_entidad string,
    id_cto_div string,
    num_garantia string,
    cod_estado string,
    fec_alta string,
    fec_vcto string,
    cod_garantia string,
    tip_cobertur string,
    num_bien int,
    cla_garantia string,
    tip_instrumentacion string,
    pignoracion string,
    tip_aval string,
    cod_divisa string,
    imp_limite decimal (19, 4),
    tip_valor string,
    imp_total decimal (19, 4),
    imp_total_garantia decimal (19, 4),
    imp_total_garantia_pesos decimal (19, 4),
    imp_cambio_fijo_vigente decimal(15, 9),
    fec_bajrelac string,
    ind_principa string,
    orden int,
    tip_gara_bdr string,
    cod_garantia_bdr string,
    num_persona_garante string,
    pais_iso_garante string,
    cod_bien string,
    cod_postal string
) 
PARTITIONED BY (partition_date string) 
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/aux_garant_garantias_especificas_div';

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.aux_garant_garantias_genericas(
    cod_entidad string,
    num_persona string,
    num_garantia string,
    cod_estado string,
    fec_alta string,
    fec_vcto string,
    fec_bajrelac string,
    cod_garantia string,
    tip_cobertur string,
    num_bien int,
    tip_instrumentacion string,
    pignoracion string,
    tip_aval string,
    cod_divisa string,
    imp_limite decimal(19, 4),
    imp_limite_pesos decimal(19, 4),
    imp_cambio_fijo_vigente decimal(15, 9),
    num_persona_garante string,
    cal_unif_pais string,
    rw_garante string,
    rw_garantia string,
    orden_cod_garantia int,
    orden int,
    tip_gara_bdr string,
    cod_garantia_bdr string,
    pais_iso_garante string,
    cod_bien string,
    cod_postal string
) 
PARTITIONED BY (partition_date string) 
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/aux_garant_garantias_genericas';

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.aux_garant_garantias_contratos_div(
    cod_entidad string,
    num_persona string,
    id_cto_div string,
    orden_contrato int,
    imp_deuda decimal(19, 4),
    apr_sin_mitigar decimal(19, 4),
    orden_garantia int,
    cla_garantia string,
    num_garantia string,
    tip_cobertur string,
    cod_garantia string,
    fec_alta string,
    fec_bajrelac string,
    fec_vcto string,
    imp_total_garantia decimal(19, 4),
    imp_total_garantia_pesos decimal(19, 4),
    orden_prelacion int,
    nominal_cobertura_actual decimal(19, 4),
    porc_cobertura_actual decimal(19, 4),
    porc_reparto decimal(19, 4),
    nominal_cobertura_inicial decimal(19, 4),
    porc_cobertura_inicial decimal(19, 4),
    imp_deuda_remanente decimal(19, 4),
    imp_total_garantia_pesos_remantente decimal(19, 4),
    stage int,
    tip_instrumentacion string,
    pignoracion string,
    gar_cod_estado string,
    gar_cod_divisa string,
    tip_aval string,
    tip_gara_bdr string,
    cod_garantia_bdr string,
    id_cto_bdr string,
    cod_entidad_bdr string
) 
PARTITIONED BY (partition_date string) 
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/aux_garant_garantias_contratos_div';

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.aux_garant_garantias_contratos_inicial_div(
    cod_entidad string,
    num_persona string,
    id_cto_div string,
    orden_contrato int,
    imp_deuda decimal(19, 4),
    apr_sin_mitigar decimal(19, 4),
    orden_garantia int,
    cla_garantia string,
    num_garantia string,
    tip_cobertur string,
    cod_garantia string,
    fec_alta string,
    fec_bajrelac string,
    fec_vcto string,
    imp_total_garantia decimal(19, 4),
    imp_total_garantia_pesos decimal(19, 4),
    orden_prelacion int,
    nominal_cobertura_actual decimal(19, 4),
    porc_cobertura_actual decimal(19, 4),
    porc_reparto decimal(19, 4),
    nominal_cobertura_inicial decimal(19, 4),
    porc_cobertura_inicial decimal(19, 4),
    imp_deuda_remanente decimal(19, 4),
    imp_total_garantia_pesos_remantente decimal(19, 4),
    stage int,
    tip_instrumentacion string,
    pignoracion string,
    gar_cod_estado string,
    gar_cod_divisa string,
    tip_aval string
) 
PARTITIONED BY (partition_date string) 
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/aux_garant_garantias_contratos_inicial_div';
