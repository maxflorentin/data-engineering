CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_omdm_cpp_agri (
    cod_adm_tipotramite string,
    cod_adm_tramite string,
    ts_adm_fecproceso string,
    cod_adm_tipoparticipante string,
    ds_adm_variable string,
    fc_adm_agrogastosestructura double,
    int_adm_superficiealquiladaagriculturamasfrutales int,
    int_adm_superficiealquiladacriainvernadatambo int,
    int_adm_superficieaparceriaagriculturamasfrutales int,
    int_adm_superficieaparceriacriainvernadatambo int,
    int_adm_superficiepropiaagriculturamasfrutales int,
    int_adm_superficiepropiacriainvernadatambo int,
    fc_adm_agricostocomerccultivobsr double,
    fc_adm_agricostodirectocultivo double,
    fc_adm_agricostodirectocultivobsr double,
    fc_adm_agrihectareascultivadas double,
    fc_adm_agrihectareascultivadasalquiladas double,
    fc_adm_agrihectareascultivadasaparceria double,
    fc_adm_agrihectareascultivadaspropias double,
    fc_adm_agripreciocultivotn double,
    fc_adm_agripreciocultivotnbsr double,
    fc_adm_agririndecultivo double,
    fc_adm_agririndecultivobsr double,
    fc_adm_agristockalinicio double,
    cod_adm_agritipocultivo int,
    ds_adm_cultivo string,
    cod_adm_idcultivo int,
    fc_adm_pctaparceria double,
    ds_adm_zonacultivo string,
    ds_adm_agrizona string,
    ds_adm_json string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_empresas_omdm_cpp_agri';