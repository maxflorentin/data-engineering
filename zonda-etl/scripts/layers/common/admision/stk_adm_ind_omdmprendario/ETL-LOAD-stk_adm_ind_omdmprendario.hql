set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_ind_omdmprendario
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}')
select tipo_tramite as cod_adm_tipotramite,
cod_tramite as cod_adm_tramite,
substr(fec_proceso,1,19) as ts_adm_fecproceso,
cast(b.anioVehiculo as int) as int_adm_aniovehiculo,
cast(b.cantidadParticipantes as int) as int_adm_cantidadparticipantes,
cast(b.cuotaFinalTradicional as double) as fc_adm_cuotafinaltradicional,
cast(b.cuotaFinalUVA as double) as fc_adm_cuotafinaluva,
cast(b.exigiblePrestPrendAvanzado as double) as fc_adm_exigibleprestprendavanzado,
b.marcaVehiculo as cod_adm_marcavehiculo,
b.marCupon as flag_adm_marcupon,
b.marNuevo as ds_adm_marnuevo,
b.modeloVehiculo as ds_adm_modelovehiculo,
cast(b.montoFinalAlternativo as double) as fc_adm_montofinalalternativo,
cast(b.montoFinalAlternativoUVA as double) as fc_adm_montofinalalternativouva,
cast(b.montoMaximoCupon as double) as fc_adm_montomaximocupon,
cast(b.montoMaximoTradicional as double) as fc_adm_montomaximotradicional,
cast(b.montoMaximoUVA as double) as fc_adm_montomaximouva,
cast(b.montoMaximoVehiculo as double) as fc_adm_montomaximovehiculo,
cast(b.montoMaximoVehiculoUVA as double) as fc_adm_montomaximovehiculouva,
cast(b.montoMinimo as double) as fc_adm_montominimo,
cast(b.montoMinimoUVA as double) as fc_adm_montominimouva,
cast(b.pctFinanciacionTradicional as double) as fc_adm_pctfinanciaciontradicional,
cast(b.pctFinanciacionUVA as double) as fc_adm_pctfinanciacionuva,
cast(b.pctMaxFinanciacionCupon as double) as fc_adm_pctmaxfinanciacioncupon,
cast(b.plazoFinalTradicional as int) as int_adm_plazofinaltradicional,
cast(b.plazoFinalUVA as int) as int_adm_plazofinaluva,
cast(b.plazoMaximoCupon as int) as int_adm_plazomaximocupon,
cast(b.plazoMinimo as int) as int_adm_plazominimo,
cast(b.plazoMinimoCupon as int) as int_adm_plazominimocupon,
cast(b.plazoMinimoUVA as int) as int_adm_plazominimouva,
cast(b.tasa12MPrenTrad as double) as fc_adm_tasa12mprentrad,
cast(b.tasa12MPrenUVA as double) as fc_adm_tasa12mprenuva,
cast(b.tasa24MPrenTrad as double) as fc_adm_tasa24mprentrad,
cast(b.tasa24MPrenUVA as double) as fc_adm_tasa24mprenuva,
cast(b.tasa36MPrenTrad as double) as fc_adm_tasa36mprentrad,
cast(b.tasa36MPrenUVA as double) as fc_adm_tasa36mprenuva,
cast(b.tasa48MPrenTrad as double) as fc_adm_tasa48mprentrad,
cast(b.tasa48MPrenUVA as double) as fc_adm_tasa48mprenuva,
cast(b.tasa60MPrenTrad as double) as fc_adm_tasa60mprentrad,
cast(b.tasa60MPrenUVA as double) as fc_adm_tasa60mprenuva,
cast(b.tasaFinalTradicional as double) as fc_adm_tasafinaltradicional,
cast(b.tasaFinalUVA as double) as fc_adm_tasafinaluva,
cast(b.tipoVehiculo as int) as cod_adm_tipovehiculo,
cast(b.valorVehiculo as double) as fc_adm_valorvehiculo,
cast(b.valorVehiculoEstimado as double) as fc_adm_valorvehiculoestimado,
json as ds_adm_json
from bi_corp_staging.scoring_omdm_solicitudes
lateral view json_tuple(json,
'anioVehiculo',
'cantidadParticipantes',
'cuotaFinalTradicional',
'cuotaFinalUVA',
'exigiblePrestPrendAvanzado',
'marcaVehiculo',
'marCupon',
'marNuevo',
'modeloVehiculo',
'montoFinalAlternativo',
'montoFinalAlternativoUVA',
'montoMaximoCupon',
'montoMaximoTradicional',
'montoMaximoUVA',
'montoMaximoVehiculo',
'montoMaximoVehiculoUVA',
'montoMinimo',
'montoMinimoUVA',
'pctFinanciacionTradicional',
'pctFinanciacionUVA',
'pctMaxFinanciacionCupon',
'plazoFinalTradicional',
'plazoFinalUVA',
'plazoMaximoCupon',
'plazoMinimo',
'plazoMinimoCupon',
'plazoMinimoUVA',
'tasa12MPrenTrad',
'tasa12MPrenUVA',
'tasa24MPrenTrad',
'tasa24MPrenUVA',
'tasa36MPrenTrad',
'tasa36MPrenUVA',
'tasa48MPrenTrad',
'tasa48MPrenUVA',
'tasa60MPrenTrad',
'tasa60MPrenUVA',
'tasaFinalTradicional',
'tasaFinalUVA',
'tipoVehiculo',
'valorVehiculo',
'valorVehiculoEstimado'
) b as anioVehiculo,cantidadParticipantes,cuotaFinalTradicional,cuotaFinalUVA,exigiblePrestPrendAvanzado,marcaVehiculo,marCupon,marNuevo,modeloVehiculo,montoFinalAlternativo,montoFinalAlternativoUVA,montoMaximoCupon,montoMaximoTradicional,montoMaximoUVA,montoMaximoVehiculo,montoMaximoVehiculoUVA,montoMinimo,montoMinimoUVA,pctFinanciacionTradicional,pctFinanciacionUVA,pctMaxFinanciacionCupon,plazoFinalTradicional,plazoFinalUVA,plazoMaximoCupon,plazoMinimo,plazoMinimoCupon,plazoMinimoUVA,tasa12MPrenTrad,tasa12MPrenUVA,tasa24MPrenTrad,tasa24MPrenUVA,tasa36MPrenTrad,tasa36MPrenUVA,tasa48MPrenTrad,tasa48MPrenUVA,tasa60MPrenTrad,tasa60MPrenUVA,tasaFinalTradicional,tasaFinalUVA,tipoVehiculo,valorVehiculo,valorVehiculoEstimado
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}'
and label='Prendario';