CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_pre_bajas (
cod_pre_entidad string,
cod_suc_sucursal int,
cod_nro_cuenta bigint,
cod_per_nup int,
cod_prod_producto string,
cod_prod_subproducto string,
cod_pre_productocierre string,
cod_pre_subproductocierre string,
cod_div_divisa string,
cod_pre_entidadvinculada string,
cod_suc_sucursalvinculada int,
cod_pre_cuentavinculada bigint,
cod_pre_productovinculado string,
cod_pre_ugofipre string,
cod_pre_ofelev string,
cod_pre_agente string,
cod_pre_tipotasa string,
cod_pre_tipproducto string,
cod_pre_indclaseproducto string,
cod_pre_proceden string,
cod_pre_destino string,
dt_pre_fechasolicitud string,
cod_pre_situacionprestamo string,
dt_pre_fechasit string,
dt_pre_fechaaprobacion string,
dt_pre_fechaformalizacion string,
dt_pre_fechaabono string,
dt_pre_fechavencprogramado string,
dt_pre_fechapago string,
cod_pre_canalventa string,
dt_pre_fechaproximaliquidacion string,
dt_pre_fechaultimovencimiento string,
cod_pre_garantia string,
dt_pre_fechauliq string,
dt_pre_fechavencimiento string,
cod_pre_tipocondalte string,
cod_pre_condalte string,
cod_pre_procedencia string,
cod_pre_situaciondeudaobjetiva string,
cod_pre_modalidadprestamo string,
cod_pre_motivobajacuenta string,
dt_pre_fechaaltacuenta string,
dt_pre_fechabajacuenta string,
dt_pre_fechaultimopago string,
fc_pre_importeultimopago decimal(15,2),
cod_pre_tipoplazo string,
cod_pre_paquete string,
cod_pre_tipopaquete string,
dt_pre_fechaprimeracuota string,
fc_pre_importeprimeracuota decimal(15,2),
int_pre_plazoinicial int,
int_pre_plazoreal int,
cod_pre_plazo int,
int_pre_diapago int,
int_pre_diapagoactual int,
int_pre_numcuota int,
cod_pre_diasperiodo string,
ds_pre_prescriptor string,
flag_pre_reestructuracion int,
flag_pre_prestamoindexado int,
fc_pre_coeficienteindexactualizacion decimal(15,2),
cod_pre_numeropropuesta string,
cod_suc_sucursaloriginante int,
cod_pre_cuentaoriginante bigint,
cod_pre_nrocuotaimpagaprestamooriginante int,
flag_pre_marcacovid int,
cod_pre_tipoamortizacion string,
fc_pre_tasaanualequivalente decimal(15,6),
fc_pre_tasainteresinicial decimal(15,2),
fc_pre_tasainteresactual decimal(15,2),
fc_pre_cantidadcuotaspendientes int,
fc_pre_capitalsolicitado decimal(15,2),
fc_pre_lineacreditototal decimal(15,2),
fc_pre_capitalconcedido decimal(15,2),
fc_pre_saldocapitalpendfactura decimal(15,2),
fc_pre_importeinicialtomado decimal(15,2),
fc_pre_interescompensatorio decimal(15,2),
fc_pre_montocuota decimal(15,2),
fc_pre_saldoreal decimal(15,2),
fc_pre_monto_impago decimal(15,2),
fc_pre_saldocapitalvencidoimpago decimal(15,2),
fc_pre_saldointeres decimal(15,2),
fc_pre_lineadisponible decimal(15,2),
fc_pre_importedispuesto decimal(15,2),
fc_pre_numdividendo decimal(15,2),
dt_pre_fechaprimerpago string,
cod_pre_oftit int,
cod_pre_tiamo string,
cod_pre_plzcontractual string,
cod_pre_perint string,
cod_pre_percap string,
flag_pre_tasacomb int,
flag_pre_perfijotasacomb int,
flag_pre_tipoapli int,
fc_pre_tipoaplihoy decimal(15,2),
cod_pre_maesitdeuct string,
dt_pre_fechasitob string,
dt_pre_fechavencin string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/prestamos/bt_pre_bajas';