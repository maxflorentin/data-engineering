CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio4_clientes(
JERARQUIA                        STRING,
CODIGO                  		 STRING,
RAZONSOCIAL                      STRING,
NRCUENTA                         STRING,
FINGRESO                         STRING,
LIMITE                           STRING,
TASA1                            STRING,
TASA2                            STRING,
TASA3                            STRING,
TASA4                            STRING,
TASA5                            STRING,
OBSERVACIONES                    STRING,
RESPONSABLE                      STRING,
CARACTERISTICAS                  STRING,
PRODUCTORES                      STRING,
PERFILES                         STRING,
VALOR1                           STRING,
VALOR2                           STRING,
VALOR3                           STRING,
VALOR4                           STRING,
VALOR5                           STRING,
VALOR6                           STRING,
VALOR7                           STRING,
VALOR8                           STRING,
VALOR9                           STRING,
VALOR10                          STRING,
FECHA1                           STRING,
FECHA2                           STRING,
FECHA3                           STRING,
FECHA4                           STRING,
FECHA5                           STRING,
FECHA6                           STRING,
FECHA7                           STRING,
FECHA8                           STRING,
FECHA9                           STRING,
FECHA10                          STRING,
ALIAS1                           STRING,
ALIAS2                           STRING,
ALIAS3                           STRING,
ALIAS4                           STRING,
ALIAS5                           STRING,
ALIAS6                           STRING,
ALIAS7                           STRING,
ALIAS8                           STRING,
ALIAS9                           STRING,
ALIAS10                          STRING,
DIRECCION                        STRING,
LOCALIDAD                        STRING,
CODIGOPOSTAL                     STRING,
TELEFONO                         STRING,
CUIT                             STRING,
CONDICIONIVA                     STRING,
CONDICIONGAN                     STRING,
GRUPOECONOMICO                   STRING,
BACRR                            STRING,
GCI                              STRING,
CIS                              STRING,
CODIGOBCRA                       STRING,
PATRIMONIO                       STRING,
FECHABAL                         STRING,
SECTOR                           STRING,
LIQHABITUAL                      STRING,
CODCOMITENTE                     STRING,
NRAGENTEBOLSA                    STRING,
DEPOSDELCOMITENTE                STRING,
ESDEPOSITANTE                    STRING,
CATEGDEPOSITANTE                 STRING,
CODIGODEPOSITANTE                STRING,
STATUS                           STRING,
FECDESHABILITACION               STRING,
CODDEPCAJAVALORES                STRING,
NRINGBRUTOS                      STRING,
NRDNRP                           STRING,
IMPUESTOSINTERNOS                STRING,
FAX                              STRING,
PAIS                             STRING,
CODGRUPOECONOMICO                STRING,
RPC                              STRING,
TIPOSUCURSAL                     STRING,
SECTORBCRA                       STRING,
EXTRACTO                         STRING,
DIRECCIONEMAIL                   STRING,
PERSONA                          STRING,
NACIONALIDAD                     STRING,
TIPODOCUM                        STRING,
NRDOCUM                          STRING,
ESTADOCIVIL                      STRING,
PERIODICIDADBALANCE              STRING,
AFJP                             STRING,
PRESENTADOPOR                    STRING,
NRUNICO                          STRING,
FAXCONFIRMACION                  STRING,
TIPOARANCEL                      STRING,
SEXO                             STRING,
FECHANACIMIENTO                  STRING,
CANTHIJOS                        STRING,
CANTACARGO                       STRING,
ESTUDIOS                         STRING,
OCUPACION                        STRING,
CARGO                            STRING,
TIPOACTIVIDAD                    STRING,
INGRESOS                         STRING,
NETWORTH                         STRING,
EXPERIENCIAINV                   STRING,
NYACONYUGE                       STRING,
FNACCONYUGE                      STRING,
SEXOCONYUGE                      STRING,
OCUPCONYUGE                      STRING,
INGRCONYUGE                      STRING,
VIVIENDA                         STRING,
RES3125                          STRING,
RES3337                          STRING,
TIPOINSCRIPCION                  STRING,
HABDERIVADOS                     STRING,
HABFUTUROS                       STRING,
HABEXTLOCAL                      STRING,
HABEXTINTL                       STRING,
HABBURPISO                       STRING,
HABBURCONT                       STRING,
HABFINANCIERO                    STRING,
HABPRESTAMOS                     STRING,
HABFX                            STRING,
CATPRESTAMOSBCRA                 STRING,
CODAS400                         STRING,
ACTIVIDADBCRA                    STRING,
INSCRIPCION                      STRING,
NROINSC                          STRING,
LIBRO                            STRING,
FOLIO                            STRING,
TOMO                             STRING,
FECHAINSC                        STRING,
PAISINSC                         STRING,
ACTCOMPLEMENTARIAS               STRING,
MINARANCEL                       STRING,
PORARANCEL                       STRING,
FECHACIEBALANUAL                 STRING,
PROVINCIA                        STRING,
PAISDIR                          STRING,
FECHADIRECTORIO                  STRING,
DURACIONDIRECTORIO               STRING,
APORTEPREV                       STRING,
AJUSTEIN                         STRING,
FEXEGAN                          STRING,
TINGBRU                          STRING,
CODPUBLICO                       STRING,
VINCULADO                        STRING,
IMPUESTOEEMP                     STRING,
NUP                              STRING,
CONTRATO                         STRING,
CNO                              STRING,
DIRECCIONEMAIL_CC                STRING,
DIRECCIONEMAIL_BCC               STRING,
TIPO_DOC_CONTROL_LIMITE          STRING,
SUC_CUENTA_PLAZO                 STRING,
NRO_CUENTA_PLAZO                 STRING,
NRMAE2                           STRING,
NRMAE                            STRING,
ID_COMITENTE_ROFEX               STRING,
CIERRA_CAMBIOS                   STRING,
CORREDOR                         STRING,
CONTRATO_TESORERIA               STRING,
TIPO_CLIENTE                     STRING,
CLIENTE_BANCA_PRIVADA            STRING,
IMPACTA_MUREX                    STRING,
FECHA_ACTUALIZACION              STRING,
CIERRA_CAMBIOS_ESPECIAL          STRING,
PAIS_RESIDENCIA                  STRING,
CODIGO_MIGRADO                   STRING,
OPERA_FONDOS                     STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio4/dim/clientes';