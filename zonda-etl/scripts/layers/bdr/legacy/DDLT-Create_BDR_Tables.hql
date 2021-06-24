
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_contr_otros(
 E0623_FEOPERAC string,
 E0623_S1EMP  string,
 E0623_CONTRA1 string,
 E0623_FEC_MES string,
 E0623_VCTO_RES string,
 E0623_VCTO_PON string,
 E0623_IDSUBPRD string,
 E0623_TIP_LIQU string,
 E0623_LIQ_PZO string,
 E0623_TIP_AMOR string,
 E0623_AMOR_PZO string,
 E0623_AMOR_SIS string,
 E0623_CTB_SITU string,
 E0623_GEST_SIT string,
 E0623_GES2_SIT string,
 E0623_ATAEMAX string,
 E0623_TIP_INT string,
 E0623_IMP1LIMI string,
 E0623_ALIMACT string,
 E0623_IMPORTH string,
 E0623_INV_NEGO string,
 E0623_IPROVISI string,
 E0623_IPROVIS1 string,
 E0623_FECULTMO string,
 E0623_ESTADTRJ string,
 E0623_INACTRJ string,
 E0623_UNNT  string,
 E0623_UNNTS  string,
 E0623_UNNV  string,
 E0623_UNNVS  string,
 E0623_RGOSUB string,
 E0623_FECINCAR string,
 E0623_FECFICAR string,
 E0623_INTNEG string,
 E0623_MTVALTA string,
 E0623_INDSUBRO string,
 E0623_TIP_INTE string,
 E0623_DIFERNCI string,
 E0623_IMPRTCUO string,
 E0623_INDSEGUR string,
 E0623_AMORTPAR string,
 E0623_FECIMPAG string,
 E0623_IMPPRIMP string,
 E0623_FHPRIMPG string,
 E0623_IMPIMPNR string,
 E0623_ESTPRINM string,
 E0623_EXCLCTO string,
 E0623_CUOTIMPA string,
 E0623_LIMOCULT string,
 E0623_CODIMPHI string,
 E0623_INDEUCON string,
 E0623_TIPCAREN string,
 E0623_CUOTPRES string,
 E0623_IBUYSELL string,
 E0623_SUTIPINT string,
 E0623_TETIPINT string,
 E0623_TIPSUELO string,
 E0623_TIPTECHO string,
 E0623_PLREVTIN string,
 E0623_FECCUOTA string,
 E0623_NUDIAATR string,
 E0623_SEGPLLIM string,
 E0623_VOLTRANS string,
 E0623_MARCAUTI string,
 E0623_TOPDEALE string,
 E0623_FECREPRE string,
 E0623_EMPREPOR string,
 E0623_IMPCUIMP string,
 E0623_TIPINEFE string,
 E0623_FORPGACT string,
 E0623_FINIUTCT string,
 E0623_FFINUTCT string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_contr_otros';


CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_contr_bis(
G4095_FEOPERAC string,
G4095_S1EMP  string,
G4095_CONTRA1 string,
G4095_IDSUCUR string,
G4095_ID_PAIS string,
G4095_ID_PROD string,
G4095_IDPRO_LC string,
G4095_FECVENTO string,
G4095_FECVE2 string,
G4095_FECHAPER string,
G4095_FECCANCE string,
G4095_FECREES string,
G4095_FECREFI string,
G4095_FECNOVAC string,
G4095_IDFINALI string,
G4095_IDFINALD string,
G4095_CONTRA2 string,
G4095_CODDIV string,
G4095_ID_CANAL string,
G4095_ID_CANA2 string,
G4095_ID_NATUR string,
G4095_ID_NSUBY string,
G4095_INDLIM string,
G4095_INDAVA string,
G4095_IND_TITU string,
G4095_INDDEUD string,
G4095_INDDEUD2 string,
G4095_TIP_INTE string,
G4095_IDEMIS string,
G4095_IDEMISI string,
G4095_IDNETING string,
G4095_IDCOLATE string,
G4095_FECULTMO string,
G4095_VENCORIG string,
G4095_DEUDPREL string,
G4095_FECENTVI string,
G4095_IDPROLC2 string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_contr_bis';


CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_client_bii(
G4093_FEOPERAC string,
G4093_S1EMP  string,
G4093_IDNUMCLI string,
G4093_TIP_PERS string,
G4093_APNOMPER string,
G4093_DATEXTO1 string,
G4093_DATEXTO2 string,
G4093_IDENTPER string,
G4093_CODIDPER string,
G4093_CLIE_GLO string,
G4093_IDSUCUR string,
G4093_CARTER string,
G4093_ID_PAIS string,
G4093_COD_SECT string,
G4093_COD_SEC2 string,
G4093_COD_SEC3 string,
G4093_CLISEGM string,
G4093_CLISEGL1 string,
G4093_TIPSEGL1 string,
G4093_CLISEGL2 string,
G4093_TIPSEGL2 string,
G4093_FCHINI string,
G4093_FCHFIN string,
G4093_FECULTMO string,
G4093_INDUSTRY string,
G4093_EXCLCLI string,
G4093_INDBCART string,
G4093_FECNACIM string,
G4093_PAISNEG string,
G4093_CDPOSTAL string,
G4093_TTO_ESPE string,
G4093_GRA_VINC string,
G4093_UTP_CLI string,
G4093_FINIUTCL string,
G4093_FFINUTCL string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_client_bii';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_flujos(
R9746_FEOPERAC string,
R9746_S1EMP  string,
R9746_CONTRA1 string,
R9746_FECMVTO string,
R9746_CLASMVTO string,
R9746_IMPORTE string,
R9746_SALONBAL string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_flujos';


CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_posic_contr(
E0621_FEOPERAC string,
E0621_S1EMP  string,
E0621_CONTRA1 string,
E0621_CTA_CONT string,
E0621_TIP_IMPT string,
E0621_FEC_MES string,
E0621_AGRCTACB string,
E0621_IMPORTH string,
E0621_CODDIV string,
E0621_FECULTMO string,
E0621_CENTCTBL string,
E0621_CTACGBAL string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_posic_contr';


CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_interv_cto(
G4128_FEOPERAC string,
G4128_S1EMP  string,
G4128_CONTRA1 string,
G4128_TIPINTEV string,
G4128_TIPINTV2 string,
G4128_NUMORDIN string,
G4128_IDNUMCLI string,
G4128_FORMINTV string,
G4128_PORPARTN string,
G4128_FECULTMO string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_interv_cto';


CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_prov_esotr(
N0625_FEOPERAC string,
N0625_S1EMP  string,
N0625_CONTRA1 string,
N0625_TIP_IMPT string,
N0625_IMPORTH string,
N0625_CODDIV string,
N0625_CTA_CONT string,
N0625_AGRCTACB string,
N0625_CENTCTBL string,
N0625_CTACGBAL string,
N0625_STAGE  string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_prov_esotr';


CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_trz_cliente(
G7015_S1EMP  string,
G7015_IDNUMCLI string,
G7015_FECDESDE string,
G7015_IDSISTE string,
G7015_TIP_PER string,
G7015_CDG_PERS string,
G7015_CODSISTE string,
G7015_FEC_HAS string,
G7015_FEC_MOD string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_trz_cliente';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_acu_mto_nd(
G7015_S1EMP string,
G7015_IDNUMCLI string,
G7015_FECDESDE string,
G7015_IDSISTE string,
G7015_TIP_PER string,
G7015_CDG_PERS string,
G7015_CODSISTE string,
G7015_FEC_HAS string,
G7015_FEC_MOD string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_acu_mto_nd';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_adic_calco(
N0632_FEOPERAC string,
N0632_S1EMP  string,
N0632_CONTRA1 string,
N0632_FECFORZ string,
N0632_MOTIVFOR string,
N0632_FECAPROB string,
N0632_ORGAPROB string,
N0632_FECULTMO string,
N0632_VINCSCO string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_adic_calco';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_inf_ad_cli(
H0780_FEOPERAC string,
H0780_S1EMP  string,
H0780_IDNUMCLI string,
H0780_TIPINFRL string,
H0780_TIPINFRG string,
H0780_IMPORTH string,
H0780_CODDIV string,
H0780_FECHAACT string,
H0780_FECULTMO string,
H0780_CUOTPRES string,
H0780_INGCLIEN string,
H0780_NUM_MTOS string,
H0780_FECINGRE string,
H0780_RDEUDING string,
H0780_TIP_EMPR string,
H0780_TOT_INGR string,
H0780_TOT_DEUF string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_inf_ad_cli';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_clien_juri(
G5508_FEOPERAC string,
G5508_S1EMP  string,
G5508_IDNUMCLI string,
G5508_INFFECHA string,
G5508_IMPFACTM string,
G5508_TOT_ACTI string,
G5508_NUM_EMPL string,
G5508_ORIG_FAC string,
G5508_ORIG_ACT string,
G5508_ORIG_EMP string,
G5508_FECULTMO string,
G5508_TDEUDACL string,
G5508_RAT_CET1 string,
G5508_TASAMORA string,
G5508_TOT_EQTY string,
G5508_ORGDEPEN string,
G5508_FLGEMPNO string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_clien_juri';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_aval_ejec (
G7232_S1EMP  string,
G7232_CONTRA1 string,
G7232_FECEJEC string,
G7232_IMPEJECU string,
G7232_CODDIV  string,
G7232_IMPRGVIV string,
G7232_FECULTMO string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_aval_ejec';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_baremos_loc (
R8180_S1EMP  string,
R8180_CODNEGOL string,
R8180_CODBARLE string,
R8180_FECDESDE string,
R8180_FECHASTA string,
R8180_DESCBARL string,
R8180_FECULTMO string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_baremos_loc';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_cal_in_cl (
E9954_FEOPERAC string,
E9954_S1EMP  string,
E9954_IDNUMCLI string,
E9954_FECCALI string,
E9954_TIPMODEL string,
E9954_TIPMODE2 string,
E9954_IDMODEL string,
E9954_TIPO  string,
E9954_IDPUNSCO string,
E9954_FECCADUC string,
E9954_C1TARPUN string,
E9954_C1SPID string,
E9954_C1DIGCON string,
E0621_FECULTMO string,
E9954_MOTIVFOR string,
E9954_IDPUNSC2 string,
E9954_FECREPFI string,
E9954_FECINOFC string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_cal_in_cl';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_cal_in_co (
E9952_FEOPERAC string,
E9952_S1EMP  string,
E9952_CONTRA1 string,
E9952_FECCALI string,
E9952_TIPMODEL string,
E9952_TIPMODE2 string,
E9952_IDMODEL string,
E9952_TIPO  string,
E9952_IDPUNSCO string,
E9952_FECCADUC string,
E9952_C1SPID string,
E9952_C1DIGCON string,
E9952_C1RESP string,
E9952_C1CIRC string,
E9952_FECULTMO string,
E9952_C1MOTEXC string,
E9952_TIPMODE3 string,
E9952_SCOVINPR string,
E9952_FECINOFC string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_cal_in_co';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_cal_emision (
E0665_S1EMP  string,
E0665_ID_EMISI string,
E0665_COD_EMIS string,
E0665_COD_AGEN string,
E0665_CCODPLZ string,
E0665_FECCALI string,
E0665_CODMERCD string,
E0665_FECHASTA string,
E0665_CALIFMAE string,
E0665_FECULTMO string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_cal_emision';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_calif_emisor (
E0664_S1EMP  string,
E0664_ID_PAIS string,
E0664_IDNUMCLI string,
E0664_TIPMODEL string,
E0664_FECINI string,
E0664_COD_AGEN string,
E0664_FECHASTA string,
E0664_CALIFMAE string,
E0664_INDICADR string,
E0664_FECULTMO string,
E0664_FECCALI string,
E0664_OUTLOOK string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_calif_emisor';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_cal_ext_cl (
R9415_FEOPERAC string,
R9415_S1EMP  string,
R9415_IDNUMCLI string,
R9415_COD_AGEN string,
R9415_CCODPLZ string,
R9415_TIPMONED string,
R9415_FECCALIF string,
R9415_CALIFMAE string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_cal_ext_cl';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_camb_sit_bdi(
E0688_S1EMP  string,
E0688_CONTRA1 string,
E0688_FECINI string,
E0688_FECSIT string,
E0688_FSALIDA string,
E0688_FECMARC string,
E0688_IDNUMCLI string,
E0688_BDSITU string,
E0688_CTB_SITU string,
E0688_GEST_SIT string,
E0688_GES2_SIT string,
E0688_RESP_SIT string,
E0688_IMPLICON string,
E0688_IMPDIPTO string,
E0688_IMPVDO string,
E0688_IMPNOVDO string,
E0688_INTEDEV string,
E0688_INTEMOR string,
E0688_COMFINA string,
E0688_COMFINB string,
E0688_GTOSDIRE string,
E0688_GTOSINDI string,
E0688_INDARRAS string,
E0688_FECULTMO string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_camb_sit_bdi';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_ctos_cance(
H0711_FEOPERAC string,
H0711_S1EMP string,
H0711_CONTRA1 string,
H0711_MOTVCANC string,
H0711_FECULTMO string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_ctos_cance';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_deud_ini_bdi(
E0687_S1EMP  string,
E0687_CONTRA1 string,
E0687_FECINI string,
E0687_BDSITU string,
E0687_FECSITU string,
E0687_IMPVALOR string,
E0687_FECULTMO string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_deud_ini_bdi';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_estado_vig(
R8182_FEOPERAC string,
R8182_S1EMP  string,
R8182_IDNUMCLI string,
R8182_CODORVIS string,
R8182_FECESTAD string,
R8182_FECGRADO string,
R8182_EST_FEVE string,
R8182_GRA_FEVE string,
R8182_FEVE_POL string,
R8182_ALERTSAR string,
R8182_FECULTMO string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_estado_vig';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_expedien_bdi(
E0685_S1EMP string,
E0685_CONTRA1 string,
E0685_FECINI string,
E0685_FECFIN string,
E0685_IDSUCUR string,
E0685_ID_PROD string,
E0685_IDPRO_LC string,
E0685_BDSITU string,
E0685_BDSITUSA string,
E0685_CAUS_CIE string,
E0685_FECULTMO string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_expedien_bdi';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_expos_no_con(
E0627_FEOPERAC string,
E0627_S1EMP  string,
E0627_CONTRA1 string,
E0627_FEC_MES string,
E0627_CTA_CONT string,
E0627_AGRCTACB string,
E0627_IMPORTH string,
E0627_IDCTACEN string,
E0621_FECULTMO string,
E0627_CENTCTBL string,
E0627_ENTCGBAL string,
E0627_CTACGBAL string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_expos_no_con';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_gara_real(
G4091_FEOPERAC string,
G4091_S1EMP  string,
G4091_BIENGAR1 string,
G4091_CONTRA1 string,
G4091_ID_PROD string,
G4091_COD_BIEN string,
G4091_BCO_CUST string,
G4091_BSINDI1 string,
G4091_INDCOTIZ string,
G4091_DEUDAPUB string,
G4091_INDDEUDA string,
G4091_TIPFRECU string,
G4091_FRECPLAZ string,
G4091_DKAFECTO string,
G4091_CLA_BIEN string,
G4091_ID_PAIS string,
G4091_ORD_HIPO string,
G4091_CODIVISA string,
G4091_FCHAVCTO string,
G4091_INF_REGI string,
G4091_IDEMIS string,
G4091_CODIDPER string,
G4091_IDNUMCLI string,
G4091_EMIS_PZO string,
G4091_CODMERCD string,
G4091_NUM_TITU string,
G4091_INDICEA1 string,
G4091_TIP_INTE string,
G4091_CODCOMPA string,
G4091_GRADOCOB string,
G4091_NUM_DOC string,
G4091_INDACEPT string,
G4091_INDDDOMI string,
G4091_INDBLO string,
G4091_IMCARG string,
G4091_FECULTMO string,
G4091_CODPOSGR string,
G4091_INAYUDPB string,
G4091_TIP_HIPO string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_gara_real';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_garant_cto(
G4124_FEOPERAC string,
G4124_S1EMP  string,
G4124_CONTRA1 string,
G4124_TIP_GARA string,
G4124_BIENGAR1 string,
G4124_FECINI string,
G4124_FECBAJA string,
G4124_FECVCTO string,
G4124_COD_GAR string,
G4124_COD_GAR2 string,
G4124_TIPO_INS string,
G4124_IND_PIGN string,
G4124_TIP_AVAL string,
G4124_TIP_COBE string,
G4124_EST_GARA string,
G4124_PORCOBER string,
G4124_COB_INIC string,
G4124_IMPT_NOM string,
G4124_IMP_ACTU string,
G4124_COMF_LET string,
G4124_FECULTMO string,
G4124_NUM_ASEG string,
G4124_CODDIV    string,
G4124_IDNUMCLI string,
G4124_INDBLO string,
G4124_INDRGOSB string,
G4124_INDCOBPF string,
G4124_VALASEJU string,
G4124_REPAPORC string,
G4124_ORDAPGAR string,
G4124_RANGOHIP string,
G4124_N_IMPAGO string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_garant_cto';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_gar_posadj(
H0911_S1EMP  string,
H0911_CONTRA1 string,
H0911_FECINI string,
H0911_TIP_GARA string,
H0911_BIENGAR1 string,
H0911_FECDESDE string,
H0911_FECHASTA string,
H0911_INGARANT string,
H0911_COD_BIEN string,
H0911_CLA_BIEN string,
H0911_ID_PAIS string,
H0911_INF_REGI string,
H0911_NOMENTI string,
H0911_IMGARANT string,
H0911_CODDIV string,
H0911_FECULTMO string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_gar_posadj';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_grup_rela(
G5515_FEOPERAC string,
G5515_S1EMP  string,
G5515_GRUP_ECO string,
G5515_IDNUMCLI string,
G5515_ROL_JERA string,
E0621_FECULTMO string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_grup_rela';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_grup_econo(
G5512_FEOPERAC string,
G5512_S1EMP  string,
G5512_GRUP_ECO string,
G5512_DCODGRUP string,
G5512_DGRUPO string,
G5512_IDSUCUR string,
G5512_ID_PAIS string,
G5512_IMPFACTM string,
G5512_TOT_ACTI string,
G5512_NUM_EMPL string,
G5512_ORIG_FAC string,
G5512_ORIG_ACT string,
G5512_ORIG_EMP string,
G5512_IMPT_RGO string,
G5512_FECINFAC string,
G5512_GRECOSEC string,
G5512_CODDIV string,
G5512_FECULTMO string,
G5512_TDEUDAGR string,
G5512_FLGEMPNO string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_grup_econo';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_linliq_tit(
G4130_FEOPERAC string,
G4130_S1EMP  string,
G4130_CONTRA1 string,
G4130_COD_SPV string,
G4130_CODIGLIN string,
G4130_LIN_LIQU string,
G4130_FECULTMO string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_linliq_tit';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_inmuebles(
O9270_FEOPERAC string,
O9270_S1EMP  string,
O9270_IDINMUEB string,
O9270_TIPOUTIL string,
O9270_TAM_INMU string,
O9270_ANO_CONS string,
O9270_MET_CONS string,
O9270_COZONA string,
O9270_TIP_PPDA string,
O9270_RRVENTAF string,
O9270_RRVENTAP string,
O9270_MRVENTA string,
O9270_RRAJVTAF string,
O9270_RRAJVTAP string,
O9270_MRAJVTA string,
O9270_PTVENTAF string,
O9270_PTVENTAP string,
O9270_PPTVENTA string,
O9270_ID_METRO string,
O9270_ID_CIEST string,
O9270_PODERADQ string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_inmuebles';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_inm_posadj(
O9220_FEOPERAC string,
O9220_S1EMP    string,
O9220_IDINMUEB string,
O9220_ID_UE    string,
O9220_REF_CATA string,
O9220_DSDIREC  string,
O9220_CD_POST  string,
O9220_ID_PAIS  string,
O9220_TIP_ACTI string,
O9220_STIP_ACT string,
O9220_TIP_SUEL string,
O9220_SIT_CONS string,
O9220_IDNUMCLI string,
O9220_FECADJUD string,
O9220_ORIADJUD string,
O9220_DEUDABR  string,
O9220_PROVACAD string,
O9220_PROVDTAC string,
O9220_GASTOSAC string,
O9220_GASTOSNA string,
O9220_VALACMOM string,
O9220_VALACADJ string,
O9220_FECULTAS string,
O9220_IND_ESTA string,
O9220_PREC_ALQ string,
O9220_FEC_VENT string,
O9220_PRE_VENT string,
O9220_COS_VENT string,
O9220_ENT_VALO string,
O9220_CODDIV   string,
O9220_ENFQ_SOL string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_inm_posadj';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_carac_emisio(
E1907_S1EMP  string,
E1907_IDEMISI string,
E1907_IDEMIS string,
E1907_FECDESDE string,
E1907_FECHASTA string,
E1907_IDNUMCLI string,
E1907_CODDIV string,
E1907_FECVENTO string,
E1907_DSUBOR string,
E1907_DEUDPUBL string,
E1907_FECULTMO string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_carac_emisio';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_derivad_cred(
E1905_S1EMP  string,
E1905_CONTRA1 string,
E1905_NUMEPATA string,
E1905_IDNUMCLI string,
E1905_FECDESDE string,
E1905_FECHASTA string,
E1905_EVENREST string,
E1905_FECULTMO string,
E1905_FECRUPTR string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_derivad_cred';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_ead_netting(
E1889_S1EMP  string,
E1889_IDNETING string,
E1889_METAPLIC string,
E1889_FASECALC string,
E1889_FEC_MES string,
E1889_EAD  string,
E1889_FECULTMO string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_ead_netting';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_ead_contr(
G5519_FEOPERAC string,
G5519_S1EMP  string,
G5519_CONTRA1 string,
G5519_METAPLIC string,
G5519_FASECALC string,
G5519_MTM  string,
G5519_EAD  string,
G5519_ESPEPROV string,
G5519_FECULTMO string,
G5519_IMPNOMCT string,
G5519_ADDONBRU string,
G5519_ADDONNET string,
G5519_COEFREGU string,
G5519_METLIQUI string,
G5519_MTM_BRUT string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_ead_contr';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_rel_bargl_lo(
N0741_S1EMP  string,
N0741_CODNEGO string,
N0741_CODBARG string,
N0741_CODBARLE string,
N0741_FECDESDE string,
N0741_FECHASTA string,
N0741_FECGRAB string,
N0741_FECULTMO string,
N0741_CODNEGOL string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_rel_bargl_lo';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_rel_inmgar(
O9217_S1EMP  string,
O9217_IDINMUEB string,
O9217_BIENGAR1 string,
O9217_FECDESDE string,
O9217_FECHASTA string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_rel_inmgar';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_trz_clie_ren(
N0736_S1EMP  string,
N0736_IDNUMCLI string,
N0736_EMP_ANT string,
N0736_CLIE_ANT string,
N0736_MOTRENU string,
N0736_FEALTREL string,
N0736_FEC_MOD string,
N0736_MOTRENUG string,
N0736_FEC_BAJA string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_trz_clie_ren';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_trz_cont_ren (
G7025_S1EMP     string,
G7025_CONTRA1   string,
G7025_EMP_ANT   string,
G7025_CONT_ANT  string,
G7025_MOTRENU   string,
G7025_FEALTREL  string,
G7025_FEC_MOD   string,
G7025_IMPRESTR string,
G7025_CODDIV string,
G7025_MOTRENUG string,
G7025_FEC_BAJA string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_trz_cont_ren';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_trz_grec_ren(
O2825_S1EMP  string,
O2825_GRUP_ECO string,
O2825_EMP_ANT string,
O2825_GRUP_ANT string,
O2825_MOTRENU string,
O2825_FEALTREL string,
O2825_FEC_MOD string,
O2825_MOTRENUG string,
O2825_FEC_BAJA string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_trz_grec_ren';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_serie_titulz(
E0635_S1EMP  string,
E0635_TRAM_TIT string,
E0635_COD_SPV string,
E0635_IMPORTH string,
E0635_COD_PREL string,
E0635_AGEN_RAT string,
E0635_FCHULTAC string,
E0635_RATI_EXT string,
E0635_RATINGP string,
E0635_FECULTMO string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_serie_titulz';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_trz_inv_apli(
G7009_S1EMP  string,
G7009_IDSISTE string,
G7009_CODSISTE string,
G7009_FEC_MOD string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_trz_inv_apli';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_tituliz(
G4132_FEOPERAC string,
G4132_S1EMP  string,
G4132_CONTRA1 string,
G4132_COD_SPV string,
G4132_TRAM_TIT string,
G4132_TPORCENT string,
G4132_TPORCEN string,
G4132_IMPSLDC string,
G4132_IMPSLD string,
G4132_FECHA  string,
G4132_INDCUMP string,
G4132_FECULTMO string,
G4132_TITUPORC string,
G4132_TACTTITU string,
G4132_IMPVSYAC string,
G4132_IMINTRES string,
G4132_CTA_CONT string,
G4132_AGRCTACB string,
G4132_CTACTBSI string,
G4132_AGCTCBSI string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_tituliz';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_trz_contrato (
G7014_S1EMP  string,
G7014_CONTRA1 string,
G7014_FECDESDE string,
G7014_IDSISTE string,
G7014_CODSISTE string,
G7014_FEC_HAS string,
G7014_FEC_MOD string,
G7014_IND_TITU string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_trz_contrato';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_trz_gar_real (
G7018_S1EMP  string,
G7018_BIENGAR1 string,
G7018_FECDESDE string,
G7018_IDSISTE string,
G7018_CODSISTE string,
G7018_FEC_HAS string,
G7018_FEC_MOD string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_trz_gar_real';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_trz_gru_econ (
G7017_S1EMP  string,
G7017_GRUP_ECO string,
G7017_FECDESDE string,
G7017_IDSISTE string,
G7017_CODSISTE string,
G7017_FEC_HAS string,
G7017_FEC_MOD string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_trz_gru_econ';



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_val_gara(
G4134_FEOPERAC string,
G4134_S1EMP  string,
G4134_BIENGAR1 string,
G4134_FECHVALR string,
G4134_NOMENTI string,
G4134_IMGARANT string,
G4134_FECULTMO string,
G4134_TIPVALN string,
G4134_TIP_GARA string,
G4134_CODDIV string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_val_gara';




CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_val_inmu(
O9218_FEOPERAC string,
O9218_S1EMP  string,
O9218_IDINMUEB string,
O9218_FECHVALR string,
O9218_TIPVALIM string,
O9218_IMP_INMU string,
O9218_CODDIV string,
O9218_FECULTMO string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_val_inmu';

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_vta_carter(
R9736_FEOPERAC  string,
R9736_S1EMP   string,
R9736_CONTRA1  string,
R9736_FVTACART  string,
R9736_IMPPDTE  string,
R9736_PRECIOOB  string,
R9736_IND_CREDIT string
)
PARTITIONED BY (`partition_date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_vta_carter';
