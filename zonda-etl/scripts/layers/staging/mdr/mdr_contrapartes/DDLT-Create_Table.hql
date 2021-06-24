CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.mdr_contrapartes(
	COD_SIS_ORIGEN	  string,
	FECHA_GENERACION	  string,
	PK_ENTIDAD	  string,
	SHORTNAME	  string,
	RAZON_SOCIAL	  string,
	STATUS	  string,
	TIPO_ENTIDAD	  string,
	SECTOR_DE_RIESGO	  string,
	GRUPO_ECONOMICO	  string,
	RAZON_SOCIAL_GE	  string,
	GRUPO_FINANCIERO	  string,
	RAZON_SOCIAL_GF	  string,
	PAIS_ORIGEN	  string,
	FECHA_ALTA	  string,
	ALIAS_NUP	  string,
	ALIAS_SAM	  string,
	ALIAS_MUREX3	  string,
	ALIAS_FENERGO	  string
PARTITIONED BY (partition_date string)
LOCATION   '${DATA_LAKE_SERVER}/santander/bi-corp/staging/mdr/mdr_contrapartes';