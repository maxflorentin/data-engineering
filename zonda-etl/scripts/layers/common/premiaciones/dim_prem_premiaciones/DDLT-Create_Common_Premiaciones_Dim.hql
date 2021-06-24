
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_prem_premiaciones

(
  cod_prem_premiacion      string,
 dt_prem_desde            string,
 dt_prem_hasta            string,
 dt_prem_asignacion       string,
 ds_prem_premusuario      string,
 cod_prem_objetolog        string,
 ds_prem_premiacion       string,
 ds_prem_novedades        string,
 cod_prem_ejecucion        string,
 de_prem_canal            string,
 ds_prem_leyenda          string,
 cod_prem_solicitud        string,
 dt_prem_solicitudalta    string,
 dt_prem_solicituddesde   string,
 dt_prem_solicitudhasta   string,
 ds_prem_periodicidad     string,
 ds_prem_origen           string,
 cod_prem_soliusuario      string,
 cod_prem_interfaz        string,
 cod_prem_campania        string,
 ds_prem_campania         string,
 ds_prem_gestion          string,
 ds_prem_accionra         string,
 ds_prem_usuriopeticion   string,
 dt_prem_ultmodificacion  string,
 ds_prem_dia              string,
 ds_prem_semana           string,
 ds_prem_ejecucion        string,
 cod_prem_usuarioultmodificacion string,
 cod_prem_sectorauto      string,
 cod_prem_sectorcontrol   string,
 ds_prem_estadoauto       string,
 cod_prem_evento           string,
 ds_prem_evento           string,
 dt_prem_evedesde         string,
 dt_prem_evehasta         string,
 ds_prem_eventocorta      string


)
PARTITIONED BY (
   partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/premiaciones/dim_prem_premiaciones'
