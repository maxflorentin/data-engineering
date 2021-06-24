CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.control_variables_BDR (
  dag                   string,
  partition_BDR         string,
  date_from             string,
  fecha_ejecucion       string,
  nombre_tabla          string,
  origen                string,
  nombre_variable       string,
  valor_variable        string,
  cantidad_registros    bigint)
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/control_variables_BDR'