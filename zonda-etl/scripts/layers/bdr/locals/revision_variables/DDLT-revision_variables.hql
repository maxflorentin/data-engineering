CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.revision_variables (
  date_from string,
  month_bdr string,
  nombre_tabla string,
  nombre_json string,
  nueva_variable_con_BDR string,
  nueva_variable_sin_BDR string,
  variable_json string)
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/revision_variables'