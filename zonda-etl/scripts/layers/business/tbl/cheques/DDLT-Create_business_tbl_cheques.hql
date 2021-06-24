CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_business.tbl_cheques
(

cod_kpi_segmento    string,
cod_kpi_subsegmento string,
ds_kpi_producto    string,
ds_kpi_subproducto string,
cod_kpi_canal       string,
ds_kpi_destino      string,
cod_kpi_zona        string,
cod_kpi_sucursal    string,
cod_kpi_divisa      string,
fc_kpi_metrica                      decimal(20,2),
fc_kpi_metricamismodiamesanterior   decimal(20,2),
fc_kpi_metricadiaanterior           decimal(20,2),
fc_kpi_metricafindemesanterior     decimal(20,2),
fc_kpi_metricamismodiaanoanterior   decimal(20,2),
fc_kpi_metricafindeanoanterior      decimal(20,2),
fc_kpi_metricasemanaanterior        decimal(20,2),
fc_kpi_clientes                     int,
fc_kpi_clientesmismodiamesanterior  int,
fc_kpi_clientesdiaanterior          int,
fc_kpi_clientesfindemesanterior     int,
fc_kpi_clientesmismodiaanoanterior  int,
fc_kpi_clientesfindeanoanterior     int,
fc_kpi_clientessemanaanterior       int,
fc_kpi_operaciones                  int,
fc_kpi_operacionesmismodiamesanterior int,
fc_kpi_operacionesdiaanterior       int,
fc_kpi_operacionesfindemesanterior  int,
fc_kpi_operacionesmismodiaanoanterior   int,
fc_kpi_operacionesfindeanoanterior  int,
fc_kpi_operacionessemanaanterior    int)
PARTITIONED BY (
  cod_kpi_kpi string, partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/business/daily_dashboard/agg/agg_all_daily_dashboard'
