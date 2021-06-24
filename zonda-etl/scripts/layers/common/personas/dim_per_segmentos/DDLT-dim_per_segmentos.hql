DROP TABLE IF EXISTS bi_corp_common.dim_per_segmentos;
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_per_segmentos
(
    cod_per_segmentoduro      string,
    ds_per_segmento           string,
    ds_per_subsegmento        string,
    cod_per_cuadrante         string,
    cod_per_tipopersona       string,
    flag_per_MRG                  int,
    flag_per_BPR                  int,
    flag_per_CIT                  int,
    flag_per_GEX                  int,
    flag_per_PST                  int,
    flag_per_FIG                  int,
    flag_per_UGE                  int,
    flag_per_PBA                  int,
    flag_per_BPW                  int,
    ds_per_grupo                  string,
    ds_per_marca                  string
)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\;'
    LINES TERMINATED BY '\n'
    STORED AS TEXTFILE
    LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/personas/dim/dim_per_segmentos';