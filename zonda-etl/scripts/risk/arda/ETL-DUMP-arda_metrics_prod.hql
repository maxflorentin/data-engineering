SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE LOCAL DIRECTORY '/aplicaciones/bi/zonda/tmp/bajadaArda/metricas_rda'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
SELECT
    substr(met.data_date_part,1,6) as Periodo ,
    met.data_date_part,
    met.num_persona,
    met.idf_cto_supra   ,
    substr(met.idf_cto_supra,1,locate('_',met.idf_cto_supra,1)-1) as cod_entidad,
    substr(met.idf_cto_supra,6,locate('_',met.idf_cto_supra,6)-6) as cod_centro,
    substr(met.idf_cto_supra,11,locate('_',met.idf_cto_supra,11)-11) as num_cuenta,
    substr(met.idf_cto_supra,24,locate('_',met.idf_cto_supra,24)-24) as cod_producto,
    substr(met.idf_cto_supra,27,locate('_',met.idf_cto_supra,27)-27) as cod_subprodu_altair,
    substr(met.idf_cto_supra,locate('_',met.idf_cto_supra,27)+1,3) as divisa,
    met.cod_segmento,
    cli.cod_subsegmento,
    cto.cod_marca_definitiva,
    cto.cod_submarca_definitiva,
    cto.fec_alta_cto,
    cto.fec_vencimiento,
    cto.fec_sit_irregular,
    cto.num_dia_demora,
    met.grado_feve,
    cto.situacion_bcra,
    met.cod_reesctr,
    cto.ind_cartera_citi,
    met.cod_cartera_basica,
    met.C_GGS_CR_00011,
    met.C_GGS_CR_00012,
    met.C_GGS_CR_00013,
    met.C_GGS_CR_00014,
    met.C_GGS_CR_00019,
    met.C_GGS_CR_00035,
    met.C_GGS_CR_00037,
    met.C_GGS_CR_00052,
    met.C_GGS_CR_00055,
    met.C_GGS_CR_00056,
    met.C_GGS_CR_00067,
    met.C_GGS_CR_00069,
    met.C_GGS_CR_00085,
    met.C_GGS_CR_00102,
    met.C_GGS_CR_00103,
    met.C_GGS_CR_00104,
    met.C_GGS_CR_00105,
    met.C_GGS_CR_00112,
    met.C_GGS_CR_00117,
    met.C_GGS_CR_00118,
    met.C_GGS_CR_00124,
    met.C_GGS_CR_00125,
    met.C_GGS_CR_00128,
    met.C_GGS_CR_00133,
    met.C_GGS_CR_00135,
    met.C_GGS_CR_00136,
    met.C_GGS_CR_00144,
    met.C_GGS_CR_00145,
    met.C_GGS_CR_00165,
    met.C_GGS_CR_00177,
    met.C_GGS_CR_00178,
    met.C_GGS_CR_00181,
    met.C_GGS_CR_00182,
    met.C_GGS_CR_00183,
    met.C_GGS_CR_00184,
    met.C_GGS_CR_00190,
    met.C_GGS_CR_00195,
    met.C_GGS_CR_00196,
    met.C_GGS_CR_00197,
    met.C_GGS_CR_00198,
    met.C_GGS_CR_00200,
    met.C_GGS_CR_00201,
    met.C_GGS_CR_00202,
    met.C_GGS_CR_00208,
    met.C_GGS_CR_00276,
    met.C_GGS_CR_00277,
    met.C_GGS_CR_00278,
    met.C_GGS_CR_00279,
    met.C_GGS_CR_00282,
    met.C_GGS_CR_00289,
    met.C_GGS_CR_00299,
    met.C_GGS_CR_00305,
    met.C_GGS_CR_00307,
    met.C_GGS_CR_00311,
    met.C_GGS_CR_00344,
    met.C_GGS_CR_00345,
    met.C_GGS_CR_00346,
    met.C_GGS_CR_00347,
    met.C_GGS_CR_00348,
    met.C_GGS_CR_00349,
    met.C_GGS_CR_00350,
    met.C_GGS_CR_00351,
    met.C_GGS_CR_00353,
    met.C_GGS_CR_00354,
    met.C_GGS_CR_00355,
    met.C_GGS_CR_00369,
    met.C_GGS_CR_00370,
    met.C_GGS_CR_00373,
    met.C_GGS_CR_00375
FROM
    santander_business_risk_arda.vista_metricas_riesgo_credito met
        left join santander_business_risk_arda.contratos cto
                  on met.data_date_part = cto.data_date_part
                      and met.idf_cto_supra = cto.idf_cto_supra
        left join santander_business_risk_arda.clientes cli
                  on met.data_date_part = cli.data_date_part
                      and met.num_persona = cli.num_persona
WHERE
        met.data_date_part = '20200131'
  and met.grouping__id = '0'
  and (met.C_GGS_CR_00011 > 0 or
       met.C_GGS_CR_00012 > 0 or
       met.C_GGS_CR_00013 > 0 or
       met.C_GGS_CR_00014 > 0 or
       met.C_GGS_CR_00019 > 0 or
       met.C_GGS_CR_00035 > 0 or
       met.C_GGS_CR_00037 > 0 or
       met.C_GGS_CR_00052 > 0 or
       met.C_GGS_CR_00055 > 0 or
       met.C_GGS_CR_00056 > 0 or
       met.grado_feve > 0 or
       met.C_GGS_CR_00067 > 0 or
       met.C_GGS_CR_00069 > 0 or
       met.C_GGS_CR_00085 > 0 or
       met.C_GGS_CR_00102 > 0 or
       met.C_GGS_CR_00103 > 0 or
       met.C_GGS_CR_00104 > 0 or
       met.C_GGS_CR_00105 > 0 or
       met.C_GGS_CR_00112 > 0 or
       met.C_GGS_CR_00117 > 0 or
       met.C_GGS_CR_00118 > 0 or
       met.C_GGS_CR_00124 > 0 or
       met.C_GGS_CR_00125 > 0 or
       met.C_GGS_CR_00128 > 0 or
       met.C_GGS_CR_00133 > 0 or
       met.C_GGS_CR_00135 > 0 or
       met.C_GGS_CR_00136 > 0 or
       met.C_GGS_CR_00144 > 0 or
       met.C_GGS_CR_00145 > 0 or
       met.C_GGS_CR_00165 > 0 or
       met.C_GGS_CR_00177 > 0 or
       met.C_GGS_CR_00178 > 0 or
       met.C_GGS_CR_00181 > 0 or
       met.C_GGS_CR_00182 > 0 or
       met.C_GGS_CR_00183 > 0 or
       met.C_GGS_CR_00184 > 0 or
       met.C_GGS_CR_00190 > 0 or
       met.C_GGS_CR_00195 > 0 or
       met.C_GGS_CR_00196 > 0 or
       met.C_GGS_CR_00197 > 0 or
       met.C_GGS_CR_00198 > 0 or
       met.C_GGS_CR_00200 > 0 or
       met.C_GGS_CR_00201 > 0 or
       met.C_GGS_CR_00202 > 0 or
       met.C_GGS_CR_00208 > 0 or
       met.C_GGS_CR_00276 > 0 or
       met.C_GGS_CR_00277 > 0 or
       met.C_GGS_CR_00278 > 0 or
       met.C_GGS_CR_00279 > 0 or
       met.C_GGS_CR_00282 > 0 or
       met.C_GGS_CR_00289 > 0 or
       met.cod_reesctr > 0 or
       met.C_GGS_CR_00299 > 0 or
       met.C_GGS_CR_00305 > 0 or
       met.C_GGS_CR_00307 > 0 or
       met.C_GGS_CR_00311 > 0 or
       met.C_GGS_CR_00344 > 0 or
       met.C_GGS_CR_00345 > 0 or
       met.C_GGS_CR_00346 > 0 or
       met.C_GGS_CR_00347 > 0 or
       met.C_GGS_CR_00348 > 0 or
       met.C_GGS_CR_00349 > 0 or
       met.C_GGS_CR_00350 > 0 or
       met.C_GGS_CR_00351 > 0 or
       met.C_GGS_CR_00353 > 0 or
       met.C_GGS_CR_00354 > 0 or
       met.C_GGS_CR_00355 > 0 or
       met.C_GGS_CR_00369 > 0 or
       met.C_GGS_CR_00370 > 0 or
       met.C_GGS_CR_00373 > 0 or
       met.C_GGS_CR_00375 > 0)
LIMIT 1000000000
;