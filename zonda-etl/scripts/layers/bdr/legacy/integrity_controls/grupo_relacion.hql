--El identificador de grupo económico debe de existir en la tabla Grupos Económicos.	R0033344343

SELECT grl.partition_date as fecha_proceso,
       'R0033344343' as cod_incidencia,
       count(*) as num_reg_total,
       sum(case when grl.g5515_grup_eco is null then 1 else 0 end ) as num_reg_error,
       from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_grup_rela  grl
         left join bi_corp_bdr.jm_grup_econo gec on grl.g5515_grup_eco = grl.g5515_grup_eco and grl.partition_date = gec.partiton_date
where grl.partition_date = '${month_partition_bdr}'
group by grl.partition_date;

--El identificador de cliente debe existir en la tabla Clientes	R0034344341

SELECT grl.partition_date as fecha_proceso,
       'R0034344341' as cod_incidencia,
       count(*) as num_reg_total,
       sum(case when grl.g5515_idnumcli is null then 1 else 0 end ) as num_reg_error,
       from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_grup_rela  grl
         left join  bi_corp_bdr.jm_client_bii cbi on grl.g5515_idnumcli = cbi.g4093_idnumcli and grl.partition_date = cbi.partition_date
where grl.partition_date = '${month_partition_bdr}'
group by grl.partition_date;



--No deben existir registros duplicados	R0125344

SELECT grl.partition_date as fecha_proceso,
       'R0125344' as cod_incidencia,
       count(*) as num_reg_total,
       count(*) - count(distinct g5515_grup_eco, g5515_idnumcli) as num_reg_error,
       from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_grup_rela  grl
where grl.partition_date = '${month_partition_bdr}'
group by grl.partition_date;


--El valor del campo Empresa debe existir en la tabla de baremos correspondiente (3.4.4)	R1304344

select
    grl.partition_date as fecha_proceso,
    'R1304344' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_grup_rela  grl
         left join bi_corp_bdr.map_baremos_global_local bgl on
            bgl.cod_negocio = 1 and --Verificar el BAREMO
            bgl.cod_baremo_global = grl.g5515_s1emp
where grl.partition_date = '${month_partition_bdr}'
group by grl.partition_date;



--El valor del campo Rol Jerárquico debe existir en la tabla de baremos correspondiente (3.4.4)	R1305344

select
    grl.partition_date as fecha_proceso,
    'R1305344' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_grup_rela  grl
         left join bi_corp_bdr.map_baremos_global_local bgl on
            bgl.cod_negocio = 58 and --Verificar el BAREMO
            bgl.cod_baremo_global = grl.g5515_rol_jera
where grl.partition_date = '${month_partition_bdr}'
group by grl.partition_date; 
