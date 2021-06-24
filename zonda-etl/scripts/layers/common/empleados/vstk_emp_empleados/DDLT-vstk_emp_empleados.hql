SET mapred.job.queue.name=root.dataeng;

CREATE VIEW IF NOT EXISTS bi_corp_common.vstk_emp_empleados AS

with malpe as (
select
    m.partition_date,
    m.penumdoc,
    max(cast(m.penumper as bigint)) as penumper
from
    bi_corp_staging.malpe_pedt015 m
where
    trim(m.petipdoc) in ('L','T','D')
group by m.partition_date,
         m.penumdoc
)

select
    m.penumper cod_per_nup,
    p.bup_legajo cod_emp_legajo,
    p.bup_usuario ds_emp_usuario,
    p.bup_nombre ds_emp_nombre,
    p.bup_apellido ds_emp_apellido,
    p.bup_email ds_emp_email,
    cast(cuil as bigint) int_emp_cuil,
    p.estado_laboral ds_emp_estado_laboral,
    p.tel_directo ds_emp_tel_directo,
    p.tel_exte ds_emp_tel_exte,
    p.tel_celular ds_emp_tel_celular,
    cast(p.bup_puesto as int) cod_emp_puesto,
    p.bup_descripcion_puesto ds_emp_puesto,
    p.bup_ccostos cod_emp_ccostos,
    p.ccostos_descripcion ds_emp_ccostos,
    p.ubicacion_descripcion ds_emp_ubicacion,
    p.jefe_directo ds_emp_jefe_directo,
    p.bup_emp_ppal ds_emp_emp_ppal,
    date_format(p.bup_fec_eff,'yyyy-MM-dd') dt_emp_fec_eff,
    p.bup_accion ds_emp_accion,
    p.partition_date
from
    bi_corp_staging.personas_vw_lake p
left join malpe m
       on (m.penumdoc = p.cuil
      and m.partition_date = p.partition_date)
;