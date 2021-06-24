SET mapred.job.queue.name=root.dataeng;

CREATE VIEW IF NOT EXISTS bi_corp_bdr.view_bdr_controles AS
select core.tabla, core.fecha_proceso, core.cod_incidencia, d.descripcion, core.num_reg_total, core.num_reg_error, core.op_timestamp as fec_ejecucion
from bi_corp_bdr.log_controles core
inner join
(select tabla, fecha_proceso, cod_incidencia, max(op_timestamp) as max_fec_ejecucion
from bi_corp_bdr.log_controles l
group by tabla, fecha_proceso, cod_incidencia) piv
       on piv.tabla = core.tabla
      and piv.fecha_proceso = core.fecha_proceso
      and piv.cod_incidencia = core.cod_incidencia
      and piv.max_fec_ejecucion = core.op_timestamp
left join bi_corp_bdr.desc_controles d on d.cod_incidencia = core.cod_incidencia;