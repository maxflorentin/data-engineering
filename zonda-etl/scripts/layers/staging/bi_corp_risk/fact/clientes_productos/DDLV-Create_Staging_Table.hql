SET mapred.job.queue.name=root.dataeng;
CREATE VIEW bi_corp_risk.clientes_productos as 
select  nup, nro_cuenta, sucursal, tipo_participante, 
orden_participante, codigo_producto, codigo_subproducto,
(case when fecha_cierre = 'null' then  null else fecha_cierre end) as fecha_cierre,
estado, marca_paquete, 
(case when motivo_baja = 'null' then  null else motivo_baja end) as motivo_baja, 
(case when fecha_alta = 'null' then  null else fecha_alta end) as fecha_alta,
nro_cuenta_, fecha_informacion 
from bi_corp_risk.raw_clientes_productos 
;