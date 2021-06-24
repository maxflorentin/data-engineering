SELECT DISTINCT CONCAT(
lpad(cast(nvl(empresa,0) as string),5,'0'),
lpad(cast(nvl(cod_negocio, 0) as string),5,'0'),
lpad(nvl(cod_baremo_global,''),5,'0'),
lpad(nvl(cod_baremo_local,''),5,'0'),
nvl(fecha_desde,'0001-01-01'),
nvl(fecha_hasta,'0001-01-01'),
nvl(fecha_grabacion,'0001-01-01'),
nvl(fecha_modificacion,'0001-01-01'),
lpad(cast(nvl(cod_negocio_local,0) as string),5,'0') ) AS line
FROM bi_corp_bdr.map_baremos_global_local