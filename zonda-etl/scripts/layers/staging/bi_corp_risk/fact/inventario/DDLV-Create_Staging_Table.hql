SET mapred.job.queue.name=root.dataeng;
Create view bi_corp_risk.inventario as
Select
cast(periodo as double) periodo,
cast(nup as  double) cup,
Cast(sucursal as  double) sucursal,
cast(nro_cuenta as double) nro_cuenta,
codigo_producto  ,
codigo_subproducto  ,
divisa  ,
fecha_situacion_irregular   ,
fecha_alta_producto   ,
fecha_vencimiento_producto   ,
cast(dias_atraso as int ) dias_atraso,
cast(dias_atraso_calculado as  int ) dias_atraso_calculado,
marca   ,
submarca   ,
codigo_segmento   ,
descripcion_segmento   ,
detalle_renta   ,
tipo_producto   ,
descripcion_producto   ,
tipo_reestructuracion   ,
motivo_riesgo_sub_est   ,
clasificacion_reestructuracion   ,
fecha_clasificacion_reestructuracion   ,
via_ingreso   ,
cast(importe_riesgo as double) importe_riesgo ,
cast(importe_irregular as double) importe_irregular ,
contrato_citi
from bi_corp_risk.raw_inventario;