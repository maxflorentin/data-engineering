--TAS

SELECT a.fecha,a.hora,a.tx_id,a.nup,a.importe,a.orig_rev,a.moneda,a.usuario,a.canal_id,a.maq_id,a.ente_id,a.suc_nro,a.suc_maq,a.cantidad_cheques,a.ide_pago,a.marca_man_aut,

       CASE

            WHEN a.canal_id = 'RIOS' THEN 'ISBAN'

            ELSE 'PRISMA'

       END AS tipo

       agrup.cod_grupo,

       CASE

           WHEN agrup.cod_grupo = 'CLI' THEN 'RECAUDACION ELECTRONICA'

           WHEN agrup.cod_grupo = 'CON' THEN 'CONSULTA'

           WHEN agrup.cod_grupo = 'CPOS' THEN 'COMPRAS EN TERMINALES POS'

           WHEN agrup.cod_grupo = 'DEP' THEN 'DEPOSITO'

           WHEN agrup.cod_grupo = 'DEPD' THEN 'DEPOSITO DIGITALIZADO DE CHEQUES'

           WHEN agrup.cod_grupo = 'DEPI' THEN 'DEPOSITOS INTELIGENTE'

           WHEN agrup.cod_grupo = 'EXT' THEN 'EXTRACCION'

           WHEN agrup.cod_grupo = 'NDC' THEN 'NO DERIVABLES DE CLIENTES'

           WHEN agrup.cod_grupo = 'NDI' THEN 'OPERACIÓN INTERNA'

           WHEN agrup.cod_grupo = 'ODC' THEN 'OTRAS DERIVABLES'

           WHEN agrup.cod_grupo = 'PED' THEN 'PEDIDO DE CHEQUERAS'

           WHEN agrup.cod_grupo = 'PRE' THEN 'PAGO DE PRESTAMOS'

           WHEN agrup.cod_grupo = 'RECI' THEN 'RECAUDACION ELECTRONICA EFVO'

           WHEN agrup.cod_grupo = 'REV' THEN 'REVERSO'

           WHEN agrup.cod_grupo = 'SER' THEN 'SERVICIOS'

           WHEN agrup.cod_grupo = 'TAR' THEN 'PAGO DE TARJETA'

           WHEN agrup.cod_grupo = 'TARI' THEN 'PAGO DE TARJETA INTELIGENTE'

       END AS ds_grupo,

       c.agrup_producto,c.grupo,c.descripcion,c.categoria

FROM   cnl01.movimiento@cnl01_dblink a  JOIN cnl01.agrupamiento@cnl01_dblink agrup ON a.canal_id = agrup.canal_id AND a.tx_id = agrup.tx_id

                                        JOIN cnl01.transacciones@cnl01_dblink c ON a.tx_id = c.tx_id

WHERE  a.fecha >= '01-aug-2020'

       AND a.fecha < TRUNC (SYSDATE)

       AND ((a.canal_id IN ('ATM1', 'ATM3') AND SUBSTR(TRIM(a.ide_pago),3,1) IN ('O', 'J'))

            OR (a.canal_id = 'RIOS' AND NVL (a.marca_man_aut, 'M') = 'M'))

       AND TRIM (a.orig_rev) = 'O'

       AND agrup.cod_grupo <> 'NDI'

UNION

--ATM

SELECT a.fecha,a.hora,a.tx_id,a.nup,a.importe,a.orig_rev,a.moneda,a.usuario,a.canal_id,a.maq_id,a.ente_id,a.suc_nro,a.suc_maq,a.cantidad_cheques,a.ide_pago,a.marca_man_aut,

       CASE

            WHEN a.canal_id = 'RIOS' THEN 'ISBAN'

            ELSE 'PRISMA'

       END AS tipo

       agrup.cod_grupo,

       CASE

           WHEN agrup.cod_grupo = 'CLI' THEN 'RECAUDACION ELECTRONICA'

           WHEN agrup.cod_grupo = 'CON' THEN 'CONSULTA'

           WHEN agrup.cod_grupo = 'CPOS' THEN 'COMPRAS EN TERMINALES POS'

           WHEN agrup.cod_grupo = 'DEP' THEN 'DEPOSITO'

           WHEN agrup.cod_grupo = 'DEPD' THEN 'DEPOSITO DIGITALIZADO DE CHEQUES'

           WHEN agrup.cod_grupo = 'DEPI' THEN 'DEPOSITOS INTELIGENTE'

           WHEN agrup.cod_grupo = 'EXT' THEN 'EXTRACCION'

           WHEN agrup.cod_grupo = 'NDC' THEN 'NO DERIVABLES DE CLIENTES'

           WHEN agrup.cod_grupo = 'NDI' THEN 'OPERACIÓN INTERNA'

           WHEN agrup.cod_grupo = 'ODC' THEN 'OTRAS DERIVABLES'

           WHEN agrup.cod_grupo = 'PED' THEN 'PEDIDO DE CHEQUERAS'

           WHEN agrup.cod_grupo = 'PRE' THEN 'PAGO DE PRESTAMOS'

           WHEN agrup.cod_grupo = 'RECI' THEN 'RECAUDACION ELECTRONICA EFVO'

           WHEN agrup.cod_grupo = 'REV' THEN 'REVERSO'

           WHEN agrup.cod_grupo = 'SER' THEN 'SERVICIOS'

           WHEN agrup.cod_grupo = 'TAR' THEN 'PAGO DE TARJETA'

           WHEN agrup.cod_grupo = 'TARI' THEN 'PAGO DE TARJETA INTELIGENTE'

       END AS ds_grupo,

       c.agrup_producto,c.grupo,c.descripcion,c.categoria

FROM   cnl01.movimiento@cnl01_dblink a  JOIN cnl01.agrupamiento@cnl01_dblink agrup ON agrup.canal_id = a.canal_id AND a.tx_id = agrup.tx_id

                                        JOIN cnl01.transacciones@cnl01_dblink c ON a.tx_id = c.tx_id

WHERE  a.fecha >= '01-aug-2020'

       AND a.fecha < TRUNC (SYSDATE)

       AND a.canal_id IN ('ATM1', 'ATM3')

       AND SUBSTR(TRIM(a.ide_pago),3,1) NOT IN ('O', 'J')

       AND TRIM(a.orig_rev) = 'O'

       AND agrup.cod_grupo <> 'NDI';

 

 

 

select * from cnl01.transacciones@cnl01_dblink;

Los campos que no uso son:

itaca

svc_coordinador

grupo_plantilla

tipo_Deriv

ind_automatiz

 

select * from cnl01.agrupamiento@cnl01_dblink;

Los campos que no uso son:

cod_Sector

ente_id

informa_marca

fecha_informa

 

select * from cnl01.movimiento@cnl01_dblink;

Los campos que no uso son:

tipo_cliente

fecha_proc

cuadrante

pesubseg

cantidad_efectivo

fecha_carga

cantidad_otros

importe_cheques

importe_efectivo

importe_otros

medio_pago

 

