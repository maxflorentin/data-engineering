----------------MOVIMIENTOS A------------------------------------------------

WITH
------------  Paquete Movimientos -----------------------------------------/
MOV AS (
    SELECT
      A.mo1_centro_alta
    , A.mo1_cuenta
    , A.mo1_divisa
    , A.mo1_producto
    , A.mo1_subprodu
    , CASE
        WHEN A.mo1_divisa = 'USD'
        THEN SUM(A.mo1_importe * 69.62)
        ELSE SUM(A.mo1_importe)
    END AS IMP_TOT
    ---------------- Movimientos -----------------------------------------/
    FROM bi_corp_staging.malbgc_bgdtmo1 A
    WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
    AND A.mo1_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
    AND A.mo1_codigo IN(
      '0301', '0302', '0303', '0304', '0305', '0306', '0308'
    , '0310', '0415', '0471', '0565', '0686', '0690', '0698'
    , '1311', '1319', '1322', '1323', '1329', '1533', '1699'
    , '1839', '1912', '1914', '1915', '2025', '2089', '2090'
    , '2266', '2382', '2383', '2385', '2494', '2495', '2551'
    , '3048', '3185', '4520', '4521', '4562', '5301', '5302'
    , '5303', '5304', '5305', '5306', '5308', '5565', '5586'
    , '5686', '6311', '6322', '6323', '6329', '6533', '6839'
    , '7025', '8185', '9520', '9521', '9562'
    )
    GROUP BY 
      A.mo1_centro_alta
    , A.mo1_cuenta
    , A.mo1_producto
    , A.mo1_subprodu
    , A.mo1_divisa
    ----------------------------------------------------------------------/
    -----------------   UNION   MO2  -------------------------------------/
    UNION
    ----------------------------------------------------------------------/
    SELECT
      A.mo2_centro_alta
    , A.mo2_cuenta
    , A.mo2_divisa
    , A.mo2_producto
    , A.mo2_subprodu
    , CASE
        WHEN A.mo2_divisa = 'USD'
        THEN SUM(A.mo2_importe * 69.62)
        ELSE SUM(A.mo2_importe)
    END AS IMP_TOT
    ---------------- Movimientos -----------------------------------------/
    FROM bi_corp_staging.malbgc_bgdtmo2 A
    WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
    AND A.mo2_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
    AND A.mo2_codigo IN(
      '0301', '0302', '0303', '0304', '0305', '0306', '0308'
    , '0310', '0415', '0471', '0565', '0686', '0690', '0698'
    , '1311', '1319', '1322', '1323', '1329', '1533', '1699'
    , '1839', '1912', '1914', '1915', '2025', '2089', '2090'
    , '2266', '2382', '2383', '2385', '2494', '2495', '2551'
    , '3048', '3185', '4520', '4521', '4562', '5301', '5302'
    , '5303', '5304', '5305', '5306', '5308', '5565', '5586'
    , '5686', '6311', '6322', '6323', '6329', '6533', '6839'
    , '7025', '8185', '9520', '9521', '9562'
    )
    GROUP BY 
      A.mo2_centro_alta
    , A.mo2_cuenta
    , A.mo2_producto
    , A.mo2_subprodu
    , A.mo2_divisa
    ----------------------------------------------------------------------/
    -----------------   UNION   MOB  -------------------------------------/
    UNION
    ----------------------------------------------------------------------/
    SELECT
      A.mob_centro_alta
    , A.mob_cuenta
    , A.mob_divisa
    , A.mob_producto
    , A.mob_subprodu
    , CASE
        WHEN A.mob_divisa = 'USD'
        THEN SUM(A.mob_importe * 69.62)
        ELSE SUM(A.mob_importe)
    END AS IMP_TOT
    ---------------- Movimientos -----------------------------------------/
    FROM bi_corp_staging.malbgc_bgdtmob A
    WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
    AND A.mob_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
    AND A.mob_codigo IN(
      '0301', '0302', '0303', '0304', '0305', '0306', '0308'
    , '0310', '0415', '0471', '0565', '0686', '0690', '0698'
    , '1311', '1319', '1322', '1323', '1329', '1533', '1699'
    , '1839', '1912', '1914', '1915', '2025', '2089', '2090'
    , '2266', '2382', '2383', '2385', '2494', '2495', '2551'
    , '3048', '3185', '4520', '4521', '4562', '5301', '5302'
    , '5303', '5304', '5305', '5306', '5308', '5565', '5586'
    , '5686', '6311', '6322', '6323', '6329', '6533', '6839'
    , '7025', '8185', '9520', '9521', '9562'
    )
    GROUP BY 
      A.mob_centro_alta
    , A.mob_cuenta
    , A.mob_producto
    , A.mob_subprodu
    , A.mob_divisa
)
--//////////////////////////////////////////////////////////////////////--
--///////////////////   MAIN                     ///////////////////////--
--//////////////////////////////////////////////////////////////////////--
SELECT 
  B.penumper AS 'NUP'
, E.petipper AS 'Tipo_Persona'
, C.pesegcla AS 'Segmento'
, C.pesegcal AS 'Subsegmento'
, D.pevalind AS 'Riesgo'
, D.peindica AS 'Indicador'
, A.mo1_centro_alta
, A.mo1_cuenta
, SUM(A.IMP_TOT)
---------------- Movimientos -----------------------------------------/
FROM MOV A
---------------- NUP -------------------------------------------------/
LEFT JOIN bi_corp_staging.malpe_pedt008 B
 ON SUBSTRING(A.mo1_cuenta,4,12) = SUBSTRING(B.penumcon,4,12)
AND A.mo1_centro_alta = B.pecodofi
AND A.mo1_producto = B.pecodpro
AND A.mo1_subprodu = B.pecodsub
---------------- Segmento --------------------------------------------/
LEFT JOIN bi_corp_staging.malpe_pedt030 C
 ON C.partition_date = B.partition_date
AND C.penumper = B.penumper
 ---------------- Riesgo ---------------------------------------------/
LEFT JOIN bi_corp_staging.malpe_pedt052 D
 ON D.partition_date = B.partition_date
AND D.penumper = B.penumper
 ---------------- Tipo Pers ------------------------------------------/
LEFT JOIN bi_corp_staging.malpe_pedt001 E
 ON E.partition_date = B.partition_date
AND E.penumper = B.penumper
 ---------------------------------------------------------------------/
WHERE B.partition_date = '2020-05-29'
AND B.peordpar = '001'
AND D.peindica IN ('RSG', 'FUN')
AND concat(A.mo1_cuenta, A.mo1_centro_alta) IN(
	SELECT
	  concat(mo1_cuenta, mo1_centro_alta)
	FROM MOV
	GROUP BY
	  mo1_centro_alta
	, mo1_cuenta
	HAVING SUM(MOV.IMP_TOT) <= -840000
)
GROUP BY
  B.penumper
, E.petipper
, C.pesegcla
, C.pesegcal
, D.pevalind
, D.peindica
, A.mo1_centro_alta
, A.mo1_cuenta
;

----------------MOVIMIENTOS B------------------------------------------------

WITH 
------------  Paquete Movimientos -----------------------------------------/
MOV AS (
    SELECT
      A.mo1_centro_alta
    , A.mo1_cuenta
    , A.mo1_divisa
    , CASE
        WHEN A.mo1_divisa = 'USD'
        THEN SUM(A.mo1_importe * 69.62)
        ELSE SUM(A.mo1_importe)
    END AS IMP_TOT
    , count(*) AS CANT
    ---------------- Movimientos -----------------------------------------/
    FROM bi_corp_staging.malbgc_bgdtmo1 A
    WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
    AND A.mo1_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
    AND A.mo1_codigo IN(
      '0032', '0037', '0038', '0039', '0047', '0050', '0081', '0082'
    , '0281', '0469', '0799', '0800', '1293', '1294', '1295', '1296'
    , '1532', '1700', '1913', '1916', '1937', '1938', '2029', '2030'
    , '2077', '2079', '2258', '2260', '2309', '3063', '3187', '3188'
    , '4522', '4523', '4746', '4747', '4949', '5003', '5032', '5037'
    , '5038', '5039', '5047', '5050', '5081', '5082', '5280', '5281'
    , '5469', '5589', '5658', '5799', '5800', '6293', '6294', '6295'
    , '6296', '6320', '6532', '6700', '6911', '6913', '6916', '6937'
    , '6938', '7029', '7030', '7258', '7260', '7309', '8063', '8187'
    , '8188', '9522', '9523', '9746', '9747'
    )
    GROUP BY 
      A.mo1_centro_alta
    , A.mo1_cuenta
    , A.mo1_divisa
    ----------------------------------------------------------------------/
    -----------------   UNION   MO2  -------------------------------------/
    UNION
    ----------------------------------------------------------------------/
    SELECT
      A.mo2_centro_alta
    , A.mo2_cuenta
    , A.mo2_divisa
    , CASE
        WHEN A.mo2_divisa = 'USD'
        THEN SUM(A.mo2_importe * 69.62)
        ELSE SUM(A.mo2_importe)
    END AS IMP_TOT
    , count(*) AS CANT
    ---------------- Movimientos -----------------------------------------/
    FROM bi_corp_staging.malbgc_bgdtmo2 A
    WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
    AND A.mo2_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
    AND A.mo2_codigo IN(
      '0032', '0037', '0038', '0039', '0047', '0050', '0081', '0082'
    , '0281', '0469', '0799', '0800', '1293', '1294', '1295', '1296'
    , '1532', '1700', '1913', '1916', '1937', '1938', '2029', '2030'
    , '2077', '2079', '2258', '2260', '2309', '3063', '3187', '3188'
    , '4522', '4523', '4746', '4747', '4949', '5003', '5032', '5037'
    , '5038', '5039', '5047', '5050', '5081', '5082', '5280', '5281'
    , '5469', '5589', '5658', '5799', '5800', '6293', '6294', '6295'
    , '6296', '6320', '6532', '6700', '6911', '6913', '6916', '6937'
    , '6938', '7029', '7030', '7258', '7260', '7309', '8063', '8187'
    , '8188', '9522', '9523', '9746', '9747'
    )
    GROUP BY 
      A.mo2_centro_alta
    , A.mo2_cuenta
    , A.mo2_divisa
    ----------------------------------------------------------------------/
    -----------------   UNION   MOB  -------------------------------------/
    UNION
    ----------------------------------------------------------------------/
    SELECT
      A.mob_centro_alta
    , A.mob_cuenta
    , A.mob_divisa
    , CASE
        WHEN A.mob_divisa = 'USD'
        THEN SUM(A.mob_importe * 69.62)
        ELSE SUM(A.mob_importe)
    END AS IMP_TOT
    , count(*) AS CANT
    ---------------- Movimientos -----------------------------------------/
    FROM bi_corp_staging.malbgc_bgdtmob A
    WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
    AND A.mob_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
    AND A.mob_codigo IN(
      '0032', '0037', '0038', '0039', '0047', '0050', '0081', '0082'
    , '0281', '0469', '0799', '0800', '1293', '1294', '1295', '1296'
    , '1532', '1700', '1913', '1916', '1937', '1938', '2029', '2030'
    , '2077', '2079', '2258', '2260', '2309', '3063', '3187', '3188'
    , '4522', '4523', '4746', '4747', '4949', '5003', '5032', '5037'
    , '5038', '5039', '5047', '5050', '5081', '5082', '5280', '5281'
    , '5469', '5589', '5658', '5799', '5800', '6293', '6294', '6295'
    , '6296', '6320', '6532', '6700', '6911', '6913', '6916', '6937'
    , '6938', '7029', '7030', '7258', '7260', '7309', '8063', '8187'
    , '8188', '9522', '9523', '9746', '9747'
    )
    GROUP BY 
      A.mob_centro_alta
    , A.mob_cuenta
    , A.mob_divisa
)
--//////////////////////////////////////////////////////////////////////--
--///////////////////   MAIN                     ///////////////////////--
--//////////////////////////////////////////////////////////////////////--
select 
      mo1_centro_alta
    , mo1_cuenta
    , SUM(IMP_TOT)
    , SUM(CANT)
  FROM MOV
	GROUP BY
	  mo1_centro_alta
	, mo1_cuenta
	HAVING SUM(MOV.IMP_TOT) >= 200000
       AND SUM(CANT) >= 75
;

----------------MOVIMIENTOS C------------------------------------------------

Alerta U434.Retiro en efectivo

Archivo 1/3
------------

WITH 
------------  Paquete Movimientos -----------------------------------------/
MOV AS (
    SELECT
      A.mo1_centro_alta
    , A.mo1_cuenta
    , A.mo1_divisa
    , CASE
        WHEN A.mo1_divisa = 'USD'
        THEN SUM(A.mo1_importe * 69.62)
        ELSE SUM(A.mo1_importe)
    END AS IMP_TOT
    ---------------- Movimientos -----------------------------------------/
    FROM bi_corp_staging.malbgc_bgdtmo1 A
    WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
    AND A.mo1_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
    AND A.mo1_codigo IN(
      '0301', '0302', '0303', '0304', '0305', '0306', '0308'
    , '0310', '0415', '0471', '0565', '0686', '0690', '0698'
    , '1311', '1319', '1322', '1323', '1329', '1533', '1699'
    , '1839', '1912', '1914', '1915', '2025', '2089', '2090'
    , '2266', '2382', '2383', '2385', '2494', '2495', '2551'
    , '3048', '3185', '4520', '4521', '4562', '5301', '5302'
    , '5303', '5304', '5305', '5306', '5308', '5565', '5586'
    , '5686', '6311', '6322', '6323', '6329', '6533', '6839'
    , '7025', '8185', '9520', '9521', '9562'
    )
    GROUP BY 
      A.mo1_centro_alta
    , A.mo1_cuenta
    , A.mo1_divisa
    ----------------------------------------------------------------------/
    -----------------   UNION   MO2  -------------------------------------/
    UNION
    ----------------------------------------------------------------------/
    SELECT
      A.mo2_centro_alta
    , A.mo2_cuenta
    , A.mo2_divisa
    , CASE
        WHEN A.mo2_divisa = 'USD'
        THEN SUM(A.mo2_importe * 69.62)
        ELSE SUM(A.mo2_importe)
    END AS IMP_TOT
    ---------------- Movimientos -----------------------------------------/
    FROM bi_corp_staging.malbgc_bgdtmo2 A
    WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
    AND A.mo2_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
    AND A.mo2_codigo IN(
      '0301', '0302', '0303', '0304', '0305', '0306', '0308'
    , '0310', '0415', '0471', '0565', '0686', '0690', '0698'
    , '1311', '1319', '1322', '1323', '1329', '1533', '1699'
    , '1839', '1912', '1914', '1915', '2025', '2089', '2090'
    , '2266', '2382', '2383', '2385', '2494', '2495', '2551'
    , '3048', '3185', '4520', '4521', '4562', '5301', '5302'
    , '5303', '5304', '5305', '5306', '5308', '5565', '5586'
    , '5686', '6311', '6322', '6323', '6329', '6533', '6839'
    , '7025', '8185', '9520', '9521', '9562'
    )
    GROUP BY 
      A.mo2_centro_alta
    , A.mo2_cuenta
    , A.mo2_divisa
    ----------------------------------------------------------------------/
    -----------------   UNION   MOB  -------------------------------------/
    UNION
    ----------------------------------------------------------------------/
    SELECT
      A.mob_centro_alta
    , A.mob_cuenta
    , A.mob_divisa
    , CASE
        WHEN A.mob_divisa = 'USD'
        THEN SUM(A.mob_importe * 69.62)
        ELSE SUM(A.mob_importe)
    END AS IMP_TOT
    ---------------- Movimientos -----------------------------------------/
    FROM bi_corp_staging.malbgc_bgdtmob A
    WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
    AND A.mob_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
    AND A.mob_codigo IN(
      '0301', '0302', '0303', '0304', '0305', '0306', '0308'
    , '0310', '0415', '0471', '0565', '0686', '0690', '0698'
    , '1311', '1319', '1322', '1323', '1329', '1533', '1699'
    , '1839', '1912', '1914', '1915', '2025', '2089', '2090'
    , '2266', '2382', '2383', '2385', '2494', '2495', '2551'
    , '3048', '3185', '4520', '4521', '4562', '5301', '5302'
    , '5303', '5304', '5305', '5306', '5308', '5565', '5586'
    , '5686', '6311', '6322', '6323', '6329', '6533', '6839'
    , '7025', '8185', '9520', '9521', '9562'
    )
    GROUP BY 
      A.mob_centro_alta
    , A.mob_cuenta
    , A.mob_divisa
)
--//////////////////////////////////////////////////////////////////////--
--///////////////////   MAIN                     ///////////////////////--
--//////////////////////////////////////////////////////////////////////--
SELECT 
  B.penumper AS 'NUP'
, E.petipper AS 'Tipo_Persona'
, C.pesegcla AS 'Segmento'
, C.pesegcal AS 'Subsegmento'
, D.pevalind AS 'Riesgo'
, D.peindica AS 'Indicador'
, A.mo1_cuenta
, A.mo1_producto
, A.mo1_subprodu
, A.mo1_codigo
, A.mo1_fecha_valor
, A.mo1_divisa
, A.mo1_importe
---------------- Movimientos -----------------------------------------/
FROM bi_corp_staging.malbgc_bgdtmo1 A
---------------- NUP -------------------------------------------------/
LEFT JOIN bi_corp_staging.malpe_pedt008 B
 ON SUBSTRING(A.mo1_cuenta,4,12) = SUBSTRING(B.penumcon,4,12)
AND A.mo1_centro_alta = B.pecodofi
AND A.mo1_producto = B.pecodpro
AND A.mo1_subprodu = B.pecodsub
---------------- Segmento --------------------------------------------/
LEFT JOIN bi_corp_staging.malpe_pedt030 C
 ON C.partition_date = B.partition_date
AND C.penumper = B.penumper
 ---------------- Riesgo ---------------------------------------------/
LEFT JOIN bi_corp_staging.malpe_pedt052 D
 ON D.partition_date = B.partition_date
AND D.penumper = B.penumper
 ---------------- Tipo Pers ------------------------------------------/
LEFT JOIN bi_corp_staging.malpe_pedt001 E
 ON E.partition_date = B.partition_date
AND E.penumper = B.penumper
 ---------------------------------------------------------------------/
WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
AND B.partition_date = '2020-05-29'
AND B.peordpar = '001'
AND A.mo1_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
AND A.mo1_codigo IN(
  '0301', '0302', '0303', '0304', '0305', '0306', '0308'
, '0310', '0415', '0471', '0565', '0686', '0690', '0698'
, '1311', '1319', '1322', '1323', '1329', '1533', '1699'
, '1839', '1912', '1914', '1915', '2025', '2089', '2090'
, '2266', '2382', '2383', '2385', '2494', '2495', '2551'
, '3048', '3185', '4520', '4521', '4562', '5301', '5302'
, '5303', '5304', '5305', '5306', '5308', '5565', '5586'
, '5686', '6311', '6322', '6323', '6329', '6533', '6839'
, '7025', '8185', '9520', '9521', '9562'
)
AND D.peindica IN ('RSG', 'FUN')
AND concat(A.mo1_cuenta, A.mo1_centro_alta) IN(
	SELECT
	  concat(mo1_cuenta, mo1_centro_alta)
	FROM MOV
	GROUP BY
	  mo1_centro_alta
	, mo1_cuenta
	HAVING SUM(MOV.IMP_TOT) <= -840000
)
;


Archivo 2/3
------------

WITH 
------------  Paquete Movimientos -----------------------------------------/
MOV AS (
    SELECT
      A.mo1_centro_alta
    , A.mo1_cuenta
    , A.mo1_divisa
    , CASE
        WHEN A.mo1_divisa = 'USD'
        THEN SUM(A.mo1_importe * 69.62)
        ELSE SUM(A.mo1_importe)
    END AS IMP_TOT
    ---------------- Movimientos -----------------------------------------/
    FROM bi_corp_staging.malbgc_bgdtmo1 A
    WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
    AND A.mo1_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
    AND A.mo1_codigo IN(
      '0301', '0302', '0303', '0304', '0305', '0306', '0308'
    , '0310', '0415', '0471', '0565', '0686', '0690', '0698'
    , '1311', '1319', '1322', '1323', '1329', '1533', '1699'
    , '1839', '1912', '1914', '1915', '2025', '2089', '2090'
    , '2266', '2382', '2383', '2385', '2494', '2495', '2551'
    , '3048', '3185', '4520', '4521', '4562', '5301', '5302'
    , '5303', '5304', '5305', '5306', '5308', '5565', '5586'
    , '5686', '6311', '6322', '6323', '6329', '6533', '6839'
    , '7025', '8185', '9520', '9521', '9562'
    )
    GROUP BY 
      A.mo1_centro_alta
    , A.mo1_cuenta
    , A.mo1_divisa
    ----------------------------------------------------------------------/
    -----------------   UNION   MO2  -------------------------------------/
    UNION
    ----------------------------------------------------------------------/
    SELECT
      A.mo2_centro_alta
    , A.mo2_cuenta
    , A.mo2_divisa
    , CASE
        WHEN A.mo2_divisa = 'USD'
        THEN SUM(A.mo2_importe * 69.62)
        ELSE SUM(A.mo2_importe)
    END AS IMP_TOT
    ---------------- Movimientos -----------------------------------------/
    FROM bi_corp_staging.malbgc_bgdtmo2 A
    WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
    AND A.mo2_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
    AND A.mo2_codigo IN(
      '0301', '0302', '0303', '0304', '0305', '0306', '0308'
    , '0310', '0415', '0471', '0565', '0686', '0690', '0698'
    , '1311', '1319', '1322', '1323', '1329', '1533', '1699'
    , '1839', '1912', '1914', '1915', '2025', '2089', '2090'
    , '2266', '2382', '2383', '2385', '2494', '2495', '2551'
    , '3048', '3185', '4520', '4521', '4562', '5301', '5302'
    , '5303', '5304', '5305', '5306', '5308', '5565', '5586'
    , '5686', '6311', '6322', '6323', '6329', '6533', '6839'
    , '7025', '8185', '9520', '9521', '9562'
    )
    GROUP BY 
      A.mo2_centro_alta
    , A.mo2_cuenta
    , A.mo2_divisa
    ----------------------------------------------------------------------/
    -----------------   UNION   MOB  -------------------------------------/
    UNION
    ----------------------------------------------------------------------/
    SELECT
      A.mob_centro_alta
    , A.mob_cuenta
    , A.mob_divisa
    , CASE
        WHEN A.mob_divisa = 'USD'
        THEN SUM(A.mob_importe * 69.62)
        ELSE SUM(A.mob_importe)
    END AS IMP_TOT
    ---------------- Movimientos -----------------------------------------/
    FROM bi_corp_staging.malbgc_bgdtmob A
    WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
    AND A.mob_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
    AND A.mob_codigo IN(
      '0301', '0302', '0303', '0304', '0305', '0306', '0308'
    , '0310', '0415', '0471', '0565', '0686', '0690', '0698'
    , '1311', '1319', '1322', '1323', '1329', '1533', '1699'
    , '1839', '1912', '1914', '1915', '2025', '2089', '2090'
    , '2266', '2382', '2383', '2385', '2494', '2495', '2551'
    , '3048', '3185', '4520', '4521', '4562', '5301', '5302'
    , '5303', '5304', '5305', '5306', '5308', '5565', '5586'
    , '5686', '6311', '6322', '6323', '6329', '6533', '6839'
    , '7025', '8185', '9520', '9521', '9562'
    )
    GROUP BY 
      A.mob_centro_alta
    , A.mob_cuenta
    , A.mob_divisa
)
--//////////////////////////////////////////////////////////////////////--
--///////////////////   MAIN                     ///////////////////////--
--//////////////////////////////////////////////////////////////////////--
SELECT 
  B.penumper AS 'NUP'
, E.petipper AS 'Tipo_Persona'
, C.pesegcla AS 'Segmento'
, C.pesegcal AS 'Subsegmento'
, D.pevalind AS 'Riesgo'
, D.peindica AS 'Indicador'
, A.mo2_cuenta
, A.mo2_producto
, A.mo2_subprodu
, A.mo2_codigo
, A.mo2_fecha_valor
, A.mo2_divisa
, A.mo2_importe
---------------- Movimientos -----------------------------------------/
FROM bi_corp_staging.malbgc_bgdtmo2 A
---------------- NUP -------------------------------------------------/
LEFT JOIN bi_corp_staging.malpe_pedt008 B
 ON SUBSTRING(A.mo2_cuenta,4,12) = SUBSTRING(B.penumcon,4,12)
AND A.mo2_centro_alta = B.pecodofi
AND A.mo2_producto = B.pecodpro
AND A.mo2_subprodu = B.pecodsub
---------------- Segmento --------------------------------------------/
LEFT JOIN bi_corp_staging.malpe_pedt030 C
 ON C.partition_date = B.partition_date
AND C.penumper = B.penumper
 ---------------- Riesgo ---------------------------------------------/
LEFT JOIN bi_corp_staging.malpe_pedt052 D
 ON D.partition_date = B.partition_date
AND D.penumper = B.penumper
 ---------------- Tipo Pers ------------------------------------------/
LEFT JOIN bi_corp_staging.malpe_pedt001 E
 ON E.partition_date = B.partition_date
AND E.penumper = B.penumper
 ---------------------------------------------------------------------/
WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
AND B.partition_date = '2020-05-29'
AND B.peordpar = '001'
AND A.mo2_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
AND A.mo2_codigo IN(
  '0301', '0302', '0303', '0304', '0305', '0306', '0308'
, '0310', '0415', '0471', '0565', '0686', '0690', '0698'
, '1311', '1319', '1322', '1323', '1329', '1533', '1699'
, '1839', '1912', '1914', '1915', '2025', '2089', '2090'
, '2266', '2382', '2383', '2385', '2494', '2495', '2551'
, '3048', '3185', '4520', '4521', '4562', '5301', '5302'
, '5303', '5304', '5305', '5306', '5308', '5565', '5586'
, '5686', '6311', '6322', '6323', '6329', '6533', '6839'
, '7025', '8185', '9520', '9521', '9562'
)
AND D.peindica IN ('RSG', 'FUN')
AND concat(A.mo2_cuenta, A.mo2_centro_alta) IN(
	SELECT
	  concat(mo1_cuenta, mo1_centro_alta)
	FROM MOV
	GROUP BY
	  mo1_centro_alta
	, mo1_cuenta
	HAVING SUM(MOV.IMP_TOT) <= -840000
)
;




Archivo 3/3
------------

WITH 
------------  Paquete Movimientos -----------------------------------------/
MOV AS (
    SELECT
      A.mo1_centro_alta
    , A.mo1_cuenta
    , A.mo1_divisa
    , CASE
        WHEN A.mo1_divisa = 'USD'
        THEN SUM(A.mo1_importe * 69.62)
        ELSE SUM(A.mo1_importe)
    END AS IMP_TOT
    ---------------- Movimientos -----------------------------------------/
    FROM bi_corp_staging.malbgc_bgdtmo1 A
    WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
    AND A.mo1_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
    AND A.mo1_codigo IN(
      '0301', '0302', '0303', '0304', '0305', '0306', '0308'
    , '0310', '0415', '0471', '0565', '0686', '0690', '0698'
    , '1311', '1319', '1322', '1323', '1329', '1533', '1699'
    , '1839', '1912', '1914', '1915', '2025', '2089', '2090'
    , '2266', '2382', '2383', '2385', '2494', '2495', '2551'
    , '3048', '3185', '4520', '4521', '4562', '5301', '5302'
    , '5303', '5304', '5305', '5306', '5308', '5565', '5586'
    , '5686', '6311', '6322', '6323', '6329', '6533', '6839'
    , '7025', '8185', '9520', '9521', '9562'
    )
    GROUP BY 
      A.mo1_centro_alta
    , A.mo1_cuenta
    , A.mo1_divisa
    ----------------------------------------------------------------------/
    -----------------   UNION   MO2  -------------------------------------/
    UNION
    ----------------------------------------------------------------------/
    SELECT
      A.mo2_centro_alta
    , A.mo2_cuenta
    , A.mo2_divisa
    , CASE
        WHEN A.mo2_divisa = 'USD'
        THEN SUM(A.mo2_importe * 69.62)
        ELSE SUM(A.mo2_importe)
    END AS IMP_TOT
    ---------------- Movimientos -----------------------------------------/
    FROM bi_corp_staging.malbgc_bgdtmo2 A
    WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
    AND A.mo2_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
    AND A.mo2_codigo IN(
      '0301', '0302', '0303', '0304', '0305', '0306', '0308'
    , '0310', '0415', '0471', '0565', '0686', '0690', '0698'
    , '1311', '1319', '1322', '1323', '1329', '1533', '1699'
    , '1839', '1912', '1914', '1915', '2025', '2089', '2090'
    , '2266', '2382', '2383', '2385', '2494', '2495', '2551'
    , '3048', '3185', '4520', '4521', '4562', '5301', '5302'
    , '5303', '5304', '5305', '5306', '5308', '5565', '5586'
    , '5686', '6311', '6322', '6323', '6329', '6533', '6839'
    , '7025', '8185', '9520', '9521', '9562'
    )
    GROUP BY 
      A.mo2_centro_alta
    , A.mo2_cuenta
    , A.mo2_divisa
    ----------------------------------------------------------------------/
    -----------------   UNION   MOB  -------------------------------------/
    UNION
    ----------------------------------------------------------------------/
    SELECT
      A.mob_centro_alta
    , A.mob_cuenta
    , A.mob_divisa
    , CASE
        WHEN A.mob_divisa = 'USD'
        THEN SUM(A.mob_importe * 69.62)
        ELSE SUM(A.mob_importe)
    END AS IMP_TOT
    ---------------- Movimientos -----------------------------------------/
    FROM bi_corp_staging.malbgc_bgdtmob A
    WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
    AND A.mob_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
    AND A.mob_codigo IN(
      '0301', '0302', '0303', '0304', '0305', '0306', '0308'
    , '0310', '0415', '0471', '0565', '0686', '0690', '0698'
    , '1311', '1319', '1322', '1323', '1329', '1533', '1699'
    , '1839', '1912', '1914', '1915', '2025', '2089', '2090'
    , '2266', '2382', '2383', '2385', '2494', '2495', '2551'
    , '3048', '3185', '4520', '4521', '4562', '5301', '5302'
    , '5303', '5304', '5305', '5306', '5308', '5565', '5586'
    , '5686', '6311', '6322', '6323', '6329', '6533', '6839'
    , '7025', '8185', '9520', '9521', '9562'
    )
    GROUP BY 
      A.mob_centro_alta
    , A.mob_cuenta
    , A.mob_divisa
)
--//////////////////////////////////////////////////////////////////////--
--///////////////////   MAIN                     ///////////////////////--
--//////////////////////////////////////////////////////////////////////--
SELECT 
  B.penumper AS 'NUP'
, E.petipper AS 'Tipo_Persona'
, C.pesegcla AS 'Segmento'
, C.pesegcal AS 'Subsegmento'
, D.pevalind AS 'Riesgo'
, D.peindica AS 'Indicador'
, A.mob_cuenta
, A.mob_producto
, A.mob_subprodu
, A.mob_codigo
, A.mob_fecha_valor
, A.mob_divisa
, A.mob_importe
---------------- Movimientos -----------------------------------------/
FROM bi_corp_staging.malbgc_bgdtmob A
---------------- NUP -------------------------------------------------/
LEFT JOIN bi_corp_staging.malpe_pedt008 B
 ON SUBSTRING(A.mob_cuenta,4,12) = SUBSTRING(B.penumcon,4,12)
AND A.mob_centro_alta = B.pecodofi
AND A.mob_producto = B.pecodpro
AND A.mob_subprodu = B.pecodsub
---------------- Segmento --------------------------------------------/
LEFT JOIN bi_corp_staging.malpe_pedt030 C
 ON C.partition_date = B.partition_date
AND C.penumper = B.penumper
 ---------------- Riesgo ---------------------------------------------/
LEFT JOIN bi_corp_staging.malpe_pedt052 D
 ON D.partition_date = B.partition_date
AND D.penumper = B.penumper
 ---------------- Tipo Pers ------------------------------------------/
LEFT JOIN bi_corp_staging.malpe_pedt001 E
 ON E.partition_date = B.partition_date
AND E.penumper = B.penumper
 ---------------------------------------------------------------------/
WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
AND B.partition_date = '2020-05-29'
AND B.peordpar = '001'
AND A.mob_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
AND A.mob_codigo IN(
  '0301', '0302', '0303', '0304', '0305', '0306', '0308'
, '0310', '0415', '0471', '0565', '0686', '0690', '0698'
, '1311', '1319', '1322', '1323', '1329', '1533', '1699'
, '1839', '1912', '1914', '1915', '2025', '2089', '2090'
, '2266', '2382', '2383', '2385', '2494', '2495', '2551'
, '3048', '3185', '4520', '4521', '4562', '5301', '5302'
, '5303', '5304', '5305', '5306', '5308', '5565', '5586'
, '5686', '6311', '6322', '6323', '6329', '6533', '6839'
, '7025', '8185', '9520', '9521', '9562'
)
AND D.peindica IN ('RSG', 'FUN')
AND concat(A.mob_cuenta, A.mob_centro_alta) IN(
	SELECT
	  concat(mo1_cuenta, mo1_centro_alta)
	FROM MOV
	GROUP BY
	  mo1_centro_alta
	, mo1_cuenta
	HAVING SUM(MOV.IMP_TOT) <= -840000
)
;


----------------------------

Alerta N1. Fraccionamiento

Archivo 1/3
------------

WITH 
------------  Paquete Movimientos -----------------------------------------/
MOV AS (
    SELECT
      A.mo1_centro_alta
    , A.mo1_cuenta
    , A.mo1_divisa
    , CASE
        WHEN A.mo1_divisa = 'USD'
        THEN SUM(A.mo1_importe * 69.62)
        ELSE SUM(A.mo1_importe)
    END AS IMP_TOT
    , count(*) AS CANT
    ---------------- Movimientos -----------------------------------------/
    FROM bi_corp_staging.malbgc_bgdtmo1 A
    WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
    AND A.mo1_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
    AND A.mo1_codigo IN(
      '0032', '0037', '0038', '0039', '0047', '0050', '0081', '0082'
    , '0281', '0469', '0799', '0800', '1293', '1294', '1295', '1296'
    , '1532', '1700', '1913', '1916', '1937', '1938', '2029', '2030'
    , '2077', '2079', '2258', '2260', '2309', '3063', '3187', '3188'
    , '4522', '4523', '4746', '4747', '4949', '5003', '5032', '5037'
    , '5038', '5039', '5047', '5050', '5081', '5082', '5280', '5281'
    , '5469', '5589', '5658', '5799', '5800', '6293', '6294', '6295'
    , '6296', '6320', '6532', '6700', '6911', '6913', '6916', '6937'
    , '6938', '7029', '7030', '7258', '7260', '7309', '8063', '8187'
    , '8188', '9522', '9523', '9746', '9747'
    )
    GROUP BY 
      A.mo1_centro_alta
    , A.mo1_cuenta
    , A.mo1_divisa
    ----------------------------------------------------------------------/
    -----------------   UNION   MO2  -------------------------------------/
    UNION
    ----------------------------------------------------------------------/
    SELECT
      A.mo2_centro_alta
    , A.mo2_cuenta
    , A.mo2_divisa
    , CASE
        WHEN A.mo2_divisa = 'USD'
        THEN SUM(A.mo2_importe * 69.62)
        ELSE SUM(A.mo2_importe)
    END AS IMP_TOT
    , count(*) AS CANT
    ---------------- Movimientos -----------------------------------------/
    FROM bi_corp_staging.malbgc_bgdtmo2 A
    WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
    AND A.mo2_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
    AND A.mo2_codigo IN(
      '0032', '0037', '0038', '0039', '0047', '0050', '0081', '0082'
    , '0281', '0469', '0799', '0800', '1293', '1294', '1295', '1296'
    , '1532', '1700', '1913', '1916', '1937', '1938', '2029', '2030'
    , '2077', '2079', '2258', '2260', '2309', '3063', '3187', '3188'
    , '4522', '4523', '4746', '4747', '4949', '5003', '5032', '5037'
    , '5038', '5039', '5047', '5050', '5081', '5082', '5280', '5281'
    , '5469', '5589', '5658', '5799', '5800', '6293', '6294', '6295'
    , '6296', '6320', '6532', '6700', '6911', '6913', '6916', '6937'
    , '6938', '7029', '7030', '7258', '7260', '7309', '8063', '8187'
    , '8188', '9522', '9523', '9746', '9747'
    )
    GROUP BY 
      A.mo2_centro_alta
    , A.mo2_cuenta
    , A.mo2_divisa
    ----------------------------------------------------------------------/
    -----------------   UNION   MOB  -------------------------------------/
    UNION
    ----------------------------------------------------------------------/
    SELECT
      A.mob_centro_alta
    , A.mob_cuenta
    , A.mob_divisa
    , CASE
        WHEN A.mob_divisa = 'USD'
        THEN SUM(A.mob_importe * 69.62)
        ELSE SUM(A.mob_importe)
    END AS IMP_TOT
    , count(*) AS CANT
    ---------------- Movimientos -----------------------------------------/
    FROM bi_corp_staging.malbgc_bgdtmob A
    WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
    AND A.mob_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
    AND A.mob_codigo IN(
      '0032', '0037', '0038', '0039', '0047', '0050', '0081', '0082'
    , '0281', '0469', '0799', '0800', '1293', '1294', '1295', '1296'
    , '1532', '1700', '1913', '1916', '1937', '1938', '2029', '2030'
    , '2077', '2079', '2258', '2260', '2309', '3063', '3187', '3188'
    , '4522', '4523', '4746', '4747', '4949', '5003', '5032', '5037'
    , '5038', '5039', '5047', '5050', '5081', '5082', '5280', '5281'
    , '5469', '5589', '5658', '5799', '5800', '6293', '6294', '6295'
    , '6296', '6320', '6532', '6700', '6911', '6913', '6916', '6937'
    , '6938', '7029', '7030', '7258', '7260', '7309', '8063', '8187'
    , '8188', '9522', '9523', '9746', '9747'
    )
    GROUP BY 
      A.mob_centro_alta
    , A.mob_cuenta
    , A.mob_divisa
)
--//////////////////////////////////////////////////////////////////////--
--///////////////////   MAIN                     ///////////////////////--
--//////////////////////////////////////////////////////////////////////--
SELECT 
  B.penumper AS 'NUP'
, E.petipper AS 'Tipo_Persona'
, C.pesegcla AS 'Segmento'
, C.pesegcal AS 'Subsegmento'
, D.pevalind AS 'Riesgo'
, D.peindica AS 'Indicador'
, A.mo1_cuenta
, A.mo1_producto
, A.mo1_subprodu
, A.mo1_codigo
, A.mo1_fecha_valor
, A.mo1_divisa
, A.mo1_importe
---------------- Movimientos -----------------------------------------/
FROM bi_corp_staging.malbgc_bgdtmo1 A
---------------- NUP -------------------------------------------------/
LEFT JOIN bi_corp_staging.malpe_pedt008 B
 ON SUBSTRING(A.mo1_cuenta,4,12) = SUBSTRING(B.penumcon,4,12)
AND A.mo1_centro_alta = B.pecodofi
AND A.mo1_producto = B.pecodpro
AND A.mo1_subprodu = B.pecodsub
---------------- Segmento --------------------------------------------/
LEFT JOIN bi_corp_staging.malpe_pedt030 C
 ON C.partition_date = B.partition_date
AND C.penumper = B.penumper
 ---------------- Riesgo ---------------------------------------------/
LEFT JOIN bi_corp_staging.malpe_pedt052 D
 ON D.partition_date = B.partition_date
AND D.penumper = B.penumper
 ---------------- Tipo Pers ------------------------------------------/
LEFT JOIN bi_corp_staging.malpe_pedt001 E
 ON E.partition_date = B.partition_date
AND E.penumper = B.penumper
 ---------------------------------------------------------------------/
WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
AND B.partition_date = '2020-05-29'
AND B.peordpar = '001'
AND A.mo1_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
AND A.mo1_codigo IN(
  '0032', '0037', '0038', '0039', '0047', '0050', '0081', '0082'
, '0281', '0469', '0799', '0800', '1293', '1294', '1295', '1296'
, '1532', '1700', '1913', '1916', '1937', '1938', '2029', '2030'
, '2077', '2079', '2258', '2260', '2309', '3063', '3187', '3188'
, '4522', '4523', '4746', '4747', '4949', '5003', '5032', '5037'
, '5038', '5039', '5047', '5050', '5081', '5082', '5280', '5281'
, '5469', '5589', '5658', '5799', '5800', '6293', '6294', '6295'
, '6296', '6320', '6532', '6700', '6911', '6913', '6916', '6937'
, '6938', '7029', '7030', '7258', '7260', '7309', '8063', '8187'
, '8188', '9522', '9523', '9746', '9747'
)
AND D.peindica IN ('RSG', 'FUN')
AND concat(A.mo1_cuenta, A.mo1_centro_alta) IN(
	SELECT
	  concat(mo1_cuenta, mo1_centro_alta)
	FROM MOV
	GROUP BY
	  mo1_centro_alta
	, mo1_cuenta
	HAVING SUM(MOV.IMP_TOT) >= 200000
       AND SUM(CANT) >= 75
)
;




Archivo 2/3
------------

WITH 
------------  Paquete Movimientos -----------------------------------------/
MOV AS (
    SELECT
      A.mo1_centro_alta
    , A.mo1_cuenta
    , A.mo1_divisa
    , CASE
        WHEN A.mo1_divisa = 'USD'
        THEN SUM(A.mo1_importe * 69.62)
        ELSE SUM(A.mo1_importe)
    END AS IMP_TOT
    , count(*) AS CANT
    ---------------- Movimientos -----------------------------------------/
    FROM bi_corp_staging.malbgc_bgdtmo1 A
    WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
    AND A.mo1_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
    AND A.mo1_codigo IN(
      '0032', '0037', '0038', '0039', '0047', '0050', '0081', '0082'
    , '0281', '0469', '0799', '0800', '1293', '1294', '1295', '1296'
    , '1532', '1700', '1913', '1916', '1937', '1938', '2029', '2030'
    , '2077', '2079', '2258', '2260', '2309', '3063', '3187', '3188'
    , '4522', '4523', '4746', '4747', '4949', '5003', '5032', '5037'
    , '5038', '5039', '5047', '5050', '5081', '5082', '5280', '5281'
    , '5469', '5589', '5658', '5799', '5800', '6293', '6294', '6295'
    , '6296', '6320', '6532', '6700', '6911', '6913', '6916', '6937'
    , '6938', '7029', '7030', '7258', '7260', '7309', '8063', '8187'
    , '8188', '9522', '9523', '9746', '9747'
    )
    GROUP BY 
      A.mo1_centro_alta
    , A.mo1_cuenta
    , A.mo1_divisa
    ----------------------------------------------------------------------/
    -----------------   UNION   MO2  -------------------------------------/
    UNION
    ----------------------------------------------------------------------/
    SELECT
      A.mo2_centro_alta
    , A.mo2_cuenta
    , A.mo2_divisa
    , CASE
        WHEN A.mo2_divisa = 'USD'
        THEN SUM(A.mo2_importe * 69.62)
        ELSE SUM(A.mo2_importe)
    END AS IMP_TOT
    , count(*) AS CANT
    ---------------- Movimientos -----------------------------------------/
    FROM bi_corp_staging.malbgc_bgdtmo2 A
    WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
    AND A.mo2_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
    AND A.mo2_codigo IN(
      '0032', '0037', '0038', '0039', '0047', '0050', '0081', '0082'
    , '0281', '0469', '0799', '0800', '1293', '1294', '1295', '1296'
    , '1532', '1700', '1913', '1916', '1937', '1938', '2029', '2030'
    , '2077', '2079', '2258', '2260', '2309', '3063', '3187', '3188'
    , '4522', '4523', '4746', '4747', '4949', '5003', '5032', '5037'
    , '5038', '5039', '5047', '5050', '5081', '5082', '5280', '5281'
    , '5469', '5589', '5658', '5799', '5800', '6293', '6294', '6295'
    , '6296', '6320', '6532', '6700', '6911', '6913', '6916', '6937'
    , '6938', '7029', '7030', '7258', '7260', '7309', '8063', '8187'
    , '8188', '9522', '9523', '9746', '9747'
    )
    GROUP BY 
      A.mo2_centro_alta
    , A.mo2_cuenta
    , A.mo2_divisa
    ----------------------------------------------------------------------/
    -----------------   UNION   MOB  -------------------------------------/
    UNION
    ----------------------------------------------------------------------/
    SELECT
      A.mob_centro_alta
    , A.mob_cuenta
    , A.mob_divisa
    , CASE
        WHEN A.mob_divisa = 'USD'
        THEN SUM(A.mob_importe * 69.62)
        ELSE SUM(A.mob_importe)
    END AS IMP_TOT
    , count(*) AS CANT
    ---------------- Movimientos -----------------------------------------/
    FROM bi_corp_staging.malbgc_bgdtmob A
    WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
    AND A.mob_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
    AND A.mob_codigo IN(
      '0032', '0037', '0038', '0039', '0047', '0050', '0081', '0082'
    , '0281', '0469', '0799', '0800', '1293', '1294', '1295', '1296'
    , '1532', '1700', '1913', '1916', '1937', '1938', '2029', '2030'
    , '2077', '2079', '2258', '2260', '2309', '3063', '3187', '3188'
    , '4522', '4523', '4746', '4747', '4949', '5003', '5032', '5037'
    , '5038', '5039', '5047', '5050', '5081', '5082', '5280', '5281'
    , '5469', '5589', '5658', '5799', '5800', '6293', '6294', '6295'
    , '6296', '6320', '6532', '6700', '6911', '6913', '6916', '6937'
    , '6938', '7029', '7030', '7258', '7260', '7309', '8063', '8187'
    , '8188', '9522', '9523', '9746', '9747'
    )
    GROUP BY 
      A.mob_centro_alta
    , A.mob_cuenta
    , A.mob_divisa
)
--//////////////////////////////////////////////////////////////////////--
--///////////////////   MAIN                     ///////////////////////--
--//////////////////////////////////////////////////////////////////////--
SELECT 
  B.penumper AS 'NUP'
, E.petipper AS 'Tipo_Persona'
, C.pesegcla AS 'Segmento'
, C.pesegcal AS 'Subsegmento'
, D.pevalind AS 'Riesgo'
, D.peindica AS 'Indicador'
, A.mo2_cuenta
, A.mo2_producto
, A.mo2_subprodu
, A.mo2_codigo
, A.mo2_fecha_valor
, A.mo2_divisa
, A.mo2_importe
---------------- Movimientos -----------------------------------------/
FROM bi_corp_staging.malbgc_bgdtmo2 A
---------------- NUP -------------------------------------------------/
LEFT JOIN bi_corp_staging.malpe_pedt008 B
 ON SUBSTRING(A.mo2_cuenta,4,12) = SUBSTRING(B.penumcon,4,12)
AND A.mo2_centro_alta = B.pecodofi
AND A.mo2_producto = B.pecodpro
AND A.mo2_subprodu = B.pecodsub
---------------- Segmento --------------------------------------------/
LEFT JOIN bi_corp_staging.malpe_pedt030 C
 ON C.partition_date = B.partition_date
AND C.penumper = B.penumper
 ---------------- Riesgo ---------------------------------------------/
LEFT JOIN bi_corp_staging.malpe_pedt052 D
 ON D.partition_date = B.partition_date
AND D.penumper = B.penumper
 ---------------- Tipo Pers ------------------------------------------/
LEFT JOIN bi_corp_staging.malpe_pedt001 E
 ON E.partition_date = B.partition_date
AND E.penumper = B.penumper
 ---------------------------------------------------------------------/
WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
AND B.partition_date = '2020-05-29'
AND B.peordpar = '001'
AND A.mo2_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
AND A.mo2_codigo IN(
  '0032', '0037', '0038', '0039', '0047', '0050', '0081', '0082'
, '0281', '0469', '0799', '0800', '1293', '1294', '1295', '1296'
, '1532', '1700', '1913', '1916', '1937', '1938', '2029', '2030'
, '2077', '2079', '2258', '2260', '2309', '3063', '3187', '3188'
, '4522', '4523', '4746', '4747', '4949', '5003', '5032', '5037'
, '5038', '5039', '5047', '5050', '5081', '5082', '5280', '5281'
, '5469', '5589', '5658', '5799', '5800', '6293', '6294', '6295'
, '6296', '6320', '6532', '6700', '6911', '6913', '6916', '6937'
, '6938', '7029', '7030', '7258', '7260', '7309', '8063', '8187'
, '8188', '9522', '9523', '9746', '9747'
)
AND D.peindica IN ('RSG', 'FUN')
AND concat(A.mo2_cuenta, A.mo2_centro_alta) IN(
	SELECT
	  concat(mo1_cuenta, mo1_centro_alta)
	FROM MOV
	GROUP BY
	  mo1_centro_alta
	, mo1_cuenta
	HAVING SUM(MOV.IMP_TOT) >= 200000
       AND SUM(CANT) >= 75
)
;




Archivo 3/3
------------

WITH 
------------  Paquete Movimientos -----------------------------------------/
MOV AS (
    SELECT
      A.mo1_centro_alta
    , A.mo1_cuenta
    , A.mo1_divisa
    , CASE
        WHEN A.mo1_divisa = 'USD'
        THEN SUM(A.mo1_importe * 69.62)
        ELSE SUM(A.mo1_importe)
    END AS IMP_TOT
    , count(*) AS CANT
    ---------------- Movimientos -----------------------------------------/
    FROM bi_corp_staging.malbgc_bgdtmo1 A
    WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
    AND A.mo1_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
    AND A.mo1_codigo IN(
      '0032', '0037', '0038', '0039', '0047', '0050', '0081', '0082'
    , '0281', '0469', '0799', '0800', '1293', '1294', '1295', '1296'
    , '1532', '1700', '1913', '1916', '1937', '1938', '2029', '2030'
    , '2077', '2079', '2258', '2260', '2309', '3063', '3187', '3188'
    , '4522', '4523', '4746', '4747', '4949', '5003', '5032', '5037'
    , '5038', '5039', '5047', '5050', '5081', '5082', '5280', '5281'
    , '5469', '5589', '5658', '5799', '5800', '6293', '6294', '6295'
    , '6296', '6320', '6532', '6700', '6911', '6913', '6916', '6937'
    , '6938', '7029', '7030', '7258', '7260', '7309', '8063', '8187'
    , '8188', '9522', '9523', '9746', '9747'
    )
    GROUP BY 
      A.mo1_centro_alta
    , A.mo1_cuenta
    , A.mo1_divisa
    ----------------------------------------------------------------------/
    -----------------   UNION   MO2  -------------------------------------/
    UNION
    ----------------------------------------------------------------------/
    SELECT
      A.mo2_centro_alta
    , A.mo2_cuenta
    , A.mo2_divisa
    , CASE
        WHEN A.mo2_divisa = 'USD'
        THEN SUM(A.mo2_importe * 69.62)
        ELSE SUM(A.mo2_importe)
    END AS IMP_TOT
    , count(*) AS CANT
    ---------------- Movimientos -----------------------------------------/
    FROM bi_corp_staging.malbgc_bgdtmo2 A
    WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
    AND A.mo2_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
    AND A.mo2_codigo IN(
      '0032', '0037', '0038', '0039', '0047', '0050', '0081', '0082'
    , '0281', '0469', '0799', '0800', '1293', '1294', '1295', '1296'
    , '1532', '1700', '1913', '1916', '1937', '1938', '2029', '2030'
    , '2077', '2079', '2258', '2260', '2309', '3063', '3187', '3188'
    , '4522', '4523', '4746', '4747', '4949', '5003', '5032', '5037'
    , '5038', '5039', '5047', '5050', '5081', '5082', '5280', '5281'
    , '5469', '5589', '5658', '5799', '5800', '6293', '6294', '6295'
    , '6296', '6320', '6532', '6700', '6911', '6913', '6916', '6937'
    , '6938', '7029', '7030', '7258', '7260', '7309', '8063', '8187'
    , '8188', '9522', '9523', '9746', '9747'
    )
    GROUP BY 
      A.mo2_centro_alta
    , A.mo2_cuenta
    , A.mo2_divisa
    ----------------------------------------------------------------------/
    -----------------   UNION   MOB  -------------------------------------/
    UNION
    ----------------------------------------------------------------------/
    SELECT
      A.mob_centro_alta
    , A.mob_cuenta
    , A.mob_divisa
    , CASE
        WHEN A.mob_divisa = 'USD'
        THEN SUM(A.mob_importe * 69.62)
        ELSE SUM(A.mob_importe)
    END AS IMP_TOT
    , count(*) AS CANT
    ---------------- Movimientos -----------------------------------------/
    FROM bi_corp_staging.malbgc_bgdtmob A
    WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
    AND A.mob_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
    AND A.mob_codigo IN(
      '0032', '0037', '0038', '0039', '0047', '0050', '0081', '0082'
    , '0281', '0469', '0799', '0800', '1293', '1294', '1295', '1296'
    , '1532', '1700', '1913', '1916', '1937', '1938', '2029', '2030'
    , '2077', '2079', '2258', '2260', '2309', '3063', '3187', '3188'
    , '4522', '4523', '4746', '4747', '4949', '5003', '5032', '5037'
    , '5038', '5039', '5047', '5050', '5081', '5082', '5280', '5281'
    , '5469', '5589', '5658', '5799', '5800', '6293', '6294', '6295'
    , '6296', '6320', '6532', '6700', '6911', '6913', '6916', '6937'
    , '6938', '7029', '7030', '7258', '7260', '7309', '8063', '8187'
    , '8188', '9522', '9523', '9746', '9747'
    )
    GROUP BY 
      A.mob_centro_alta
    , A.mob_cuenta
    , A.mob_divisa
)
--//////////////////////////////////////////////////////////////////////--
--///////////////////   MAIN                     ///////////////////////--
--//////////////////////////////////////////////////////////////////////--
SELECT 
  B.penumper AS 'NUP'
, E.petipper AS 'Tipo_Persona'
, C.pesegcla AS 'Segmento'
, C.pesegcal AS 'Subsegmento'
, D.pevalind AS 'Riesgo'
, D.peindica AS 'Indicador'
, A.mob_cuenta
, A.mob_producto
, A.mob_subprodu
, A.mob_codigo
, A.mob_fecha_valor
, A.mob_divisa
, A.mob_importe
---------------- Movimientos -----------------------------------------/
FROM bi_corp_staging.malbgc_bgdtmob A
---------------- NUP -------------------------------------------------/
LEFT JOIN bi_corp_staging.malpe_pedt008 B
 ON SUBSTRING(A.mob_cuenta,4,12) = SUBSTRING(B.penumcon,4,12)
AND A.mob_centro_alta = B.pecodofi
AND A.mob_producto = B.pecodpro
AND A.mob_subprodu = B.pecodsub
---------------- Segmento --------------------------------------------/
LEFT JOIN bi_corp_staging.malpe_pedt030 C
 ON C.partition_date = B.partition_date
AND C.penumper = B.penumper
 ---------------- Riesgo ---------------------------------------------/
LEFT JOIN bi_corp_staging.malpe_pedt052 D
 ON D.partition_date = B.partition_date
AND D.penumper = B.penumper
 ---------------- Tipo Pers ------------------------------------------/
LEFT JOIN bi_corp_staging.malpe_pedt001 E
 ON E.partition_date = B.partition_date
AND E.penumper = B.penumper
 ---------------------------------------------------------------------/
WHERE A.partition_date BETWEEN '2020-05-01' AND '2020-06-10'
AND B.partition_date = '2020-05-29'
AND B.peordpar = '001'
AND A.mob_fecha_valor BETWEEN '2020-05-01' AND '2020-05-31'
AND A.mob_codigo IN(
  '0032', '0037', '0038', '0039', '0047', '0050', '0081', '0082'
, '0281', '0469', '0799', '0800', '1293', '1294', '1295', '1296'
, '1532', '1700', '1913', '1916', '1937', '1938', '2029', '2030'
, '2077', '2079', '2258', '2260', '2309', '3063', '3187', '3188'
, '4522', '4523', '4746', '4747', '4949', '5003', '5032', '5037'
, '5038', '5039', '5047', '5050', '5081', '5082', '5280', '5281'
, '5469', '5589', '5658', '5799', '5800', '6293', '6294', '6295'
, '6296', '6320', '6532', '6700', '6911', '6913', '6916', '6937'
, '6938', '7029', '7030', '7258', '7260', '7309', '8063', '8187'
, '8188', '9522', '9523', '9746', '9747'
)
AND D.peindica IN ('RSG', 'FUN')
AND concat(A.mob_cuenta, A.mob_centro_alta) IN(
	SELECT
	  concat(mo1_cuenta, mo1_centro_alta)
	FROM MOV
	GROUP BY
	  mo1_centro_alta
	, mo1_cuenta
	HAVING SUM(MOV.IMP_TOT) >= 200000
       AND SUM(CANT) >= 75
)
;