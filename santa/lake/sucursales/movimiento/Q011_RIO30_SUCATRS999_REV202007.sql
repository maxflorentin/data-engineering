SELECT TX_ID
              --Sentencia original Santander Tecnologia
              --,FECHA
              --PyMO
              ,TO_CHAR(FECHA,'DD/MM/YYYY-hh:mm:ss') FECHAT
              --PyMO
              ,HORA
              ,ORIG_REV
              ,CANAL_ID
              ,MAQ_ID
              ,SUC_NRO
              ,SUC_MAQ
              ,MONEDA
              ,IMPORTE
              ,CANTIDAD_CHEQUES
              ,medio_pago
              ,NUP
              ,CUADRANTE
              ,PESUBSEG
          FROM cnl01.MOVIMIENTO
         WHERE (   (CANAL_ID = 'RIOS' AND TX_ID IN ('003913', '003951', '003954', '013434', '013761', '013762',
'100000','100100','100200','101100','101200','101300','103100','103200','104000','520000','520031','200001','200002',
'200011','200012','200013','200031','200032','200040','960001','960002','960011','180001','180011','940001','940002',
'940011','940012','950001','950002','950011','950012','550000'))
                OR (      CANAL_ID LIKE 'ATM%'
                     AND (   TX_ID LIKE '10%'
                           OR TX_ID LIKE '16%'
                           OR TX_ID LIKE '20%'
                           OR TX_ID LIKE '57%'
                           OR TX_ID LIKE '58%'
                           OR TX_ID LIKE '94%'
                           OR TX_ID LIKE '95%'
                          )
                   )
               )
           --Sentencia original Santander Tecnologia
		   --AND FECHA BETWEEN id_fechaini AND id_fechafin
		   --PyMO
		   AND SUC_MAQ = '432'
		   AND FECHA BETWEEN '01-JUN-2020' AND '30-JUN-2020'
		   --PyMO
         ORDER BY SUC_MAQ, MAQ_ID, FECHA, HORA;