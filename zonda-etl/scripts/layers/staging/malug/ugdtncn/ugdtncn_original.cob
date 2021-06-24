      ******************************************************************00000200
      * ALTAIR - ACTIVO - PRESTAMOS Y AVALES                           *00000300
      * REGISTRO - UGTCNCN                                             *00000400
      * FECHA DE CREACION: 27-DIC-2019                                 *00000500
      * AREA DE DATOS CON LA CA�DA DE CUOTAS NIIF CON EL DETALLE DEL   *00000600
      * INTER�S A DEVENGAR POR CADA TRAMO                              *00000600
      ******************************************************************00001200
      *                     LOG DE MODIFICACIONES                      *00001300
      * -------------------------------------------------------------- *00001400
      * FECHA    | AUTOR     | DESCRIPCI�N                             *00001500
      * -------------------------------------------------------------- *00001600
      *                                                                *00001700
      ******************************************************************00001800
      *                                                                *00001900
      * CAMPO                      DESCRIPCI�N                         *00002000
      * -------------------------- ----------------------------------- *00002100
      * ENTIDAD                    ENTIDAD                             *00002400
      * OFICINA                    OFICINA                             *00002300
      * CUENTA                     N�MERO DE CUENTA                    *00002200
      * FELIQ                      FECHA LIQUIDACI�N DEL RECIBO        *00002200
      * NUMREC                     N�MERO DE RECIBO                    *00002200
      * FEFIDEV                    FECHA DEVENGAMIENTO DEL RECIBO      *00002200
      * SDO-NIIF-PREFAC            SALDO TE�RICO PRE FACTURACI�N NIFF  *00003700
      * IMP-INT-NIIF               INTER�S NIIF DEL RECIBO             *00003700
      * SDO-NIIF-POSTFAC           SALDO TE�RICO POST FACTURACI�N NIFF *00002610
      * INT-DEV-NIIF               INTER�S DEVENGO TRAMO NIIF          *00002700
      * INT-DIF-DEV                DIFERENCIA INTER�S A DEVENGAR ENTRE *00002700
      *                            INTER�S DEVENGO TRAMO NIIF E INTER�S*00002700
      *                            DEVENGO CA�DA CLIENTE (UGTCNCC)     *00002700
      * FECODEV                    FECHA DE CONTABILIZACI�N DEL        *00002700
      *                            DEVENGAMIENTO                       *00002700
      * ENTIDAD-UMO                ENTIDAD ULTIMA MODIFICACION         *00006700
      * CENTRO-UMO                 CENTRO ULTIMA MODIFICACION          *00006800
      * USERID-UMO                 USUARIO ULTIMA MODIFICACION         *00006900
      * NETNAME-UMO                TERMINAL ULTIMA MODIFICACION        *00007000
      * TIMEST-UMO                 TIMESTAMP ULTIMA MODIFICACION       *00007100
      ******************************************************************00007300
       02  UGTCNCN.                                                     00007400
         05 NCN-CLAVE.                                                  00007500
            10 NCN-ENTIDAD             PIC X(04).                       00007800
            10 NCN-OFICINA             PIC X(04).                       00007800
            10 NCN-CUENTA              PIC X(12).                       00007800
            10 NCN-FELIQ               PIC X(10).                       00007800
            10 NCN-NUMREC              PIC S9(5)V USAGE COMP-3.         00007700
            10 NCN-FEFIDEV             PIC X(10).                       00007600
         05 NCN-DATOS.                                                  00008100
            10 NCN-SDO-NIIF-PREFAC     PIC S9(13)V9(4) COMP-3.          00008200
            10 NCN-IMP-INT-NIIF        PIC S9(13)V9(4) COMP-3.          00008200
            10 NCN-SDO-NIIF-POSTFAC    PIC S9(13)V9(4) COMP-3.          00008200
            10 NCN-INT-DEV-NIIF        PIC S9(13)V9(4) COMP-3.          00008200
            10 NCN-INT-DIF-DEV         PIC S9(13)V9(4) COMP-3.          00008200
            10 NCN-FECODEV             PIC X(10).                       00008200
            10 NCN-STAMP-UMO.                                           00074600
               15 NCN-ENTIDAD-UMO      PIC X(04).                       00008200
               15 NCN-CENTRO-UMO       PIC X(04).                       00008200
               15 NCN-USERID-UMO       PIC X(08).                       00008200
               15 NCN-NETNAME-UMO      PIC X(08).                       00008200
               15 NCN-TIMEST-UMO       PIC X(26).                       00008200
