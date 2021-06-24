      ******************************************************************00000200
      * ALTAIR - ACTIVO - PRESTAMOS Y AVALES                           *00000300
      * REGISTRO - UGTCNCC                                             *00000400
      * FECHA DE CREACION: 27-DIC-2019                                 *00000500
      * AREA DE DATOS CON LA SIMULACI‰N FINANCIERA DE CARA AL CLIENTE  *00000600
      * AL MOMENTO DE LA FORMALIZACI‰N - NIIF                          *00000600
      ******************************************************************00001200
      *                     LOG DE MODIFICACIONES                      *00001300
      * -------------------------------------------------------------- *00001400
      * FECHA    | AUTOR     | DESCRIPCI‰N                             *00001500
      * -------------------------------------------------------------- *00001600
      *                                                                *00001700
      ******************************************************************00001800
      *                                                                *00001900
      * CAMPO                      DESCRIPCI‰N                         *00002000
      * -------------------------- ----------------------------------- *00002100
      * ENTIDAD                    ENTIDAD                             *00002400
      * OFICINA                    OFICINA                             *00002300
      * CUENTA                     NßMERO DE CUENTA                    *00002200
      * FELIQ                      FECHA LIQUIDACI‰N DEL RECIBO        *00002200
      * NUMREC                     NßMERO DE RECIBO                    *00002200
      * FEFIDEV                    FECHA DE FIN DEVENGO TRAMO          *00002200
      * CAPINIRE                   IMP CAPITAL RECIBO A FORMALIZACI‰N  *00003700
      * INTINIRE                   IMP INTERàS RECIBO A FORMALIZACI‰N  *00003700
      * SALTEOR                    IMP SALDO CAPITAL POST FACTURACI‰N  *00002610
      * IMP-INT-DEV                IMP INTERàS A DEVENGAR POR TRAMO    *00002700
      * ENTIDAD-UMO                ENTIDAD ULTIMA MODIFICACION         *00006700
      * CENTRO-UMO                 CENTRO ULTIMA MODIFICACION          *00006800
      * USERID-UMO                 USUARIO ULTIMA MODIFICACION         *00006900
      * NETNAME-UMO                TERMINAL ULTIMA MODIFICACION        *00007000
      * TIMEST-UMO                 TIMESTAMP ULTIMA MODIFICACION       *00007100
      ******************************************************************00007300
       02  UGTCNCC.                                                     00007400
         05 NCC-CLAVE.                                                  00007500
            10 NCC-ENTIDAD             PIC X(04).                       00007800
            10 NCC-OFICINA             PIC X(04).                       00007800
            10 NCC-CUENTA              PIC X(12).                       00007800
            10 NCC-FELIQ               PIC X(10).                       00007800
            10 NCC-NUMREC              PIC S9(5)V USAGE COMP-3.         00007700
            10 NCC-FEFIDEV             PIC X(10).                       00007600
         05 NCC-DATOS.                                                  00008100
            10 NCC-CAPINIRE            PIC S9(13)V9(4) COMP-3.          00008200
            10 NCC-INTINIRE            PIC S9(13)V9(4) COMP-3.          00008200
            10 NCC-SALTEOR             PIC S9(13)V9(4) COMP-3.          00008200
            10 NCC-IMP-INT-DEV         PIC S9(13)V9(4) COMP-3.          00008200
            10 NCC-STAMP-UMO.                                           00074600
               15 NCC-ENTIDAD-UMO      PIC X(04).                       00008200
               15 NCC-CENTRO-UMO       PIC X(04).                       00008200
               15 NCC-USERID-UMO       PIC X(08).                       00008200
               15 NCC-NETNAME-UMO      PIC X(08).                       00008200
               15 NCC-TIMEST-UMO       PIC X(26).                       00008200
