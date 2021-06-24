      ******************************************************************00000200
      * ALTAIR - ACTIVO - PRESTAMOS Y AVALES                           *00000300
      * REGISTRO - UGTCNMA                                             *00000400
      * FECHA DE CREACION: 27-DIC-2019                                 *00000500
      * AREA DE DATOS PARA LA TABLA MAESTRA DE PRESTAMOS NIIF          *00000600
      ******************************************************************00001200
      *                     LOG DE MODIFICACIONES                      *00001300
      * -------------------------------------------------------------- *00001400
      * FECHA    | AUTOR     | DESCRIPCI‰N                             *00001500
      * -------------------------------------------------------------- *00001600
      *                                                                *00001700
      ******************************************************************00001800
      *                                                                *00001900
      * CAMPO                      DESCRIPCIÏN                         *00002000
      * -------------------------- ----------------------------------- *00002100
      * ENTIDAD                    ENTIDAD                             *00002400
      * OFICINA                    OFICINA                             *00002300
      * CUENTA                     NßMERO DE CUENTA                    *00002200
      * CODCONLI                   C‰DIGO DE LIQUIDACI‰N               *00002600
      * IMP-COMISION               IMPORTE COMISI‰N DE OTORGAMIENTO    *00003700
      * SALININIIF                 SALDO INICIAL CA≤DA NIIF            *00003700
      * CFTEA-NIIF                 CFTEA NIIF                          *00002610
      * IMPDEV-ACUM                IMPORTE DEVENGADO ACUMULADO         *00002700
      * FEUDEV                     FECHA ßLTIMO DEVENGAMIENTO          *00002710
      * ENTIDAD-UMO                ENTIDAD ULTIMA MODIFICACION         *00006700
      * CENTRO-UMO                 CENTRO ULTIMA MODIFICACION          *00006800
      * USERID-UMO                 USUARIO ULTIMA MODIFICACION         *00006900
      * NETNAME-UMO                TERMINAL ULTIMA MODIFICACION        *00007000
      * TIMEST-UMO                 TIMESTAMP ULTIMA MODIFICACION       *00007100
      ******************************************************************00007300
       02  UGTCNMA.                                                     00007400
         05 NMA-CLAVE.                                                  00007500
            10 NMA-ENTIDAD             PIC X(04).                       00007800
            10 NMA-OFICINA             PIC X(04).                       00007700
            10 NMA-CUENTA              PIC X(12).                       00007600
         05 NMA-DATOS.                                                  00008100
            10 NMA-CODCONLI            PIC X(03).                       00008200
            10 NMA-IMP-COMISION        PIC S9(13)V9(4) COMP-3.          00008200
            10 NMA-SALININIIF          PIC S9(13)V9(4) COMP-3.          00008200
            10 NMA-CFTEA-NIIF          PIC S9(3)V9(6)  COMP-3.          00008200
            10 NMA-IMPDEV-ACUM         PIC S9(13)V9(4) COMP-3.          00008200
            10 NMA-FEUDEV              PIC X(10).                       00008200
            10 NMA-STAMP-UMO.                                           00074600
               15 NMA-ENTIDAD-UMO      PIC X(04).                       00008200
               15 NMA-CENTRO-UMO       PIC X(04).                       00008200
               15 NMA-USERID-UMO       PIC X(08).                       00008200
               15 NMA-NETNAME-UMO      PIC X(08).                       00008200
               15 NMA-TIMEST-UMO       PIC X(26).                       00008200
