      *****************************************************************
      * NOMBRE ARCHIVO.......: XFTC310                                *
      * DESCRIPCION..........: MOV. LIQUIDADOS (NI CONSUMOS/ADELANTOS)*
      *                        MOV. REVERSADOS                        *
      * LONGITUD DE REGISTRO.: 150 CARACTERES                         *
      * ORGANIZACION.........:                                        *
      *                                                               *
      * CLAVES                                                        *
      * ------> PRINCIPAL....:                                        *
      * ------> ALTERNATIVA 1:                                        *
      *****************************************************************
      *
       02 310.
         05  ENTIDAD                 PIC 9(03).
         05  COD-SUCU                PIC 9(03).
         05  COD-OPE                 PIC 9(04).
         05  NRO-CTA                 PIC 9(10).
         05  NRO-TARJ                PIC 9(16).
         05  FEC-PRES-AAMMDD          PIC 9(06).
         05  FEC-ORIGEN-AAMMDD       PIC 9(06).
         05  NRO-COMP                PIC 9(08).
         05  MONEDA                  PIC X(03).
         05  IMPORTE                 PIC 9(11)V99.
         05  SIGNO                   PIC X.
         05  GRP-AFI                 PIC 9(04).
         05  CARTERA                 PIC 9(02).
         05  NRO-EST                 PIC 9(10).
         05  ORIGEN                  PIC X(01).
         05  REVERSO                 PIC X(01).
         05  IMPORTE-ARP             PIC 9(11)V99.
         05  SIGNO-ARP               PIC X.
         05  IMPORTE-USD             PIC 9(11)V99.
         05  SIGNO-USD               PIC X.
         05  MK-AGRO                 PIC X.
         05  FILLER                  PIC X(27).
         05  TPO-REG                 PIC X(2).
         05  MK-FINAL                PIC X(01).