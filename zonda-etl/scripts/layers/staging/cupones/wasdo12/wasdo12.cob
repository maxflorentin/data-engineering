          01 WASDO12.
            10 TIPO_CUENTA       PIC  9(01).
            10 NRO_CUENTA        PIC  9(14).
            10 F_PAGO_AA1        PIC  9(02).
            10 F_PAGO_AA2        PIC  9(02).
            10 F_PAGO_MM         PIC  9(02).
            10 F_PAGO_DD         PIC  9(02).
            10 NRO_SEQ           PIC  9(07) COMP-3.
            10 IMPORTE           PIC  9(15)V99 COMP-3.
            10 EMPRESA           PIC  9(05).
            10 SUC_RECAUD        PIC  9(03).
            10 ORIGEN_MOV        PIC  X(04).
            10 MONEDA            PIC  9(02).
            10 NRO_COMPROBANTE   PIC  X(08).
            10 ESTADO            PIC X(01).
            10 ESTADO_FECHA      PIC  9(08).
            10 CARTERA           PIC  9(02).
            10 IDENT_CANAL       PIC X(2).
            10 IDENT_FECHA       PIC 9(7) COMP-3.
            10 IDENT_NRO         PIC 9(9) COMP-3.
            10 NRO_REL_SIMUL     PIC  X(08).
            10 IND_PAGO_EFECTIVO PIC X(01).
            10 IND_BOLETO_ONLINE PIC X(02).
            10 IND_PAGO_BOLETO   PIC X(01).
            10 FILLER            PIC X(58).
