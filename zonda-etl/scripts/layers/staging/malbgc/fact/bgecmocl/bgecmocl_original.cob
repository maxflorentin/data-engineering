02  BGECMOCL.

    05  MOCL-CCC.
        10  MOCL-ENTIDAD            PIC X(4).
        10  MOCL-CENTRO-ALTA        PIC X(4).
        10  MOCL-CUENTA             PIC X(12).

    05  MOCL-DIVISA                 PIC X(3).
    05  MOCL-CCC-AGRUPADA.
        10  MOCL-ENTIDAD-AGRUPADA   PIC X(4).
        10  MOCL-CENTRO-ALTA-AGRUPADA
                                    PIC X(4).
        10  MOCL-CUENTA-AGRUPADA    PIC X(12).

    05  MOCL-DIVISA-AGRUPADA        PIC X(3).
    05  MOCL-CPC.
        10  MOCL-ENTIDAD-PAQ        PIC X(4).
        10  MOCL-CENTRO-ALTA-PAQ    PIC X(4).
        10  MOCL-PAQUETE            PIC X(12).

    05  MOCL-PIVOTE                 PIC X(1).
    05  MOCL-PLAN                   PIC X(4).
    05  MOCL-IND-PAQUETE            PIC X(1).
    05  MOCL-DIVISA-BIS             PIC X(3).
    05  MOCL-CONCEPTO               PIC X(4).
    05  MOCL-COMISION               PIC X(4).
    05  MOCL-IND-GRUPO              PIC X(1).
        88  MOCL-CANJE-24HS                 VALUE '1'.
        88  MOCL-CANJE-48-72HS              VALUE '2'.
        88  MOCL-DEPO-CHEQ                  VALUE '3'.
        88  MOCL-DEPO-EFEC                  VALUE '4'.
        88  MOCL-OTROS-DEBI                 VALUE '5'.
        88  MOCL-OTROS-CRED                 VALUE '6'.
        88  MOCL-DEPO-CHEQ-24HS             VALUE '7'.
    05  MOCL-CODIGO                 PIC X(4).
    05  MOCL-IMPORTE                PIC S9(13)V9(2) COMP-3.
    05  MOCL-FECHA-OPER             PIC X(10).
    05  MOCL-CHEQUE                 PIC S9(9) COMP-3.
    05  MOCL-MINIBANCO              PIC X(2).
    05  MOCL-CANAL                  PIC X(2).
    05  MOCL-ZONA                   PIC X(1).
    05  MOCL-OPERACION              PIC X(1).
    05  MOCL-ACTUA.
        10  MOCL-ENTIDAD-UMO        PIC X(4).
        10  MOCL-CENTRO-UMO         PIC X(4).
        10  MOCL-USERID-UMO         PIC X(8).
        10  MOCL-CAJERO-UMO         PIC X(1).
        10  MOCL-NETNAME-UMO        PIC X(8).
        10  MOCL-TIMEST-UMO         PIC X(26).

    05  MOCL-FILLER                 PIC X(32).

