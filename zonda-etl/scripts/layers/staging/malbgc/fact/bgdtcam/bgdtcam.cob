       ******************************************************************
      * DCLGEN TABLE(BGTCCAM)                                          *
      *        LIBRARY(OPI.CPF.DCLGEN(BGTCCAM))                        *
      *        ACTION(REPLACE)                                         *
      *        APOST                                                   *
      * ... IS THE BGTCCAM COMMAND THAT MADE THE FOLLOWING STATEMENTS   *
      ******************************************************************

      ******************************************************************
      * COBOL DECLARATION FOR TABLE OPIGD1.BGTCCAM                     *
      ******************************************************************
       01 CAM.
           10 CAMPANIA                          PIC X(04).
           10 FECHA-DESDE                       PIC X(10).
           10 PRODUCTO                          PIC X(02).
           10 SUBPRODU                          PIC X(04).
           10 FECHA-HASTA                       PIC X(10).
           10 PLAZO                             PIC S9(03) USAGE COMP-3.
           10 FECHA-VTO                         PIC X(10).
           10 TARIFA                            PIC X(04).
           10 PERIOD-LIQ                        PIC X(01).
           10 DIVISA                            PIC X(03).
           10 PLAN                              PIC X(04).
           10 DESCRIPCION                       PIC X(40).
           10 CLASE-LIQ                         PIC X(01).
           10 CLASE-TAF                         PIC X(01).
           10 PERIODO-TAR                       PIC X(01).
           10 INDESTA                           PIC X(01).
           10 FECHA-ESTADO                      PIC X(10).
           10 ENTIDAD-UMO                       PIC X(04).
           10 CENTRO-UMO                        PIC X(04).
           10 USERID-UMO                        PIC X(08).
           10 NETNAME-UMO                       PIC X(08).
           10 TIMEST-UMO                        PIC X(26).
      ******************************************************************
      * THE NUMBER OF COLUMNS DESCRIBED BY THIS DECLARATION IS 12      *
      ******************************************************************.