      ******************************************************************
      * DCLGEN TABLE(BGDTOBS)                                          *
      *        LIBRARY(OPI.CPF.DCLGEN(BGGTOBS))                        *
      *        ACTION(REPLACE)                                         *
      *        APOST                                                   *
      * ... IS THE DCLGEN COMMAND THAT MADE THE FOLLOWING STATEMENTS   *
      ******************************************************************

      ******************************************************************
      * COBOL DECLARATION FOR TABLE OPIGD1.BGDTOBS                     *
      ******************************************************************
       01  OBS.
           10  ENTIDAD                PIC X(04).
           10  CENTRO-ALTA            PIC X(04).
           10  CUENTA                 PIC X(12).
           10  DIVISA                 PIC X(03).
           10  NUMER-MOV              PIC S9(9)V USAGE COMP-3.
           10  OBSERVACIONES          PIC X(90).
           10  ENTIDAD-UMO            PIC X(4).
           10  CENTRO-UMO             PIC X(4).
           10  USERID-UMO             PIC X(8).
           10  CAJERO-UMO             PIC X(1).
           10  NETNAME-UMO            PIC X(8).
           10  TIMEST-UMO             PIC X(26).
      ******************************************************************
      * THE NUMBER OF COLUMNS DESCRIBED BY THIS DECLARATION IS 12      *
      ******************************************************************
