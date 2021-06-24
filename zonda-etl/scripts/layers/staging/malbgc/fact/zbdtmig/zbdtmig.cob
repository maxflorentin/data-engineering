      ******************************************************************
      * DCLGEN TABLE(AALT3.ZBDTMIG)                                    *
      *        LIBRARY(AMT3.ID7124.CAP.JM.DCA(ZBGTMIG))                *
      *        ACTION(REPLACE)                                         *
      *        LANGUAGE(COBOL)                                         *
      *        NAMES(MIG-)                                             *
      *        STRUCTURE(ZBGTMIG)                                      *
      *        QUOTE                                                   *
      *        COLSUFFIX(YES)                                          *
      *        INDVAR(YES)                                             *
      * ... IS THE DCLGEN COMMAND THAT MADE THE FOLLOWING STATEMENTS   *
      ******************************************************************

      ******************************************************************
      * COBOL DECLARATION FOR TABLE AALT3.ZBDTMIG                      *
      ******************************************************************
       01  MIG.
      *                       OLD_ENTIDAD
           10 OLD-ENTIDAD      PIC X(4).
      *                       OLD_CENT_ALTA
           10 OLD-CENT-ALTA    PIC X(4).
      *                       OLD_CUENTA
           10 OLD-CUENTA       PIC X(12).
      *                       OLD_DIVISA
           10 OLD-DIVISA       PIC X(3).
      *                       NEW_ENTIDAD
           10 NEW-ENTIDAD      PIC X(4).
      *                       NEW_CENT_ALTA
           10 NEW-CENT-ALTA    PIC X(4).
      *                       NEW_CUENTA
           10 NEW-CUENTA       PIC X(12).
      *                       NEW_DIVISA
           10 NEW-DIVISA       PIC X(3).
      *                       OLD_FECH_BAJA
           10 OLD-FECH-BAJA    PIC X(10).
      *                       OLD_HORA_BAJA
           10 OLD-HORA-BAJA    PIC X(8).
      *                       OLD_MOTIV_BAJA
           10 OLD-MOTIV-BAJA   PIC X(2).
      *                       OLD_MOTIV_MIGR
           10 OLD-MOTIV-MIGR   PIC X(4).
      *                       OLD_INDESTA
           10 OLD-INDESTA      PIC X(1).
      *                       NEW_INDESTA
           10 NEW-INDESTA      PIC X(1).
      *                       ENTIDAD_UMO
           10 ENTIDAD-UMO      PIC X(4).
      *                       CENTR_UMO
           10 CENTR-UMO        PIC X(4).
      *                       USERID_UMO
           10 USERID-UMO       PIC X(8).
      *                       NETNAME_UMO
           10 NETNAME-UMO      PIC X(8).
      *                       TIMEST_UMO
           10 TIMEST-UMO       PIC X(26).
