           02  RCC.
               10  LENGTH              PIC S9(08) BINARY.
               10  SUBJECT-CODE        PIC X(01).
               10  TENANT-ID           PIC S9(08) BINARY.
               10  SPID                PIC S9(03) BINARY.
               10  PROC-MODE           PIC X(10).
               10  COMP-EST-EXPT-ID    PIC X(05).
               10  PROC-DATE-CYMD      PIC 9(08) BINARY.
               10  CUSTOMER-ID         PIC X(20).
               10  PREV-CUSTOMER-ID    PIC X(20).
               10  ACCOUNT-ID          PIC X(20).
               10  PREV-ACCOUNT-ID     PIC X(20).
               10  CALL-TYPE           PIC X(01).
               10  SAMPL-FTR           PIC S9(06) BINARY.
               10  CO-PROD-TYPE        PIC 9(01).
               10  VERIFICATION-ID     PIC X(33).
               10  LENGTH-1            PIC S9(08) BINARY.
               10  SUBJECT-CODE-1      PIC X(01).
               10  SUBSEGMENT-CODE     PIC X(01).
               10  CC-DGTS-RANGE.
                   15  LOWLMT          PIC S9(04) BINARY.
                   15  UPLMT           PIC S9(04) BINARY.
               10  STGY-TYPE           PIC X(03).
               10  LIMIT-ID            PIC 9(02) BINARY.
               10  LIM-COMP            OCCURS 10 TIMES
                   15 LIMIT-COMPONENT  PIC S9(09) BINARY.
               10  LIM-FCTRS           OCCURS 10 TIMES
                   15 LIMIT-FACTORS    PIC S9(5)V99 BINARY.
               10  FILLER              PIC X(05).
