           02  REO.
               10  LENGTH                  PIC S9(08) BINARY.
               10  SUBJECT-CODE            PIC X(01).
               10  TENANT-ID               PIC S9(08) BINARY.
               10  SPID                    PIC S9(03) BINARY.
               10  PROC-MODE               PIC X(10).
               10  COMP-EST-EXPT-ID        PIC X(05).
               10  PROC-DATE-CYMD          PIC 9(08) BINARY.
               10  CUSTOMER-ID             PIC X(20).
               10  PREV-CUSTOMER-ID        PIC X(20).
               10  ACCOUNT-ID              PIC X(20).
               10  PREV-ACCOUNT-ID         PIC X(20).
               10  CALL-TYPE               PIC X(01).
               10  SAMPL-FTR               PIC S9(06) BINARY.
               10  CO-PROD-TYPE            PIC 9(01).
               10  VERIFICATION-ID         PIC X(33).
           05  EO-LENGTH                   PIC S9(08) BINARY.
           05  EO-OUTCOMES-DATA.
               10  SUBJECT-CODE            PIC X(01).
               10  SUBSEGMENT-CODE         PIC X(01).
               10  HD-TENANT-ID            PIC S9(08) BINARY.
               10  HD-SPID                 PIC S9(03) BINARY.
               10  HD-PROC-DATE-CYM        PIC 9(06) BINARY.
               10  RDG.
                   15 RDG-LOWLMT           PIC S9(04) BINARY.
                   15 RDG-UPLMT            PIC S9(04) BINARY.
               10  SPID                    PIC S9(03) BINARY.
               10  STGY-ROW                PIC S9(08) BINARY.
               10  PROCESS-SWITCH          PIC X(04).
               10  RECLASS-TYPE            PIC 9(01).
               10  SCEN-ID                 PIC S9(04) BINARY.
               10  SCEN-SEG-ID             PIC X(05).
               10  FILLER                  PIC X(05).
               10  ACTION-CTR              PIC S9(2)  BINARY.
               10  TIMED-ACTION-1          PIC X(06).
               10  TIMED-ACTION-2          PIC X(04).
               10  TIMED-ACTION-3          PIC X(05).
               10  TIMED-ACTION-4          PIC X(05).
               10  TIMED-ACTION-5          PIC X(05).
               10  FIXED-ACTION-1          PIC X(05).
               10  FIXED-ACTION-2          PIC X(05).
               10  RECLASS-ATTEMPTED       PIC X(01).
               10  DAYS-SNC-SCEN-ASSIGNED  PIC S9(02) BINARY.
               10  RECLASS-REASON          PIC S9(02) BINARY.
               10  STGY-ID                 PIC S9(03) BINARY.
               10  DAYS-IN-COLL            PIC S9(03) BINARY.
               10  COLL-BALANCE            PIC S9(09) BINARY.
               10  INIT-COLL-BALANCE       PIC S9(09) BINARY.
               10  PROMISE-KEPT-IND        PIC 9(01).
               10  PROMISE-BROKEN-IND      PIC 9(01).
               10  BALANCE-COLLECTED       PIC S9(09) BINARY.
               10  FILTER-KEYS             OCCURS 3 TIMES
                                         INDEXED BY EO-IDX.
                    15 FILTER-VAL          PIC S9(08) BINARY.
               10  CU-SCORE-DATA.
                   15  CU-RAW-SCORE        PIC S9(07) BINARY.
                   15  CU-ALIGNED-SCORE    PIC S9(07) BINARY.
                   15  RISK-FACTOR         PIC S9(01)V9(07) BINARY.
               10 HD-SAMPL-FTR             PIC S9(06) BINARY.
               10 HD-PROC-MODE             PIC X(10).
               10 HD-CALL-TYPE             PIC X(01).           

