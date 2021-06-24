      ******************************************************************
      *                     TRIAD                                      *
      *    DESCRIPCION: STANDARD REPORT RECORD - HEADER                *
      ******************************************************************
      *    COPY FICO  : TRDRSOCR                                       *
      *    COPY LAKE  : RRHSO                                          *
      *    LONGUITUD  : 237                                            *
      ******************************************************************

           05  RR-HE-CNTL.
               10  RR-HE-LENGTH                    PIC S9(08) BINARY.
                   88  RR-HE-LENGTH-SET            VALUE +149.          T914

           05  RR-HE-HEADER.
               10  RR-HE-SUBJECT-CODE              PIC  X(01).
                   88  RR-HE-SUBJ-HEADER           VALUE 'H'.
               10  RR-HE-TENANT-ID                 PIC S9(08) BINARY.
               10  RR-HE-SPID                      PIC S9(03) BINARY.
               10  RR-HE-PROC-MODE                 PIC  X(10).
                   88 RR-HE-PRODUCTION             VALUE 'P         '.
      ***  NOT USED FOR T9. SET TO BLANK. S/B 9(05).
               10  RR-HE-COMP-EST-EXPT-ID          PIC  X(05).
               10  RR-HE-PROC-DATE-CYMD            PIC  9(08) BINARY.
               10  RR-HE-CUSTOMER-ID               PIC  X(20).
               10  RR-HE-PREV-CUSTOMER-ID          PIC  X(20).
               10  RR-HE-ACCOUNT-ID                PIC  X(20).
               10  RR-HE-PREV-ACCOUNT-ID           PIC  X(20).
               10  RR-HE-CALL-TYPE                 PIC  X(01).
                   88  RR-HE-CALL-OPEN             VALUE LOW-VALUES.
                   88  RR-HE-CALL-CLOSE            VALUE HIGH-VALUES.
                   88  RR-HE-CALL-UNSET            VALUE SPACES.
                   88  RR-HE-COLLECTIONS-CALL VALUE 'C'.
                   88  RR-HE-EVENT-CALL            VALUE 'E'.
                   88  RR-HE-EVENT-COLL-CALL       VALUE 'T'.
                   88  RR-HE-CONF-CALL             VALUE '1' '2' '3'
                                                         '4' '5' '6'
                                                         '7' '8' '9'
                                                         '0' 'H' 'I'
                                                         'J' 'K' 'L'.
                   88  RR-HE-REVW-CALL             VALUE 'R'.
                   88  RR-HE-REVW-COLL-CALL        VALUE 'W'.
                   88  RR-HE-NEW-CALL              VALUE 'N'.
               10  RR-HE-SAMPL-FTR                 PIC  S9(06) BINARY.
               10  RR-HE-CO-PROD-TYPE              PIC   9(01).
               10  RR-HE-VERIFICATION-ID           PIC   X(33).

           05  SO-SCORE-LENGTH.
               10  SO-LENGTH                      PIC S9(08) BINARY.
                   88  SO-LENGTH-VALUE            VALUE +88.            T914
      *----------------------------------------------------------------*
      *    SCORING OUTCOMES                                            *
      *----------------------------------------------------------------*
           05  SO-SCORE-OUTCOMES.
               10  SO-SUBJECT-CODE                PIC  X(01).
                   88  SO-SUBJ-SCOR-OC             VALUE 'S'.
               10  SO-SUBSEGMENT-CODE             PIC  X(01).
                   88  SO-SUBSEG-SCOR-OC          VALUE 'O'.
               10  SO-HD-TENANT-ID                PIC S9(08) BINARY.    T914
               10  SO-HD-SPID                     PIC S9(03) BINARY.    T914
               10  SO-HD-PROC-DATE-CYM            PIC  9(06) BINARY.    T914
               10  SO-SCORE-TYPE                  PIC  9(02) BINARY.
               10  SO-MB-IMPORTED-IND             PIC  9(01).
                   88  SO-MB-IMPORTED             VALUE 1.
                   88  SO-MB-NOT-IMPORTED         VALUE 0.
               10  SO-SCORE-METHOD                PIC  9(01) BINARY.
                   88 SO-CURR                     VALUE 1.
                   88 SO-REV                      VALUE 2.
                   88 SO-LOAN                     VALUE 3.
                   88 SO-MTGE                     VALUE 4.
                   88 SO-DEP                      VALUE 5.
                   88 SO-CUST                     VALUE 6.
               10  SO-SCORE-EXCLUSION-CAT         PIC  X(01).
                   88 SO-EXCLUSION                VALUE 'E'.
                   88 SO-RETAIN                   VALUE 'R'.
                   88 SO-TRACK                    VALUE 'T'.
               10  SO-SCORE-EXCLUSION-IND         PIC S9(04) BINARY.
                   88 SO-SCORE-EXCLUSION          VALUE +01
                                                        THRU +50.
                   88 SO-SCORE-RETAIN             VALUE +51
                                                  THRU +99.
               10  SO-SCRD-ID                     PIC S9(04) BINARY.
               10  SO-RAW-SCORE                   PIC S9(07) BINARY.
               10  SO-ALIGNED-SCORE               PIC S9(07) BINARY.

               10  SO-RISK-FACTOR                 PIC S9(01)V9(07)
                                                             BINARY.
               10  SO-SEARCH-VALUE                OCCURS 3 TIMES
                                         INDEXED BY SO-IDX.
                   15 SO-FILTER-VAL               PIC S9(08)  BINARY.

      ******************************************************************T914
      * FIELDS USED BY SCORE PERFORMANCE REPORTS                        T914
      ******************************************************************T914
               10  SO-HIST-SCORE-HIST-DATA.                             T914
                   15  SO-HIST-SCRD-ID                PIC S9(04) BINARY.T914
                   15  SO-HIST-RAW-SCORE              PIC S9(07) BINARY.T914
                   15  SO-HIST-ALIGNED-SCORE          PIC S9(07) BINARY.T914
                                                                        T914
               10  SO-PERF-DEFN                   PIC X(01).            T914
                   88  SO-PRIMARY                 VALUE 'P'.            T914
                   88  SO-SECONDARY               VALUE 'S'.            T914
               10  SO-PERF-WINDOW                 PIC S9(02)    BINARY. T914
               10  SO-MAX-DELQ                    PIC 9(01).            T914
               10  SO-GOOD-BAD-IND                PIC X(01).            T914
                   88  SO-PERF-GOOD                   VALUE 'G'.        T914
                   88  SO-PERF-BAD                    VALUE 'B'.        T914
                   88  SO-PERF-INDETERMINATE          VALUE 'I'.        T914
               10  SO-ORIG-BAL                    PIC S9(09) BINARY.    T914
               10  SO-AMT-RECOVERED               PIC S9(09) BINARY.    T914
               10  SO-GOOD-BAD-SCHEME                 PIC 9(01).        T914
                   88  SO-GOOD-BAD-DISCRETE           VALUE 0.          T914
                   88  SO-GOOD-BAD-CONTINUOUS         VALUE 1.          T914
                                                                        T914
               10  SO-HD-SAMPL-FTR                  PIC S9(06) BINARY.  T914
               10  SO-HD-PROC-MODE                  PIC X(10).          T914
