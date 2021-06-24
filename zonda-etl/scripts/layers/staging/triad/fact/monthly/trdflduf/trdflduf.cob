            02  DUF.
              10  LENGTH             PIC S9(08) BINARY.
              10  TENANT-ID          PIC S9(08) BINARY.
              10  CUSTOMER-ID        PIC  X(20).
              10  REC-ID             PIC  X(02).
              10  REC-ID-NUM         PIC  9(02).
              10  REC-SEQ-NUM        PIC  9(02).
              05  USER-AREA-BYTES-USED  PIC  9(05).
              05  UA.
                  20  CA-ACCT-SEGMENT OCCURS 16.
                      25  MONTHLY-SEGMENT OCCURS 12.
                        30  MX-CON-DYS-EXCESS PIC S9(3) BINARY.
                        30  PYMTS-RVLNMG    PIC S9(09) BINARY.
                        30  FILLER          PIC X(10).
                  20  DP-ACCT-SEGMENT OCCURS 12.
                      25  MONTHLY-SEGMENT OCCURS 12.
                        30  PYMTS-RVLNMG    PIC S9(09) BINARY.
                        30  FILLER          PIC X(10).
                  20  RV-ACCT-SEGMENT OCCURS 12.
                      25  MONTHLY-SEGMENT OCCURS 12.
                        30  INST-BALANCE    PIC S9(09) BINARY.
                        30  INST-LIMIT      PIC S9(09) BINARY.
                        30  FILLER          PIC X(10).
                  20  BE-ACCT-SEGMENT.
                      25  DATE-OPEN         PIC 9(08).
                      25  DATE-CLOSED       PIC 9(08).
                      25  PROD-TYPE         PIC 9(01).
                      25  RCPT-TYPE         PIC 9(01).
                      25  CURR-BALANCE      PIC S9(09) BINARY.
                      25  START-ARREARS     PIC 9(08).
                      25  AVG-RCPT-TERM     PIC S9(03) BINARY.
                      25  MONTHLY-SEGMENT OCCURS 12.
                        30  BLOCK-CODE      PIC 9(3).
                        30  BALANCE-EOM     PIC S9(09) BINARY.
                        30  LINE-LIMIT      PIC S9(09) BINARY.
                        30  VAL-PYMNT-CUST PIC S9(09) BINARY.
                        30  VAL-RCPT-PAID-BUYER PIC S9(9) BINARY.
                        30  NUM-RCPT-PAID-BUYER PIC S9(3) BINARY.
                        30  VAL-RCPT-RTRND-BUYER PIC S9(9) BINARY.
                        30  NUM-RCPT-RTRND-BUYER PIC S9(3) BINARY.
                        30  NUM-UNPAID      PIC S9(03) BINARY.
                        30  VAL-ARREARS     PIC S9(09) BINARY.
                        30  MAX-CON-DAYS-UNPAID PIC S9(9) BINARY.
                        30  FILLER          PIC X(10).
                  20 MAPA-RIESGO  OCCURS 12.
                        30 MAPA-SITUACION        PIC 9(1).
                  20 SEGMENT OCCURS 12.
                        30  EXIGIBLE-HIST      PIC S9(9) BINARY.
                  20 CA-MORA-TARDIA OCCURS 16.
                        30 IND-MORA-TARDIA    PIC 9(1).
                  20 RV-MORA-TARDIA OCCURS 12.
                        30 IND-MORA-TARDIA    PIC 9(1).
                  20 MG-MORA-TARDIA OCCURS 12.
                        30 IND-MORA-TARDIA    PIC 9(1).
                  20 LN-MORA-TARDIA OCCURS 12.
                        30 IND-MORA-TARDIA PIC 9(1).
                  05  OP-NAME                  PIC  X(62).
                  05  OP-ADDRESS               PIC  X(70).
                  05  OP-CITY                  PIC  X(40).
                  05  OP-ZIP                   PIC  X(09).
                  05  OP-SSN                   PIC  9(09).