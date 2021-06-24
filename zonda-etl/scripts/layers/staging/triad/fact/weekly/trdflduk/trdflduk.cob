         02  DUK.
             10  LENGTH         PIC S9(08) BINARY.
             10  TENANT-ID      PIC S9(08) BINARY.
             10  CUSTOMER-ID    PIC  X(20).
             10  REC-ID         PIC  X(02).
             10  REC-ID-NUM     PIC  9(02).
             10  REC-SEQ-NUM    PIC  9(02).
             10  USER-KEYS-USED PIC  9(03).
             10  USER-DEFINED-KEY OCCURS 484 TIMES.
                15  USER-DEFINED-KEY-N PIC S9(07) BINARY.
             10  ACCOUNT-ID-SEG-CO PIC  X(20).
