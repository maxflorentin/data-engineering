02  BGTCPCO.
    05 PCO-CLAVE.
       10 PCO-PLAN                  PIC X(04).
       10 PCO-CONCEPTO              PIC X(04).
       10 PCO-FECHA-DESDE           PIC X(10).
    05 PCO-DATOS.
       10 PCO-FECHA-HASTA           PIC X(10).
       10 PCO-MVTOS-LIBR-RIOM       PIC S9(05) USAGE COMP-3.
       10 PCO-PERIOD-COM            PIC X(01).
          88 PCO-PERIOD-COM-P                   VALUE 'P'.
          88 PCO-PERIOD-COM-T                   VALUE 'T'.
       10 PCO-PERIOD-COBR           PIC  X(01).
          88 PCO-PERIOD-COBR-D                  VALUE 'D'.
          88 PCO-PERIOD-COBR-W                  VALUE 'W'.
          88 PCO-PERIOD-COBR-Q                  VALUE 'Q'.
          88 PCO-PERIOD-COBR-M                  VALUE 'M'.
          88 PCO-PERIOD-COBR-B                  VALUE 'B'.
          88 PCO-PERIOD-COBR-T                  VALUE 'T'.
          88 PCO-PERIOD-COBR-S                  VALUE 'S'.
          88 PCO-PERIOD-COBR-A                  VALUE 'A'.
          88 PCO-PERIOD-COBR-V                  VALUE 'V'.
          88 PCO-PERIOD-COBR-J                  VALUE 'J'.
          88 PCO-PERIOD-COBR-N                  VALUE ' '.
       10 PCO-DIA-NAT-COBR          PIC S9(02) USAGE COMP-3.
    05 PCO-STAMP.
       10 PCO-ENTIDAD-UMO           PIC X(04).
       10 PCO-CENTRO-UMO            PIC X(04).
       10 PCO-USERID-UMO            PIC X(08).
       10 PCO-NETNAME-UMO           PIC X(08).
       10 PCO-TIMEST-UMO            PIC X(26).