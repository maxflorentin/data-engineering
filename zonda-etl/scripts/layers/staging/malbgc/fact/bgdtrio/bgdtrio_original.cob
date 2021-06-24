02  BGTCRIO.                                  
    05 RIO-CLAVE.                             
       10 RIO-PLAN                  PIC X(04).
       10 RIO-CONCEPTO              PIC X(04).
       10 RIO-TIP-OPERACION         PIC X(01).
          88 RIO-CT-TIP-OPER-E      VALUE 'E'.
          88 RIO-CT-TIP-OPER-C      VALUE 'C'.
       10 RIO-ZONA                  PIC X(01).
          88 RIO-CT-ZONA-I          VALUE 'I'.
          88 RIO-CT-ZONA-E          VALUE 'E'.
       10 RIO-DIVISA                PIC X(03).
       10 RIO-FECHA-DESDE           PIC X(10).
    05 RIO-DATOS.                             
       10 RIO-FECHA-HASTA           PIC X(10).
       10 RIO-COMISION              PIC X(04).
    05 RIO-STAMP.                             
       10 RIO-ENTIDAD-UMO           PIC X(04).
       10 RIO-CENTRO-UMO            PIC X(04).
       10 RIO-USERID-UMO            PIC X(08).
       10 RIO-NETNAME-UMO           PIC X(08).
       10 RIO-TIMEST-UMO            PIC X(26).