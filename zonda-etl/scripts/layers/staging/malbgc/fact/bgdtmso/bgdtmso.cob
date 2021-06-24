      *************************************************************
      *                                                           *
      * BGTCMSO: TABLA MOVIMIENTOS SOBREGIRADOS                   *
      *                                                           *
      *  CLAVE                                                    *
      *  CCC                                                      *
      *  ENTIDAD            CLAVE DE ENTIDAD                      *
      *  CENTRO-ALTA        CENTRO DE LA CUENTA                   *
      *  CUENTA             NUMERO DE CUENTA                      *
      *  DIVISA             DIVISA DE LA CUENTA                   *
      *                                                           *
      *                   *************************************   *
      *                   ***    DATOS DE LA OPERACION     ****   *
      *                   *************************************   *
      * CODIGO            CODIGO DE LA OPERACIìN                  *
      * FECHA-OPER        FECHA OPERACION                         *
      * FECHA-VTOVIG      VENCIMIENTO DE VIGENCIA                 *
      * IMPORTE-CUOTA     IMPORTE DE LA OPERACIìN                 *
      * IMPORTE-ACUM      IMPORTE ACUM                            *
      * DISPO-AUT         DISPONIBLE ANTES DE LA OPERACION        *
      * DISPO-DES         DISPONIBLE DESPUES DE LA OPERACION      *
      * DISP-TOTAL-ACU    MONTO DE ACUERDO QUE TIENE ASIGNADO     *
      *                                                           *
      *                                                           *
      *                    ***********************************    *
      *                    ***   LOG DE MODIFICACIONES   ****     *
      *                    ***********************************    *
      *                                                           *
      * ENTIDAD-UMO         ENTIDAD ULTIMA MODIFICACION           *
      * CENTRO-UMO          CENTRO ULTIMA MODIFICACION            *
      * USERID-UMO          USUARIO ULTIMA MODIFICACION           *
      * NETNAME-UMO         TERMINAL ULTIMA MODIFICACION          *
      * TIMEST-UMO          FECHA Y HORA DE LA ULTIMA MODIFICACION*
      *                                                           *
      *                                                           *
      *************************************************************
      * LONG. 232 POSICIONES.                                     *
      *************************************************************
      *                                                           *
      *************************************************************
      * MODIFICACION     :                                        *
      * PETICION         :                                        *
      * AUTOR            :                                        *
      * FECHA            :                                        *
      * DESCRIPCION      :                                        *
      *************************************************************
      *
       01  BGTCMSO.
           10 MSO-ENTIDAD             PIC X(04).
           10 MSO-CENTRO-ALTA         PIC X(04).
           10 MSO-CUENTA              PIC X(12).
           10 MSO-DIVISA              PIC X(03).
           10 MSO-CODIGO              PIC X(04).
           10 MSO-FECHA-OPER          PIC X(10).
           10 MSO-FECHA-VTOVIG        PIC X(10).
           10 MSO-IMPORTE-CUOTA       PIC S9(13)V9(4) USAGE COMP-3.
           10 MSO-IMPORTE-SOBREGIRADO PIC S9(13)V9(4) USAGE COMP-3.
           10 MSO-DISPO-AUT           PIC S9(13)V9(4) USAGE COMP-3.
           10 MSO-DISPO-DES           PIC S9(13)V9(4) USAGE COMP-3.
           10 MSO-DISP-TOTAL-ACU      PIC S9(13)V9(4) USAGE COMP-3.
           10 MSO-OBSERVA             PIC X(90).
           10 MSO-ENTIDAD-UMO         PIC X(04).
           10 MSO-CENTRO-UMO          PIC X(04).
           10 MSO-USERID-UMO          PIC X(08).
           10 MSO-NETNAME-UMO         PIC X(08).
           10 MSO-TIMEST-UMO          PIC X(26).