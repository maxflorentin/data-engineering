      ******************************************************************
      *                                                                *
      * NOMBRE DEL OBJETO:  BGTCRIE                                    *
      *                                                                *
      * DESCRIPCION: TABLA DE RELACION IMPOSICIONES Y MOVIMIENTOS      *
      *     ----------------------------------------------------       *
      *                                                                *
      *                                                                *
      *                   *******-------------------------------****** *
      *                   *******RELACION CCC_IPF/MOVIMIENTO    ****** *
      *                   *******-------------------------------****** *
      * CLAVE                                                          *
      * CCC                                                            *
      * ENTIDAD           CLAVE DE ENTIDAD                             *
      * CENTRO_ALTA       CENTRO DE ALTA                               *
      * CUENTA            NUMERO DE LA CUENTA                          *
      * SECUENCIA         SECUENCIA DE LA IPF                          *
      * SECUENCIA_REN     SECUENCIA DE RENOVACION DEL CERTIFICADO      *
      * NUMER_MOV         NUMERO DE MOVIMIENTO                         *
      * IND_CTA_ASO       INDICADOR DE SI EXISTE CUENTA ASOCIADA       *
      * EXISTE_ASO        <S> CADOR DE SI EXISTE CUENTA ASOCIADA       *
      * NO_EXISTE_ASO     <N>ICADOR DE SI EXISTE CUENTA ASOCIADA       *
      * CODIGO            CODIGO DE LA OPERACION                       *
      * CONCEPTO          CONCEPTO DE LA OPERACION                     *
      * FEC_VALOR         FECHA VALOR                                  *
      * FEC_OPERA         FECHA DE OPERACION                           *
      * IMPORTE           IMPORTE DE LA OPERACION                      *
      * IND_PAGADO        INDICADOR DE PAGADO                          *
      * PAGADO            <S> PAGADO                                   *
      * NO_PAGADO         <N> NO PAGADO                                *
      * IND_ANULADO       INDICADOR DE PAGADO                          *
      * ANULADO           <S> PAGADO                                   *
      * NO_ANULADO        <N> NO PAGADO                                *
      * DIVISA            DIVISA DE LA OPERACION                       *
      * PAGO              FORMA DE PAGO DE LA EMISION DEL CERTIFICADO  *
      * PAGO_CAJA         <E> PAGO EN EFECTIVO                         *
      * PAGO_TELEPRO      <O> PAGO CONTRA CUENTA DE COMPENS. ON_LINE   *
      * PAGO_CTA          <C> PAGO CONTRA CUENTA DE CARGO              *
      * PAGO_CHEQUE       <Q> PAGO CHEQUE                              *
      * PAGO_ACREEDOR     <A> VARIOS ACREEDORES                        *
      * PAGO_CANDEP       <D> CANCELACION DE OTROS DEPOSITOS           *
      * PAGO_SIN_CTA      <S> PAGO SIN CUENTA ASOCIADA                 *
      * PAGO_ENDOSO       <N>-PAGO POR ENDOSO                          *
      * PAGO_FRACCION     <F>-PAGO POR FRACCIONAMIENTO                 *
      * PAGO_CONCENTR     <T>-PAGO POR CONCENTRACION                   *
      * PAGO_CTA_SUC      <R>-PAGO CONTRA CUENTA DE LA SUCURSAL        *
      * CTRO_OPER         CENTRO QUE REALIZA LA OPERACION              *
      * CTRO_ORIGEN       CENTRO ORIGEN DE LA OPERACION                *
      * CTRO_GESTOR       CENTRO GESTOR DE LA IPF                      *
      * IND_CONTAB        INDICADOR DE SI YA ESTA CONTABILIZADO        *
      * CONTAB            <S> CONTABILIZADO                            *
      * NO_CONTAB         <N> NO CONTABILIZADO                         *
      * IND_AOM           INDICADOR DE SI ES ON_LINE O BATCH           *
      * ON_LINE           <O> ON_LINE                                  *
      * BATCH             <B> BATCH                                    *
      * COD_CONTAB        CODIGO DE CONTABILIZACION                    *
      * COD_PROVINC       CODIGO DE PROVINCIA                          *
      * CLAV_INTERFAZ     CLAVE DE INTERFAZ                            *
      * COD_EVENTO        CODIGO DE EVENTO IMPOSITIVO                  *
      *04_01466_0A_I                                                   *
      * TIPOINT           TIPO DE INTERES FIJO A LIQUIDAR.             *
      * INC_PORC_VIG      INCREMENTO PORCENTUAL APLICADO.(SPREAD)      *
      *04_01466_0A_I                                                   *
      *                   *******-------------------------------****** *
      *                   *******  LOG DE MODIFICACIONES        ****** *
      *                   *******-------------------------------****** *
      * STAMP                                                          *
      * ENTIDAD_UMO       ENTIDAD ULTIMA MODIFICACION                  *
      * CENTRO_UMO        CENTRO ULTIMO MODIFICACION                   *
      * USERID_UMO        USUARIO ULTIMA MODIFICACION                  *
      * NETNAME_UMO       TERMINAL ULTIMA MODIFICACION                 *
      * TIMEST_UMO        FECHA Y HORA ULTIMA MODIFICACION             *
      *                                                                *
      ******************************************************************

           02  BGDTRIE.
               10  ENTIDAD             PIC X(4).
               10  CENTRO_ALTA         PIC X(4).
               10  CUENTA              PIC X(12).
               10  SECUENCIA           PIC S9(5)V COMP-3.
               10  SECUENCIA_REN       PIC S9(5)V COMP-3.
               10  NUMER_MOV           PIC S9(9)V COMP-3.
               10  IND_CTA_ASO         PIC X(1).
               10  CODIGO              PIC X(4).
               10  CONCEPTO            PIC X(4).
               10  FEC_VALOR           PIC X(10).
               10  FEC_OPERA           PIC X(10).
               10  IMPORTE             PIC S9(13)V9(2) COMP-3.
               10  IND_PAGADO          PIC X(1).
               10  IND_ANULADO         PIC X(1).
               10  DIVISA              PIC X(3).
               10  PAGO                PIC X(1).
               10  CTRO_OPER           PIC X(4).
               10  CTRO_ORIGEN         PIC X(4).
               10  CTRO_GESTOR         PIC X(4).
               10  IND_CONTAB          PIC X(1).
               10  IND_AOM             PIC X(1).
               10  COD_CONTAB          PIC X(5).
               10  COD_PROVINC         PIC X(2).
               10  CLAV_INTERFAZ       PIC X(3).
               10  COD_EVENTO          PIC X(3).
               10  TIPOINT             PIC S9(03)V9(5) COMP-3.
               10  INC_PORC_VIG        PIC S9(03)V9(5) COMP-3.
               10  ENTIDAD_UMO         PIC X(4).
               10  CENTRO_UMO          PIC X(4).
               10  USERID_UMO          PIC X(8).
               10  NETNAME_UMO         PIC X(8).
               10  TIMEST_UMO          PIC X(26).
