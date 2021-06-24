      ******************************************************************
      * NOMBRE DE COPY : WABGPPER                                      *
      *****************************************************************
      *           COPY PARA NUEVO ARCHIVO MAESTRO QUE CONTIENE DATOS  *
      *           DE PLAZO FIJO Y DE PERSONAS                         *
      *                                                               *
      *           CONTIENE LA SIGUIENTE INFORMACIäN:                  *
      *                                                               *
      *           _ CONTRATO                                          *
      *           _ SECUENCIA Y SECUENCIA_REN DESCOMPRIMIDAS          *
      *           _ CENTRO GESTOR                                     *
      *           _ FECHA DE ALTA DEL CONTRATO                        *
      *           _ FECHA DEL PRäXIMO VENCIMIENTO                     *
      *           _ INDICADOR DE ESTADO                               *
      *           _ NUP (N§MERO §NICO DE PERSONA)                     *
      *           _ CALIDAD DE PARTICIPACIäN                          *
      *           _ ORDEN DE PARTICIPACIäN                            *
      *           _ TIPO DE DOCUMENTO                                 *
      *           _ N§MERO DE DOCUMENTO                               *
      *           _ NOMBRE                                            *
      *           _ APELLIDO                                          *
      *           _ TIPO DE PERSONA                                   *
      *           _ FECHA ALTA RELACIäN (MALPE)                       *
      *           _ FECHA BAJA RELACIäN (MALPE)                       *
      *           _ ID INVERSOR ESPEJO (SOLO PARA BANCA PRIVADA)      *
      *
      * LARGO DE REGISTRO: 318
      *----------------------------------------------------------------
      ******************************************************************
      *                                                                *
      *    MODIFICACION.: GOA001                                       *
      *    AUTOR........: GUSTAVO AOUADA                               *
      *    FECHA........: 03_12_2019                                   *
      *    DESCRIPCION..: SE AGREGA NUEVO CAMPO PARA CONTEMPLAR CODIGO *
      *                   ACTIVIDAD Y SE REDUCE TAMA#O FILLER          *
      *                                                                *
      *----------------------------------------------------------------*

       02  WABGPPER.
           10 ENTIDAD              PIC X(04).
           10 CENTRO_ALTA          PIC X(04).
           10 CUENTA               PIC X(12).
           10 SECUENCIA            PIC 9(05) COMP-3.
           10 SECUENCIA_REN        PIC 9(05) COMP-3.
           10 SEC_DESCOM           PIC 9(05).
           10 SECREN_DESCOM        PIC 9(05).
           10 CENTRO_GEST          PIC X(04).
           10 FECHA_ALTA           PIC X(10).
           10 FECHA_PROVEN         PIC X(10).
           10 ESTADO               PIC X(01).
           10 NUP                  PIC X(08).
           10 CALPAR               PIC X(02).
           10 ORDPAR               PIC 9(03).
           10 TIPDOC               PIC X(02).
           10 NUMDOC               PIC X(11).
           10 NOMBRE               PIC X(40).
           10 APELLIDO             PIC X(20).
           10 TIPOPER              PIC X(01).
           10 FECALT               PIC X(10).
           10 FECBAJ               PIC X(10).
           10 INV_BP               PIC X(20).
           10 SUBSEGMENTO          PIC X(03).
           10 CODIGO_ACTIVIDAD     PIC X(08).
           10 FILLER               PIC X(119).

