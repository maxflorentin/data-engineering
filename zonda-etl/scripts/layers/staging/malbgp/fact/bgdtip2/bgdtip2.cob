      ******************************************************************        
      * COPY PARA LA BGDTIP2                                           *        
      *   TABLA CON LOS PLAZOS QUE ORIGINALMENTE PROVENIAN DE OTRA     *        
      *      DIVISA                                                    *        
      ******************************************************************        
      * PETICION  :03_02964_0A                                         *
      * FECHA     :09_07_2003                                          *
      * MODIFACION:ALTEC_0001                                          *
      * USUARIO   : WAL219                                             *
      * DESCRIP   : SE AGREGAN CAMPOS RELACIONADOS A PF PRECANCELABLE E*
      *             INTERESANTE. SE AGREGA EL NIVEL 88 DE VALOR 'F'    *
      *             DEL CAMPO PERIOD_LIQ.                              *
           02  BGDTIP2.
               10 ENTIDAD              PIC X(4).
               10 CENTRO_ALTA          PIC X(4).
               10 CUENTA               PIC X(12).
               10 SECUENCIA            PIC S9(5)V COMP-3.
               10 SECUENCIA_REN        PIC S9(5)V COMP-3.
               10 DIVISA               PIC X(3).
               10 IND_PRE_CANCEL       PIC X(1).
               10 PLZ_MIN_CANCEL       PIC S9(5)V COMP-3.
               10 POR_PENALIZ          PIC S9(3)V9(5) COMP-3.
               10 TAS_MIN_OFR          PIC S9(4)V9(5) COMP-3.
               10 TAS_MAX_OFR          PIC S9(8)V9(5) COMP-3.
               10 TAS_MIN_VAR          PIC S9(4)V9(5) COMP-3.
               10 TAS_MAX_VAR          PIC S9(4)V9(5) COMP-3.
               10 NUM_DIAS             PIC S9(5)V COMP-3.
               10 TAS_INC_SPREAD       PIC S9(4)V9(5) COMP-3.
               10 COR_CANAL            PIC S9(4)V9(5) COMP-3.
               10 COR_SEGMENTO         PIC S9(4)V9(5) COMP-3.
               10 INC_TASA             PIC S9(4)V9(5) COMP-3.
               10 DIA_PARA_LIQ         PIC S9(5)V COMP-3.
               10 TIP_DIA_LIQ_VAR      PIC X(3).
               10 NUM_DIA_LIQ_VAR      PIC S9(2)V COMP-3.
               10 TIP_PLZ_MIN_CANCEL   PIC X(02).
               10 TIP_LIQ_PREC         PIC X(02).
               10 IND_AJUS_PREC        PIC X(01).
               10 IND_LIQ_AJUS_PREC    PIC X(01).
               10 NUM_MAX_TASAS_PREC   PIC S9(03)V COMP-3.
               10 COD_TARIFA_PREC      PIC X(04).
               10 TAS_INC_PREC_APLIC   PIC S9(03)V9(05) COMP-3.
               10 TIP_INC_PREC         PIC X(01).
               10 TAS_MIN_VAR_GAR      PIC S9(4)V9(5) COMP-3.
               10 NUM_DIA_LIQ_HASTA    PIC S9(2)V COMP-3.
               10 NUM_FREC_REV_TAS     PIC S9(2) COMP-3.
               10 TIP_FREC_REV         PIC X(01).
               10 NUM_DIA_MIN_LIQ      PIC S9(2) COMP-3.
               10 TAS_PRIMER_TRAMO     PIC S9(4)V9(5) COMP-3.
               10 IND_BCA_PRIVADA      PIC X(01).
               10 ENTIDAD_UMO          PIC X(4).
               10 CENTRO_UMO           PIC X(4).
               10 USERID_UMO           PIC X(8).
               10 NETNAME_UMO          PIC X(8).
               10 TIMEST_UMO           PIC X(26).
               10 COD_REDON            PIC X(03).
