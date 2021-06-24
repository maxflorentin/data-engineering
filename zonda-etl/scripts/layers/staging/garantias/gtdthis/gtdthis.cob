      ******************************************************************
      *                                                                      *
      * NOMBRE DEL OBJETO:  GTTCHIS                                    *
      *                                                                *
      * DESCRIPCION: HISTORIAL DE LA GARANTIA.                         *
      * ______________________________________________________________ *
      * GTTCHIS                                                        *
      *  CLAVE                                                         *
      *   COD_ENTIDAD            ENTIDAD                               *
      *   NUM_GARANTIA           NUMERO DE GARANTIA                    *
      *   NUM_SECHISTO           NUMERO DE SECUENCIA DENTRO DEL        *
      *                          HISTORIAL DE LA GARANTIA              *
      *  DATOS                                                         *
      *   TIP_EVENTO             TIPO DE EVENTO                        *
      *   DES_HISTORIA           DESCRIPCION                           *
      *  STAMP                                                         *
      *   ENTIDAD_UMO            ENTIDAD ULTIMA MODIFICACION           *
      *   CENTRO_UMO             CENTRO DE ULTIMA MODIFICACION         *
      *   USERID_UMO             USUARIO ULTIMA MODIFICACION           *
      *   NETNAME_UMO            TERMINAL ULTIMA MODIFICACION          *
      *   TIMEST_UMO             TIMESTAMP ULTIMA MODIFICACION         *
      *                                                                *
      ******************************************************************
       02  GTTCHIS.
               10  HIS_COD_ENTIDAD         PIC X(4).
               10  HIS_NUM_GARANTIA        PIC S9(9)V COMP-3.
               10  HIS_NUM_SECHISTO        PIC S9(9)V COMP-3.
               10  HIS_TIP_EVENTO          PIC X(3).
               10  HIS_DES_HISTORIA        PIC X(50).
               10  HIS_ENTIDAD_UMO         PIC X(4).
               10  HIS_CENTRO_UMO          PIC X(4).
               10  HIS_USERID_UMO          PIC X(8).
               10  HIS_NETNAME_UMO         PIC X(8).
               10  HIS_TIMEST_UMO          PIC X(26).

