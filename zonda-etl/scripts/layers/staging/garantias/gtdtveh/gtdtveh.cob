      ******************************************************************
      *                                                                *
      * NOMBRE DEL OBJETO: GTTCVEH                                     *
      *                                                                *
      * DESCRIPCION: TABLA DE DATOS COMPLEMENTARIOS                    *
      *              DE BIENES VEHICULOS.                              *
      *                                                                *
      * ______________________________________________________________ *
      *                                                                *
      *           LONGITUD : 157 POSICIONES.                           *
      *           PREFIJO  : VEH.                                      *
      *                                                                *
      ******************************************************************

       01  GTTCVEH.
               10  VEH_COD_ENTIDAD         PIC X(04).
               10  VEH_NUM_BIEN            PIC S9(9) COMP-3.
               10  VEH_ANIO_FABRICAC       PIC S9(4) COMP-3.
               10  VEH_NUM_MOTOR           PIC X(20).
               10  VEH_NUM_PATENTE         PIC X(20).
               10  VEH_NUM_CHASIS          PIC X(20).
               10  VEH_COD_MARCAVEH        PIC X(03).
               10  VEH_COD_MODELOVE        PIC X(03).
               10  VEH_TIP_VEHICULO        PIC X(03).
               10  VEH_COD_COLORVEH        PIC X(03).
               10  VEH_COD_USOVEHIC        PIC X(03).
               10  VEH_COD_ESTATUSA        PIC X(03).
               10  VEH_TIP_CARROCER        PIC X(03).
               10  VEH_NUM_SERIE           PIC X(20).
               10  VEH_NUM_FACTURA         PIC X(20).
               10  VEH_NUM_MATRICULA       PIC X(20).
               10  VEH_ENTIDAD_UMO         PIC X(04).
               10  VEH_CENTRO_UMO          PIC X(04).
               10  VEH_USERID_UMO          PIC X(08).
               10  VEH_NETNAME_UMO         PIC X(08).
               10  VEH_TIMEST_UMO          PIC X(26).
