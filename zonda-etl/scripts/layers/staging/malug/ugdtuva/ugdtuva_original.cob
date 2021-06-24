      ******************************************************************00000200
      * ALTAIR - ACTIVO - PRESTAMOS Y AVALES                           *00000300
      * REGISTRO - UGTCUVA                                             *00000400
      * FECHA DE CREACION: 31-01-2020                                  *00000500
      * AREA DE DATOS PARA LA TABLA DE DATOS MAESTROS DE PRESTAMOS     *00000600
      * ALCANZADOS POR NORMATIVA UVA                                   *00000600
      ******************************************************************00000700
      *  LOG DE MODIFICACIONES                                         *00000800
      ******************************************************************00000900
      * FECHA | AUTOR  | DESCRIPCION                                   *00001000
      ******************************************************************00001100
      ******************************************************************00001400
      *                                                                *00001500
      * CAMPO                      DESCRIPCION                         *00001600
      ******************************************************************00001700
      * UGTCUVA                                                        *00001800
      *                                                                *00001900
      * CLAVE              CCC                                         *00002000
      * ENTIDAD            CLAVE DE ENTIDAD                            *00002100
      * OFICINA            OFICINA                                     *00002200
      * CUENTA             NUMERO DE CUENTA                            *00002300
      * FELIQ              FECHA DE VTO DEL RECIBO                     *00002600
      *                                                                         
      * DATOS                                                          *00002500
      * ESTADO             SI ESTA ALCANZADO POR LA NORMA              *00002700
      *    0 - ESTA ALCANZADO POR LA NORMA                             *00004000
      *    1 - DEJO DE ESTAR ALCANZADO POR CANCELACION PARCIAL         *00004100
      *    2 - DEJO DE ESTAR ALCANZADO POR LA NORMA                    *00004100
      * IMP_DIFERENCIA     IMPORTE DE DIFERENCIA A RESTAR              *00007500
      * RECIBOS-PEND       CANTIDAD DE RECIBOS PENDIENTES HASTA CUMPLIR*        
      *                    LA NORMATIVA (12 MESES)                     *        
      *                    ***LOG DE MODIFICACIONES**                  *00038400
      * ENTIDAD-UMO        ENTIDAD ULTIMA MODIFICACION                 *00038500
      * CENTRO-UMO         CENTRO ULTIMA MODIFICACION                  *00038600
      * USERID-UMO         USERID ULTIMA MODIFICACION                  *00038700
      * NETNAME-UMO        TERMINAL ULTIMA MODIFICACION                *00038800
      * TIMESTAMP          TIMESTAMP                                   *00038900
      * IND-RECIBO         INDICADOR RECIBO                            *00038900
      *    1 - SOLO BENEFICIO                                          *00038900
      *    2 - SOLO CONGELAMIENTO                                      *00038900
      *    3 - BENEFICIO Y CONGELAMIENTO                               *00038900
      * IMP-BENEFICIO      IMPORTE DE BENEFICIO                        *00038900
      * IMP-ALTA           IMPORTE PARA ALTA PRESTAMO                  *00038900
      * IMP-ALTA2          IMPORTE PARA ALTA PRESTAMO (RESERVADO)      *00038900
      * FEALTA-NVO         FECHA ALTA NUEVO PRESTAMO                   *00038900
      ******************************************************************00039000
       02 UGTCUVA.                                                      00039200
          05 UVA-CLAVE.                                                 00039300
             10 UVA-ENTIDAD            PIC X(04).                       00039400
             10 UVA-OFICINA            PIC X(04).                       00039500
             10 UVA-CUENTA             PIC X(12).                       00039600
             10 UVA-FELIQ              PIC X(10).                       00039600
          05 UVA-DATOS.                                                 00039700
             10 UVA-ESTADO             PIC X(01).                       00041100
                   88  UVA-88-ESTADO-NORMAL           VALUE '0'.        00041200
                   88  UVA-88-ESTADO-ENAN             VALUE '1'.        00041300
                   88  UVA-88-ESTADO-BAJA             VALUE '2'.        00041300
             10 UVA-IMP-DIFERENCIA     PIC S9(13)V9(4) COMP-3.          00044900
             10 UVA-RECIBOS-PEND       PIC X(04).                       00044900
             10 UVA-STAMP-UMO.                                          00074600
                15 UVA-ENTIDAD-UMO     PIC X(04).                       00074700
                15 UVA-CENTRO-UMO      PIC X(04).                       00074800
                15 UVA-USERID-UMO      PIC X(08).                       00074900
                15 UVA-NETNAME-UMO     PIC X(08).                       00075000
                15 UVA-TIMESTAMP       PIC X(26).                       00075100
             10 UVA-IND-RECIBO         PIC X(01).                       00075100
             10 UVA-IMP-BENEFICIO      PIC S9(13)V9(4) COMP-3.          00075100
             10 UVA-IMP-ALTA           PIC S9(13)V9(4) COMP-3.          00075100
             10 UVA-IMP-ALTA2          PIC S9(13)V9(4) COMP-3.          00075100
             10 UVA-FEALTA-NVO-PRES    PIC X(10).                       00075100
