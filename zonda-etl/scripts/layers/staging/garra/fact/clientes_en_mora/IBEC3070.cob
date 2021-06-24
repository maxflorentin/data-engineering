      ******************************************************************
      *             DESCRIPCION DE LOS CAMPOS DE LA COPY.              *
      ******************************************************************
      *        IBEC3070 --> C L I E N T E S   G A R R A                *
      *        MAESTRO DE CLIENTES CON DATOS DE PERSONAS               *
      ******************************************************************
      *                                                                *
      * IBEC3070-NUM-PERSONA    --> NUMERO DE PERSONA                  *
      * IBEC3070-MARCA-LO       --> MARCA LOCAL DEL CLIENTE            *
      * IBEC3070-COD-MARCLI     --> MARCA DE CLIENTE                   *
      * IBEC3070-COD-SUBMARCL   --> SUBMARCA DE CLIENTE                *
      * IBEC3070-FEC-ALMARCL    --> FECHA ALTA MARCA CLIENTE           *
      * IBEC3070-FEC-ALTSMCL    --> FECHA ALTA SUBMARCA CLIENTE        *
      * IBEC3070-COD-GESTOR     --> CD. DE GESTOR                     *
      * IBEC3070-COD-SITUIRRE   --> CD. SITUACIN IRREGULAR           *
      * IBEC3070-GRADO-FEVE     --> GRADO FEVE                         *
      * IBEC3070-IMP-RIESGO     --> IMPORTE DE RIESGO                  *
      * IBEC3070-IMP-SITUIRRE   --> IMPORTE EN SITUACIN IRREGULAR     *
      * IBEC3070-MARCA-TOPN     --> MARCA CLIENTE TOPN (S / N)         *
      * IBEC3070-COD-KGL        --> CDIGO KGL                         *
      * IBEC3070-PRIM-APELLIDO  --> PRIMER APELLIDO                    *
      * IBEC3070-NOMBRE         --> NOMBRE                             *
      * IBEC3070-TIPO-PERSONA   --> TIPO DE PERSONA - FSICA/JURDICA  *
      * IBEC3070-TIPO-DOCUMENTO --> TIPO DE DOCUMENTO DE IDENTIDAD     *
      * IBEC3070-NUM-DOCUMENTO  --> NRO. DE DOCUMENTO                  *
      * IBEC3070-COD-ACTIVIDAD  --> CDIGO DE ACTIVIDAD ECONMICA      *
      * IBEC3070-SEGMENTO       --> SEGMENTO AL QUE PERTENECE EL       *
      *                             CLIENTE                            *
      * IBEC3070-SUBSEGMENTO    --> SUBSEGMENTO AL QUE PERTENECE EL    *
      *                             CLIENTE                            *
      * IBEC3070-MARCA-PYME     --> MARCA SEGMENTO PYME                *
      * IBEC3070-UC-BANCA       --> CD.BANCA S/UNID. CTRAL. ACTIVOS   *
      *                             IRREG.                             *
      * IBEC3070-UC-DIVISION    --> CD.DIVIS. S/UNID. CTRAL. ACTIVOS  *
      *                             IRREG.                             *
      * IBEC3070-UC-SUC-ADM     --> SUCUR.ADMIN. S/UNID. CTRAL. ACTIVOS*
      *                             IRREG.                             *
      * IBEC3070-EMPLEADO       --> MARCA DE EMPLEADO                  *
      * IBEC3070-TIPO-SOCIE     --> TIPO DE SOCIEDAD RELACIONADA AL    *
      *                             CLIENTE                            *
      * IBEC3070-DOMICILIO      --> DOMICILIO PRINCIPAL DEL CLIENTE    *
      * IBEC3070-LOCALIDAD      --> LOCALIDAD PRINCIPAL DEL CLIENTE    *
      * IBEC3070-PROVINCIA      --> CD. DE PROVINCIA PRINCIPAL DEL    *
      *                             CLIENTE                            *
      * IBEC3070-PAIS           --> PAS DE RESIDENCIA                 *
      * IBEC3070-CODPOS         --> CD. POSTAL                        *
      * IBEC3070-SEXO           --> SEXO                               *
      * IBEC3070-FEC-ALTACLI    --> FECHA DE ALTA DEL CLIENTE EN       *
      *                             PERSONAS                           *
      * IBEC3070-FEC-NACIM      --> FECHA DE NACIMIENTO DEL CLIENTE    *
      * IBEC3070-TELEFONO       --> NRO. DE TELFONO                   *
      * IBEC3070-ESTADO-CIVIL   --> ESTADO CIVIL                       *
      * IBEC3070-NACIONAL       --> NACIONALIDAD                       *
      * IBEC3070-MON-ING-BRUTOS --> INGRESOS BRUTOS                    *
      * IBEC3070-MON-ING-GPO-FLIA --> INGRESOS GRUPO FAMILIAR          *
      * IBEC3070-MON-GASTOS     --> GASTOS                             *
      * IBEC3070-IMP-INGRESO    --> IMPORTE DE INGRESO                 *
      * IBEC3070-PROFESION      --> PROFESION DEL CLIENTE              *
      * IBEC3070-TIPO-ACTIVIDAD --> TIPO DE ACTIVIDAD                  *
      * IBEC3070-CARGO-EMPRESA  --> CARGO EN LA EMPRESA                *
      * IBEC3070-NIVEL-ESTUDIO  --> NIVEL DE ESTUDIO DEL CLIENTE       *
      * IBEC3070-COD-ENT-PREV   --> CODIGO DE ENTIDAD DE PREVISION     *
      * IBEC3070-RAMO-EMPRESA   --> RAMO DE LA EMPRESA                 *
      * IBEC3070-RESP-IVA       --> RESPONSABILIDAD ANTE IVA           *
      * IBEC3070-RESP-GAN       --> RESPONSABILIDAD ANTE GANANCIAS     *
      * IBEC3070-RESP-ING       --> RESPONSABILIDAD ANTE INGRESOS      *
      * IBEC3070-FECHA-ULT-ACRED--> FECHA DE ULTIMA ACREDITACION DEL   *
      *                             PLAN SUELDO                        *
      * IBEC3070-TIPO-PAQUETE   --> TIPO DE PAQUETE DONDE EL CLIENTE ES*
      *                             TITULAR, EN CASO DE SER TITULAR EN *
      *                             MAS DE UNO, ES EL MEJOR PAQUETE    *
      *                                                                *
      ******************************************************************
      * COMPILAR CON COBOL II                                          *
      * EL PREFIJO DEBE SER REEMPLAZADO POR UN COPY REPLACING          *
      * EJ.: COPY IBEC3070 REPLACING ==:IBEC3070:== BY ==IBEC3070==    *
      ******************************************************************
           02 :IBEC3070:-REG.
               05 :IBEC3070:-MAESTRO.
                  10 :IBEC3070:-NUM-PERSONA       PIC X(08).
                  10 :IBEC3070:-MARCA-LO          PIC X(02).
                  10 :IBEC3070:-COD-MARCLI        PIC X(02).
                  10 :IBEC3070:-COD-SUBMARCL      PIC X(02).
                  10 :IBEC3070:-FEC-ALMARCL       PIC X(10).
                  10 :IBEC3070:-FEC-ALTSMCL       PIC X(10).
                  10 :IBEC3070:-COD-GESTOR        PIC X(04).
                  10 :IBEC3070:-COD-SITUIRRE      PIC X(02).
                  10 :IBEC3070:-GRADO-FEVE        PIC X(02).
                  10 :IBEC3070:-IMP-RIESGO        PIC X(19).
                  10 :IBEC3070:-IMP-SITUIRRE      PIC X(19).
                  10 :IBEC3070:-COD-TOPN          PIC X(01).
                  10 :IBEC3070:-COD-KGL           PIC X(08).
                  10 :IBEC3070:-PRIM-APELLIDO     PIC X(40).
                  10 :IBEC3070:-NOMBRE            PIC X(40).
                  10 :IBEC3070:-TIPO-PERSONA      PIC X(01).
                  10 :IBEC3070:-TIPO-DOCUMENTO    PIC X(02).
                  10 :IBEC3070:-NUM-DOCUMENTO     PIC X(11).
                  10 :IBEC3070:-COD-ACTIVIDAD     PIC X(08).
                  10 :IBEC3070:-SEGMENTO          PIC X(03).
                  10 :IBEC3070:-SUBSEGMENTO       PIC X(03).
                  10 :IBEC3070:-MARCA-PYME        PIC X(01).
                  10 :IBEC3070:-UC-BANCA          PIC X(02).
                  10 :IBEC3070:-UC-DIVISION       PIC X(02).
                  10 :IBEC3070:-UC-SUC-ADM        PIC X(04).
                  10 :IBEC3070:-EMPLEADO          PIC X(01).
                  10 :IBEC3070:-TIPO-SOCIE        PIC X(02).
                  10 :IBEC3070:-DOMICILIO         PIC X(50).
                  10 :IBEC3070:-LOCALIDAD         PIC X(30).
                  10 :IBEC3070:-PROVINCIA         PIC X(02).
                  10 :IBEC3070:-PAIS              PIC X(03).
                  10 :IBEC3070:-CODPOS            PIC X(08).
                  10 :IBEC3070:-SEXO              PIC X(01).
                  10 :IBEC3070:-FEC-ALTACLI       PIC X(10).
                  10 :IBEC3070:-FEC-NACIM         PIC X(10).
                  10 :IBEC3070:-TELEFONO          PIC X(20).
                  10 :IBEC3070:-ESTADO-CIVIL      PIC X(01).
                  10 :IBEC3070:-NACIONAL          PIC X(04).
               05 :IBEC3070:-INGRESOS.
                  10 :IBEC3070:-MON-INGR-BRUTOS   PIC X(17).
                  10 :IBEC3070:-MON-INGR-GPO-FLIA PIC X(17).
                  10 :IBEC3070:-MON-GASTOS        PIC X(17).
                  10 :IBEC3070:-IMP-INGRESO       PIC X(20).
               05 :IBEC3070:-DATOS-LAB.
                  10 :IBEC3070:-PROFESION         PIC X(02).
                  10 :IBEC3070:-TIPO-ACTIVIDAD    PIC X(04).
                  10 :IBEC3070:-CARGO-EMPRESA     PIC X(02).
                  10 :IBEC3070:-NIVEL-ESTUDIO     PIC X(03).
                  10 :IBEC3070:-COD-ENT-PREV      PIC X(04).
                  10 :IBEC3070:-RAMO-EMPRESA      PIC X(04).
               05 :IBEC3070:-SIT-FISCAL.
                  10 :IBEC3070:-RESP-IVA          PIC X(02).
                  10 :IBEC3070:-RESP-GAN          PIC X(01).
                  10 :IBEC3070:-RESP-ING          PIC X(03).
               05 :IBEC3070:-PLAN-SUELDO.
                  10 :IBEC3070:-FECHA-ULT-ACRED   PIC X(10).
               05 :IBEC3070:-PRODUCTO.
                  10 :IBEC3070:-TIPO-PAQUETE      PIC X(04).
      ******************************************************************
      *                         LONGITUD 458                           *
      ******************************************************************
