CREATE MATERIALIZED VIEW prem.MVW_PREM_NOVEDADES_MILLAS_AA
   REFRESH
      START WITH (SYSDATE)
      NEXT (TRUNC (SYSDATE) + 1) + 1 / 24
      WITH ROWID
AS
select a.* from PREM.PREM_NOVEDADES_MILLAS_AA a,
                PREM.PREM_SOLICITUDES b,
                PREM.PREM_LOGS_SQL C
          where A.ID_SOLICITUD = B.ID_SOLICITUD
            AND B.ID_SOLICITUD = C.ID_SOLICITUD
            AND TRUNC (C.FECHA_EJECUCION) = TRUNC(SYSDATE-1);