CREATE VIEW IF NOT EXISTS bi_corp_common.vstk_emp_arbol_ccostos AS

select
    a.ccostos cod_emp_ccostos,
    a.descripcion ds_emp_ccostos,
    a.usuario_responsable ds_emp_usuario_responsable,
    a.ayn_responsable ds_emp_ayn_responsable,
    a.ccostos_padre cod_emp_ccostos_padre,
    a.descripcion_padre ds_emp_ccostos_padre,
    a.partition_date
from
    bi_corp_staging.arbol_cc_vw_lake a
;