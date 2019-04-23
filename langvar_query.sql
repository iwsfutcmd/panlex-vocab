select
  langvar.lang_code,
  expr.txt as name_expr_txt,
  uid(langvar.lang_code, langvar.var_code),
  langvar.var_code,
  script_expr.txt as script_expr_txt
from
  langvar
  inner join expr on expr.id = langvar.name_expr
  inner join expr as script_expr on script_expr.id = langvar.script_expr
where
  uid(langvar.lang_code, langvar.var_code) = %s