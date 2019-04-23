select
  expr.id,
  expr.langvar,
  expr.txt,
  expr.txt_degr
from
  expr
where
  expr.langvar = uid_langvar(%s)
