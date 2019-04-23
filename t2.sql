select
  expr.id,
  expr.langvar,
  expr.txt,
  expr.txt_degr
from
  expr
where
  expr.langvar = uid_langvar('nav-000')
order by
  expr.txt asc
limit
  2000