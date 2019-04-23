select
  expr.id,
  expr.langvar,
  expr.txt,
  denotationsrc.expr as trans_expr,
  grp_quality_score(
    array_agg(denotation.grp),
    array_agg(denotation.quality)
  ) as trans_quality
from
  expr
  inner join denotationx as denotation on denotation.expr = expr.id
  inner join denotationx as denotationsrc on denotationsrc.meaning = denotation.meaning
  and denotationsrc.expr != denotation.expr
where
  expr.langvar = uid_langvar(%s)
  and denotationsrc.expr = any(%s)
group by
  expr.id,
  denotationsrc.expr
order by
  trans_quality desc