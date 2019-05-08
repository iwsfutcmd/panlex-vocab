import subprocess
from operator import attrgetter
import os
import math
from io import StringIO

import regex as re

import psycopg2
from psycopg2.extras import NamedTupleCursor

DEBUG = False

LANGVAR_CACHE = {}
PAGE_COUNT_CACHE = {}
PAGE_SIZE = 50

LANGVAR_QUERY = """
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
"""

EXPR_QUERY = """
select
  expr.id,
  expr.txt,
  expr.txt_degr
from
  expr
where
  expr.langvar = uid_langvar(%s)
"""

EXPR_PAGE_QUERY = """
select
  uid_expr.id,
  uid_expr.txt
from
  uid_expr
where
  uid_expr.uid = %s
  and
  uid_expr.idx > %s
  and
  uid_expr.idx <= %s
order by
  uid_expr.idx
"""

TRANSLATE_QUERY = """
select
  expr.id,
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
"""

SCRIPT_RE = {
    "Blis": None,
    "Geok": r"\p{Geor}",
    "Hanb": r"\p{Hani}|\p{Bopo}",
    "Hans": r"\p{Hani}",
    "Hant": r"\p{Hani}",
    "Hrkt": r"\p{Hira}|\p{Kana}",
    "Jamo": r"\p{Hang}",
    "Jpan": r"\p{Hani}|\p{Hira}|\p{Kana}",
    "Kore": r"\p{Hani}|\p{Hang}",
    "Zmth": None,
    "Zsye": None,
    "Zsym": None,
}

def db_connect():
    global conn, cur
    conn = psycopg2.connect(dbname='plx')
    cur = conn.cursor(cursor_factory=NamedTupleCursor)

db_connect()

def query(query_string, args, one=False):
    if DEBUG:
        print(cur.mogrify(query_string, args).decode())
    try:
        cur.execute(query_string, args)
    except (psycopg2.OperationalError, psycopg2.errors.InFailedSqlTransaction):
        db_connect()
        cur.execute(query_string, args)
    try:
        if one:
            return cur.fetchone()
        else:
            return cur.fetchall()
    except psycopg2.ProgrammingError:
        return None

def refresh_expr_cache():
    query('truncate uid_expr', ())

    uids = [x.uid for x in query('select uid(lang_code,var_code) from langvar order by 1', ())]

    for uid in uids:
        refresh_expr_cache_langvar(uid)

    conn.commit()

def refresh_expr_cache_langvar(uid):
    print("fetching exprs for " + uid)
    script = get_langvar(uid).script_expr_txt
    sortfunc = sort_by_script(script)
    exprs = sorted(query(EXPR_QUERY, (uid,)), key=lambda x: sortfunc(x.txt_degr))

    copy = ''

    idx = 1
    for expr in exprs:
        copy += '\t'.join([uid,str(idx),str(expr.id),escape_for_copy(expr.txt)]) + '\n'
        idx = idx + 1

    f = StringIO(copy)
    cur.copy_from(f, 'uid_expr', columns=('uid','idx','id','txt'))
    f.close()

def sort_by_script(script):
    try:
        matchre = SCRIPT_RE[script]
    except KeyError:
        matchre = r"\p{" + script + r"}"

    if matchre is None:
        def sortfunc(string):
            return string
    else:
        def sortfunc(string):
            matchscript = bool(re.match(matchre, string))
            return (not matchscript, string)

    return sortfunc

def escape_for_copy(txt):
    return re.sub(r'\\', r'\\\\', txt)

def get_langvar(uid):
    try:
        return LANGVAR_CACHE[uid]
    except KeyError:
        #print("fetching langvar data for " + uid)
        LANGVAR_CACHE[uid] = query(LANGVAR_QUERY, (uid,), one=True)
        return get_langvar(uid)

def get_expr_page(uid, pageno):
    last_expr = pageno * PAGE_SIZE
    return query(EXPR_PAGE_QUERY, (uid, last_expr - PAGE_SIZE, last_expr))

def get_page_count(uid):
    try:
        return PAGE_COUNT_CACHE[uid]
    except KeyError:
        expr_count = query('select count(*) from uid_expr where uid = %s', (uid,), True).count
        PAGE_COUNT_CACHE[uid] = math.ceil(expr_count / PAGE_SIZE)
        return get_page_count(uid)

def get_translated_page(de_uid, al_uid, pageno):
    exprs = get_expr_page(de_uid, pageno)

    if al_uid != "":
        trans_results = query(TRANSLATE_QUERY, (al_uid, [expr.id for expr in exprs]))
    else:
        trans_results = []

    trans_dict = {expr.id: [] for expr in exprs}
    for trans in trans_results:
        trans_dict[trans.trans_expr].append(trans)

    return [(expr, trans_dict[expr.id]) for expr in exprs]
