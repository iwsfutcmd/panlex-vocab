import subprocess
from operator import attrgetter
import os
import math
from io import StringIO

import regex as re
import aiopg
import psycopg2
from psycopg2.extras import NamedTupleCursor
import asyncio

DEBUG = False

LANGVAR_CACHE = {}
PAGE_COUNT_CACHE = {}
CHAR_INDEX_CACHE = {}
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

async def connect():
    global pool
    pool = await aiopg.create_pool('dbname=plx', minsize=2, maxsize=4)

async def acquire():
    return await pool.acquire()

async def tx_begin(conn):
    cur = await conn.cursor()
    await cur.execute('begin', ())

async def tx_commit(conn):
    cur = await conn.cursor()
    await cur.execute('commit', ())

async def tx_rollback(conn):
    cur = await conn.cursor()
    await cur.execute('rollback', ())

async def query(query_string, args, one=False, conn=None):
    if conn:
        will_release = False
    else:
        conn = await pool.acquire()
        will_release = True

    cur = await conn.cursor(cursor_factory=NamedTupleCursor)

    if DEBUG:
        print(cur.mogrify(query_string, args).decode())

    await cur.execute(query_string, args)

    try:
        if one:
            result = await cur.fetchone()
        else:
            result = await cur.fetchall()
    except psycopg2.ProgrammingError:
        result = None

    if will_release:
        asyncio.ensure_future(pool.release(conn))

    return result

async def refresh_cache():
    conn = await acquire()
    await tx_begin(conn)

    await query('truncate uid_expr', (), conn=conn)
    await query('truncate uid_expr_char_index', (), conn=conn)

    uids = [x.uid for x in await query('select uid(lang_code,var_code) from langvar order by 1', (), conn=conn)]

    for uid in uids:
       await refresh_cache_langvar(uid, conn=conn)

    await tx_commit(conn)
    asyncio.ensure_future(pool.release(conn))

async def refresh_cache_langvar(uid, conn=None):
    print("fetching exprs for " + uid)
    script = (await get_langvar(uid, conn=conn)).script_expr_txt
    sortfunc = sort_by_script(script)
    exprs = await query(EXPR_QUERY, (uid,), conn=conn)
    exprs = sorted(exprs, key=lambda x: sortfunc(x.txt_degr))

    copy_uid_expr = ''
    idx = 1
    index_chars = []
    index_char_count = {}

    for expr in exprs:
        copy_uid_expr += '\t'.join([uid,str(idx),str(expr.id),escape_for_copy(expr.txt)]) + '\n'

        if expr.txt_degr:
            char = expr.txt_degr[0]
            if char in index_char_count:
                index_char_count[char] += 1
            elif match_script(char, script):
                index_chars.append((char,idx))
                index_char_count[char] = 1

        idx += 1

    cur = await conn.cursor()

    f = StringIO(copy_uid_expr)
    cur.copy_from(f, 'uid_expr', columns=('uid','idx','id','txt'))
    f.close()

    index_chars = list(filter(lambda x: index_char_count[x[0]] >= 3, index_chars))
    if index_chars:
        copy_uid_expr_char_index = ''
        char_idx = 1

        for i in index_chars:
            copy_uid_expr_char_index += '\t'.join([uid,str(char_idx),i[0],str(i[1])]) + '\n'
            char_idx += 1

        f = StringIO(copy_uid_expr_char_index)
        cur.copy_from(f, 'uid_expr_char_index', columns=('uid','idx','char','uid_expr_idx'))
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

def match_script(char, script):
    try:
        matchre = SCRIPT_RE[script]
    except KeyError:
        matchre = r"\p{" + script + r"}"

    if matchre is None:
        return True
    else:
        return bool(re.match(matchre, char))

def escape_for_copy(txt):
    return re.sub(r'\\', r'\\\\', txt)

async def get_langvar(uid, conn=None):
    try:
        return LANGVAR_CACHE[uid]
    except KeyError:
        #print("fetching langvar data for " + uid)
        LANGVAR_CACHE[uid] = await query(LANGVAR_QUERY, (uid,), one=True, conn=conn)
        return await get_langvar(uid, conn=conn)

async def get_expr_page(uid, pageno, conn=None):
    last_expr = pageno * PAGE_SIZE
    return await query(EXPR_PAGE_QUERY, (uid, last_expr - PAGE_SIZE, last_expr), conn=conn)

async def get_page_count(uid, conn=None):
    try:
        return PAGE_COUNT_CACHE[uid]
    except KeyError:
        expr_count = (await query('select count(*) from uid_expr where uid = %s', (uid,), one=True, conn=conn)).count
        PAGE_COUNT_CACHE[uid] = get_page_number(expr_count)
        return await get_page_count(uid, conn=conn)

async def get_char_index(uid, conn=None):
    try:
        return CHAR_INDEX_CACHE[uid]
    except KeyError:
        entries = await query('select char, uid_expr_idx as idx from uid_expr_char_index where uid = %s order by idx', (uid,), conn=conn)
        CHAR_INDEX_CACHE[uid] = [(x.char,get_page_number(x.idx)) for x in entries]
        return await get_char_index(uid, conn=conn)

def get_page_number(idx):
    return math.ceil(idx / PAGE_SIZE)

async def get_translated_page(de_uid, al_uid, pageno, conn=None):
    exprs = await get_expr_page(de_uid, pageno, conn=conn)

    if al_uid != "":
        trans_results = await query(TRANSLATE_QUERY, (al_uid, [expr.id for expr in exprs]), conn=conn)
    else:
        trans_results = []

    trans_dict = {expr.id: [] for expr in exprs}
    for trans in trans_results:
        trans_dict[trans.trans_expr].append(trans)

    return [(expr, trans_dict[expr.id]) for expr in exprs]
