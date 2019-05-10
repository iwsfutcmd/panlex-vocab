import subprocess
from operator import attrgetter
import os
import math

import regex as re
import asyncio
import asyncpg

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
  uid(langvar.lang_code, langvar.var_code) = $1
"""

EXPR_QUERY = """
select
  expr.id,
  expr.txt,
  expr.txt_degr
from
  expr
where
  expr.langvar = uid_langvar($1)
"""

EXPR_PAGE_QUERY = """
select
  uid_expr.id,
  uid_expr.txt
from
  uid_expr
where
  uid_expr.uid = $1
  and
  uid_expr.idx > $2
  and
  uid_expr.idx <= $3
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
  expr.langvar = uid_langvar($1)
  and denotationsrc.expr = any($2)
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

tx_conn = None

async def connect():
    global pool
    pool = await asyncpg.create_pool(database="plx", min_size=1, max_size=4)

async def tx_begin():
    global tx_conn
    tx_conn = await pool.acquire()

def tx_release():
    global tx_conn
    asyncio.ensure_future(pool.release(tx_conn))
    tx_conn = None

async def query(sql, args=(), fetch="all"):
    if tx_conn:
        conn = tx_conn
        will_release = False
    else:
        conn = await pool.acquire()
        will_release = True

    # if DEBUG:
    #     print(cur.mogrify(sql, args).decode())

    if fetch == "all":
        result = await conn.fetch(sql, *args)
    elif fetch == "row":
        result = await conn.fetchrow(sql, *args)
    elif fetch == "val":
        result = await conn.fetchval(sql, *args)
    else:
        await conn.execute(sql, *args)
        result = None

    if will_release:
        asyncio.ensure_future(pool.release(conn))

    return result

async def copy_records_to_table(*args, **kwargs):
    if tx_conn:
        conn = tx_conn
        will_release = False
    else:
        conn = await pool.acquire()
        will_release = True

    await conn.copy_records_to_table(*args, **kwargs)

    if will_release:
        asyncio.ensure_future(pool.release(conn))

async def refresh_cache():
    await tx_begin()

    async with tx_conn.transaction():
        await query('truncate uid_expr', fetch="none")
        await query('truncate uid_expr_char_index', fetch="none")

        uids = [x['uid'] for x in await query('select uid(lang_code,var_code) from langvar order by 1')]

        for uid in uids:
            await refresh_cache_langvar(uid)

    tx_release()

async def refresh_cache_langvar(uid):
    print("fetching exprs for " + uid)
    script = (await get_langvar(uid))['script_expr_txt']
    sortfunc = sort_by_script(script)
    exprs = await query(EXPR_QUERY, (uid,))
    exprs = sorted(exprs, key=lambda x: sortfunc(x['txt_degr']))

    copy_uid_expr = []
    idx = 1
    index_chars = []
    index_char_count = {}

    for expr in exprs:
        copy_uid_expr.append((uid,idx,expr['id'],expr['txt']))

        if expr['txt_degr']:
            char = expr['txt_degr'][0]
            if char in index_char_count:
                index_char_count[char] += 1
            elif match_script(char, script):
                index_chars.append((char,idx))
                index_char_count[char] = 1

        idx += 1

    await copy_records_to_table('uid_expr', records=copy_uid_expr, columns=['uid','idx','id','txt'])

    index_chars = list(filter(lambda x: index_char_count[x[0]] >= 3, index_chars))
    if index_chars:
        copy_uid_expr_char_index = []
        char_idx = 1

        for i in index_chars:
            copy_uid_expr_char_index.append((uid,char_idx,i[0],i[1]))
            char_idx += 1

        await copy_records_to_table('uid_expr_char_index', records=copy_uid_expr_char_index, columns=['uid','idx','char','uid_expr_idx'])

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

async def get_langvar(uid):
    try:
        return LANGVAR_CACHE[uid]
    except KeyError:
        #print("fetching langvar data for " + uid)
        LANGVAR_CACHE[uid] = await query(LANGVAR_QUERY, (uid,), fetch="row")
        return await get_langvar(uid)

async def get_expr_page(uid, pageno):
    last_expr = pageno * PAGE_SIZE
    return await query(EXPR_PAGE_QUERY, (uid, last_expr - PAGE_SIZE, last_expr))

async def get_page_count(uid):
    try:
        return PAGE_COUNT_CACHE[uid]
    except KeyError:
        expr_count = await query('select count(*) from uid_expr where uid = $1', (uid,), fetch="val")
        PAGE_COUNT_CACHE[uid] = get_page_number(expr_count)
        return await get_page_count(uid)

async def get_char_index(uid):
    try:
        return CHAR_INDEX_CACHE[uid]
    except KeyError:
        entries = await query('select char, uid_expr_idx as idx from uid_expr_char_index where uid = $1 order by idx', (uid,))
        CHAR_INDEX_CACHE[uid] = [(x['char'],get_page_number(x['idx'])) for x in entries]
        return await get_char_index(uid)

def get_page_number(idx):
    return math.ceil(idx / PAGE_SIZE)

async def get_translated_page(de_uid, al_uid, pageno):
    exprs = await get_expr_page(de_uid, pageno)

    if al_uid != "":
        trans_results = await query(TRANSLATE_QUERY, (al_uid, [expr['id'] for expr in exprs]))
    else:
        trans_results = []

    trans_dict = {expr['id']: [] for expr in exprs}
    for trans in trans_results:
        trans_dict[trans['trans_expr']].append(trans)

    return [(expr, trans_dict[expr['id']]) for expr in exprs]
