import os
import math
import asyncio

import regex as re
import asyncpg

DEBUG = False

LANGVAR_CACHE = {}
SOURCE_CACHE = {}
PAGE_SIZE = 50

LANGVAR_QUERY = """
select
  langvar.id,
  expr.txt as name_expr_txt,
  uid(langvar.lang_code, langvar.var_code),
  script_expr.txt as script_expr_txt,
  vocab_langvar.expr_count,
  vocab_langvar.analyzed_source_count
from
  langvar
  left join vocab_langvar on vocab_langvar.id = langvar.id
  join expr on expr.id = langvar.name_expr
  join expr as script_expr on script_expr.id = langvar.script_expr
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
  vocab_expr.id,
  vocab_expr.txt
from
  vocab_expr
where
  vocab_expr.uid = $1
  and
  vocab_expr.idx > $2
  and
  vocab_expr.idx <= $3
order by
  vocab_expr.idx
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
  join denotationx as denotation on denotation.expr = expr.id
  join denotationx as denotationsrc on denotationsrc.meaning = denotation.meaning
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

# WIP
SOURCE_QUERY = """
select
  source.id,
  source.label,
  source_editorial.denotation_count as denotation_count_estimate
from
  source
  join source_editorial on source_editorial.source = source.id
where
  source_editorial.submit_file is true
  and exists (select 1 from source_langvar where source_langvar.source = source.id and source_langvar.langvar = uid_langvar('nav-000'))
order by
  source.id asc
"""

SEARCH_QUERY = """
select
  vocab_expr.idx
from
  vocab_expr
  join expr on expr.id = vocab_expr.id
where
  vocab_expr.uid = $1
  and expr.txt_degr like $2
order by vocab_expr.idx
limit 1
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
    pool = await asyncpg.create_pool(database="plx", min_size=1, max_size=4)

async def query(sql, args=(), fetch="all", conn=None):
    if conn:
        will_release = False
    else:
        conn = await pool.acquire()
        will_release = True

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
    if "conn" in kwargs:
        conn = kwargs["conn"]
        del kwargs["conn"]
        will_release = False
    else:
        conn = await pool.acquire()
        will_release = True

    await conn.copy_records_to_table(*args, **kwargs)

    if will_release:
        asyncio.ensure_future(pool.release(conn))

async def refresh_cache():
    conn = await pool.acquire()

    async with conn.transaction():
        await query('truncate vocab_expr', fetch="none", conn=conn)
        await query('truncate vocab_langvar', fetch="none", conn=conn)

        uids = [x["uid"] for x in await query('select uid(lang_code,var_code) from langvar order by 1', conn=conn)]

        for uid in uids:
            await refresh_cache_langvar(uid, conn)

    asyncio.ensure_future(pool.release(conn))

async def refresh_cache_langvar(uid, conn):
    print("fetching exprs for " + uid)
    langvar = await get_langvar(uid, conn=conn)
    sortfunc = sort_by_script(langvar["script_expr_txt"])
    exprs = await query(EXPR_QUERY, (uid,), conn=conn)
    exprs = sorted(exprs, key=lambda x: sortfunc(x["txt_degr"],x["txt"]))

    copy_vocab_expr = []
    idx = 1

    for expr in exprs:
        copy_vocab_expr.append((uid,idx,expr["id"],expr["txt"]))
        idx += 1

    await copy_records_to_table("vocab_expr", records=copy_vocab_expr, columns=["uid","idx","id","txt"], conn=conn)

    asc = await query("select count(*) from source where exists (select 1 from denotationx dx where dx.langvar = $1 and dx.source = source.id)", args=(langvar["id"],), fetch="val", conn=conn)
    await query("insert into vocab_langvar (id, expr_count, analyzed_source_count) values ($1, $2, $3)", args=(langvar["id"],len(exprs),asc), fetch="none", conn=conn)

def sort_by_script(script):
    try:
        matchre = SCRIPT_RE[script]
    except KeyError:
        matchre = r"\p{" + script + r"}"

    if matchre is None:
        def sortfunc(txt_degr, txt):
            return (txt_degr, txt)
    else:
        def sortfunc(txt_degr, txt):
            matchscript = bool(re.match(matchre, txt_degr))
            return (not matchscript, txt_degr, txt)

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

async def get_langvar(uid, conn=None):
    try:
        return LANGVAR_CACHE[uid]
    except KeyError:
        await get_all_langvars(conn=conn)
        return await get_langvar(uid, conn=conn)

async def get_all_langvars(conn=None):
    try:
        LANGVAR_CACHE["*"]
        return [LANGVAR_CACHE[uid] for uid in LANGVAR_CACHE if uid != "*"]
    except KeyError:
        print("fetching all langvar data...")
        for langvar in await query(LANGVAR_QUERY, conn=conn):
            LANGVAR_CACHE[langvar["uid"]] = langvar
        LANGVAR_CACHE["*"] = True
        return await get_all_langvars()

async def get_expr_page(uid, pageno):
    last_expr = pageno * PAGE_SIZE
    return await query(EXPR_PAGE_QUERY, (uid, last_expr - PAGE_SIZE, last_expr))

async def get_page_count(uid):
    langvar = await get_langvar(uid)
    return get_page_number(langvar["expr_count"])

def get_page_number(idx):
    return math.ceil(idx / PAGE_SIZE)

async def get_translated_page(de_uid, al_uid, pageno):
    exprs = await get_expr_page(de_uid, pageno)

    if al_uid != "":
        trans_results = await query(TRANSLATE_QUERY, (al_uid, [expr["id"] for expr in exprs]))
    else:
        trans_results = []

    trans_dict = {expr["id"]: [] for expr in exprs}
    for trans in trans_results:
        trans_dict[trans["trans_expr"]].append(trans)

    return [(expr, trans_dict[expr["id"]]) for expr in exprs]

# WIP
async def get_sources(uid, conn=None):
    try:
        return SOURCE_CACHE[uid]
    except KeyError:
        pass

async def get_matching_page(uid, txt, conn=None):
    txt_degr = await query('select txt_degr($1)', (txt,), fetch="val", conn=conn)
    if not txt_degr:
        return None

    txt_degr_like = like_escape(txt_degr) + "%"

    idx = await query(SEARCH_QUERY, (uid, txt_degr_like), fetch="val", conn=conn)

    if idx:
        return get_page_number(idx)
    else:
        return None

def like_escape(txt):
    return re.sub(r"([\\%_])", r"\\$1", txt)
