import subprocess
from operator import attrgetter

import regex as re

import keyring
import psycopg2
from psycopg2.extras import NamedTupleCursor
import getpass


USERNAME = getpass.getuser()
DEBUG = False

EXPR_CACHE = {}
LANGVAR_CACHE = {}
PAGE_SIZE = 50

def db_connect():
    global conn, cur
    try:
        subprocess.run(['autossh', '-M 20000', '-f', '-NT', '-L 5432:localhost:5432', 'db.panlex.org'])
    except FileNotFoundError:
        pass
    try:
        conn = psycopg2.connect(
            dbname='plx',
            user=USERNAME,
            password=keyring.get_password('panlex_db', USERNAME),
            host='localhost')
    except psycopg2.OperationalError:
        keyring.set_password('panlex_db', USERNAME, getpass.getpass('Enter PanLex db password: '))
        conn = psycopg2.connect(
            dbname='plx',
            user=USERNAME,
            password=keyring.get_password('panlex_db', USERNAME),
            host='localhost')
    cur = conn.cursor(cursor_factory=NamedTupleCursor)

db_connect()

def query(query_string, args, one=False):
    if DEBUG:
        print(cur.mogrify(query_string, args).decode())
    try:
        cur.execute(query_string, args)
    except psycopg2.OperationalError:
        db_connect()
        cur.execute(query_string, args)
    if one:
        return cur.fetchone()
    else:
        return cur.fetchall()

def all_expr(uid):
    return query(open("expr_query.sql").read(), (uid,))

def sort_by_script(script):
    def sortfunc(string):
        matchscript = bool(re.match(r"\p{" + script + r"}", string))
        return (not matchscript, string)
    return sortfunc

def get_script(uid):
    try:
        return LANGVAR_CACHE[uid].script_expr_txt
    except KeyError:
        print("fetching langvar data for " + uid)
        LANGVAR_CACHE[uid] = query(open("langvar_query.sql").read(), (uid,), one=True)
        return get_script(uid)

def get_langvar_name(uid):
    try:
        return LANGVAR_CACHE[uid].name_expr_txt
    except KeyError:
        print("fetching langvar data for " + uid)
        LANGVAR_CACHE[uid] = query(open("langvar_query.sql").read(), (uid,), one=True)
        return get_langvar_name(uid)

def get_exprs(uid):
    try:
        return EXPR_CACHE[uid]
    except KeyError:
        script = get_script(uid)
        print("fetching exprs for " + uid)
        exprs = sorted(all_expr(uid), key=lambda x: sort_by_script(script)(x.txt_degr))
        EXPR_CACHE[uid] = [exprs[i:i + PAGE_SIZE] for i in range(0, len(exprs), PAGE_SIZE)]
        return get_exprs(uid)

def get_translated_page(de_uid, al_uid, pageno):
    exprs = get_exprs(de_uid)[pageno]
    trans_results = query(open("translate_query.sql").read(), (al_uid, [expr.id for expr in exprs]))
    trans_dict = {expr.id: [] for expr in exprs}
    for trans in trans_results:
        trans_dict[trans.trans_expr].append(trans)
    return [(expr, trans_dict[expr.id]) for expr in exprs]
