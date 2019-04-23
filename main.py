from collections import namedtuple
import os

from sanic import Sanic, response
from jinja2 import Template

import panlex_db

Trn = namedtuple("Trn", ["de_ex", "al_ex"])
PAGE_RANGE = 2

app = Sanic()

app.static("/vocab/static", "./static")
app.static("/favicon.ico", "./favicon.ico")

@app.route("/<de_uid>/<al_uid>")
async def main(request, de_uid, al_uid):
    template = Template(open("vocab.jinja2").read())
    try:
        page = int(request.args["page"][0])
    except KeyError:
        page = 1
    try:
        de_lang_name = panlex_db.get_langvar_name(de_uid)
    except AttributeError:
        de_lang_name = ""
    try:
        de_expr_pages = panlex_db.get_exprs(de_uid)
    except AttributeError:
        de_expr_pages = [[]]
    try:
        al_lang_name = panlex_db.get_langvar_name(al_uid)
    except AttributeError:
        al_lang_name = ""
    try:
        trn_list = panlex_db.get_translated_page(de_uid, al_uid, page - 1)
    except (IndexError, AttributeError) as e:
        trn_list = []
    return response.html(template.render(
        de_lang_name=de_lang_name,
        al_lang_name=al_lang_name,
        trn_list=trn_list,
        page=page,
        last_page=len(de_expr_pages),
        page_range=PAGE_RANGE,
    ))

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=os.environ["PORT"])