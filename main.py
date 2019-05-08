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

@app.route(r"/<de_uid:[a-z]{3}-\d{3}>/<al_uid:[a-z]{3}-\d{3}>")
async def main(request, de_uid, al_uid=""):
    template = Template(open("vocab.jinja2").read())
    try:
        page = int(request.args["page"][0])
    except KeyError:
        page = 1

    de_lang = panlex_db.get_langvar(de_uid)
    if de_lang is None:
        de_lang = ""

    if al_uid == "":
        al_lang = ""
    else:
        al_lang = panlex_db.get_langvar(al_uid)
        if al_lang is None:
            al_lang = ""

    try:
        trn_list = panlex_db.get_translated_page(de_uid, al_uid, page)
    except (IndexError, AttributeError):
        trn_list = []
    return response.html(template.render(
        de_lang=de_lang,
        al_lang=al_lang,
        trn_list=trn_list,
        page=page,
        last_page=panlex_db.get_page_count(de_uid),
        page_range=PAGE_RANGE,
    ))

app.add_route(main, r"/<de_uid:[a-z]{3}-\d{3}>")

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=os.environ["PORT"])
