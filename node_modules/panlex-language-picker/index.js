const VERSION = 2
const APISERVER = 'https://api.panlex.org'
const URLBASE = (VERSION === 2) ? APISERVER + '/v2' : APISERVER

function query(ep, params, get = false) {
  let url = new URL(URLBASE + ep);
  let headers = new Headers({
    'x-app-name': `panlex-language-picker/1.1.0`,
    'content-type': 'application/json',
  });
  return (fetch(url, {
    method: 'POST',
    headers,
    body: JSON.stringify(params),
  }).then((response) => response.json()));

}

class PanLexLanguagePicker extends HTMLElement {
  constructor() {
    super();
    this.innerHTML = `
    <style>
      panlex-language-picker {
        display: inline-block;
      }
      
      panlex-language-picker > ul {
        all: unset;
        display: block;
        list-style-type: none;
        position: absolute;
        background-color: white;
      }
      
      panlex-language-picker li > div {
        display: flex;
        flex-direction: row;
        justify-content: space-between;
      }
    </style>
    `
    this.input = document.createElement("input");
    this.input.type = "text";
    this.input.addEventListener("input", this.debouncedGetSuggestions.bind(this))
    this.appendChild(this.input);
    this.lngList = document.createElement("ul");
    this.appendChild(this.lngList);
  }

  debounce(func, delay) {
    return (args) => {
      let previousCall = this.lastCall;
      this.lastCall = Date.now();
      if (previousCall && ((this.lastCall - previousCall) <= delay)) {
        clearTimeout(this.lastCallTimer);
      }
      this.lastCallTimer = setTimeout(func, delay, args);
    }
  }

  getSuggestions(txt) {
    query("/suggest/langvar", { "txt": txt, "pref_trans_langvar": 187 }).then((response) => {
      if (response.suggest) {
        this.lngList.innerHTML = "";
        response.suggest.forEach(s => {
          let li = document.createElement("li");
          li.dataset.id = s.id;
          li.dataset.uid = s.uid;
          li.dataset.name = s.trans[0].txt;
          li.addEventListener("click", this.clickSuggestion.bind(this));
          li.innerHTML = `
          <div>
            <span>
              ${s.trans[0].txt}
            </span>
            <span>
              ${s.uid}
            </span>
          </div>
          <div>
            ${s.trans.slice(1).map(tran => tran.txt).join(' — ') || "&nbsp;"}
          </div>
          `;
          this.lngList.appendChild(li);
        })
      }
    });
  }

  debouncedGetSuggestions(e) {
    this.debounce(this.getSuggestions.bind(this), 500)(e.target.value);
  }

  clickSuggestion(e) {
    this.dataset["lv"] = e.currentTarget.dataset.id;
    this.dataset["uid"] = e.currentTarget.dataset.uid;
    this.input.value = e.currentTarget.dataset.name;
    this.lngList.innerHTML = "";
  }
}

window.customElements.define("panlex-language-picker", PanLexLanguagePicker);