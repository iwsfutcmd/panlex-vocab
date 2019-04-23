const deLangPicker = document.getElementById("de-lang-picker");
deLangPicker.addEventListener("language-select", e => {
  let alLang = location.pathname.replace(/\/$/, "").slice(-7);
  location = `../../${e.currentTarget.dataset["uid"]}/${alLang}`;
});
const alLangPicker = document.getElementById("al-lang-picker");
alLangPicker.addEventListener("language-select", e => {
  let deLang = location.pathname.replace(/\/$/, "").slice(-15, -8);
  location = `../../${deLang}/${e.currentTarget.dataset["uid"]}`;
});