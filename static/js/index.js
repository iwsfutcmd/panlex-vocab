const deLangPicker = document.getElementById("de-lang-picker");
deLangPicker.addEventListener("language-select", e => {
  let deLang = location.pathname.replace(/\/$/, "").slice(-15, -8);
  location = location.href.replace(deLang, e.currentTarget.dataset["uid"]);
});
const alLangPicker = document.getElementById("al-lang-picker");
alLangPicker.addEventListener("language-select", e => {
  let alLang = location.pathname.replace(/\/$/, "").slice(-7);
  location = location.href.replace(alLang, e.currentTarget.dataset["uid"]);
});