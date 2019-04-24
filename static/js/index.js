const deLangPicker = document.getElementById("de-lang-picker");
const alLangPicker = document.getElementById("al-lang-picker");
deLangPicker.addEventListener("language-select", e => {
  let deUid = e.currentTarget.dataset["uid"];
  let alUid = alLangPicker.dataset["uid"];
  location.pathname = location.pathname.replace(/(\/\w{3}-\d{3}){1,2}\/?$/, "/" + deUid + "/" + alUid);
});
alLangPicker.addEventListener("language-select", e => {
  let deUid = deLangPicker.dataset["uid"];
  let alUid = e.currentTarget.dataset["uid"];
  location.pathname = location.pathname.replace(/(\/\w{3}-\d{3}){1,2}\/?$/, "/" + deUid + "/" + alUid);
});