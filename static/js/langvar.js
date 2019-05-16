const langPicker = document.getElementById("lang-picker");
langPicker.addEventListener("language-select", e => {
  let uid = e.currentTarget.dataset["uid"];
  // location.pathname = location.pathname.replace(/(\/\w{3}-\d{3}){1,2}\/?$/, "/" + uid);
  location.href = uid;
});
