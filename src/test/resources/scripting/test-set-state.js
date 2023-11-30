function process_row(r, state) {
  state.put("mykey", "myvalue");
}

function process_ddl(ddl, state) {
  var num = parseInt(state.get("number"));
  state.put("number", Number(num + 1).toFixed(0));
}