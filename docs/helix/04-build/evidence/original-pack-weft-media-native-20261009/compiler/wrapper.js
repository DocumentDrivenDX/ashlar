// packages/weft-browser/src/index.ts
var fatalResponse = '{"diagnostics":[{"code":"WFT-BACKEND-FAILURE","message":"Compiler host trapped; initialize a fresh instance","phase":"host","recoverability":"host-action","severity":"error"}],"interfaceVersion":"weft-compile/0.1.0","status":"blocked"}';
function scalarText(value) {
  if (typeof value !== "string")
    throw new TypeError("compileJson requires a UTF-8 scalar string");
  for (let i = 0;i < value.length; i++) {
    const c = value.charCodeAt(i);
    if (c >= 55296 && c <= 56319) {
      const next = value.charCodeAt(++i);
      if (!(next >= 56320 && next <= 57343))
        throw new TypeError("Unpaired UTF-16 surrogate");
    } else if (c >= 56320 && c <= 57343)
      throw new TypeError("Unpaired UTF-16 surrogate");
  }
}
function bindCompiler(api) {
  let poisoned = false;
  return Object.freeze({ compileJson(requestJson) {
    scalarText(requestJson);
    if (poisoned)
      return fatalResponse;
    try {
      return api.compile_json(requestJson);
    } catch (error) {
      if (!(error instanceof WebAssembly.RuntimeError))
        throw error;
      poisoned = true;
      return fatalResponse;
    }
  } });
}
export {
  bindCompiler
};
