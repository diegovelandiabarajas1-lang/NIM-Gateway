import test from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { createGateway } from "../gateway.js";

// ---------- servidor falso que imita a NVIDIA ----------
const calls = {};
const seen = {};
function chunk(res, delta, finish = null, extra = {}) {
  res.write(`data: ${JSON.stringify({ id: "x", object: "chat.completion.chunk", choices: [{ index: 0, delta, finish_reason: finish }], ...extra })}\n\n`);
}
const mock = http.createServer(async (req, res) => {
  let raw = ""; for await (const c of req) raw += c;
  const body = raw ? JSON.parse(raw) : {};
  const m = body.model;
  calls[m] = (calls[m] || 0) + 1;
  seen[m] = { body, auth: req.headers.authorization };
  const stream = body.stream === true;
  const ok = () => {
    if (!stream) {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ choices: [{ message: { role: "assistant", content: "<think>x</think>Hola", reasoning: "r" }, finish_reason: "stop" }], usage: { prompt_tokens: 5, completion_tokens: 3 } }));
      return;
    }
    res.writeHead(200, { "content-type": "text/event-stream" });
    chunk(res, { reasoning: "pienso " });
    chunk(res, { content: "<think>otra " });
    chunk(res, { content: "idea</think>Hola " });
    chunk(res, { content: "mundo" });
    chunk(res, {}, "stop", { usage: { prompt_tokens: 5, completion_tokens: 3 } });
    res.write("data: [DONE]\n\n"); res.end();
  };
  switch (m) {
    case "ok": return ok();
    case "hang": return; // nunca responde
    case "late": return void setTimeout(ok, 1500);
    case "slow": return void setTimeout(ok, 300);
    case "once429":
      if (calls[m] === 1) { res.writeHead(429, { "retry-after": "1", "content-type": "application/json" }); return res.end('{"error":"rate"}'); }
      return ok();
    case "always500": res.writeHead(500, { "content-type": "application/json" }); return res.end('{"error":"boom"}');
    case "nope": res.writeHead(403, { "content-type": "application/json" }); return res.end('{"error":"sin acceso"}');
    case "bad": res.writeHead(400, { "content-type": "application/json" }); return res.end('{"error":"peticion mala"}');
    case "stall":
      res.writeHead(200, { "content-type": "text/event-stream" });
      chunk(res, { content: "parte1 " }); chunk(res, { content: "parte2 " });
      return; // se queda colgado a mitad
    case "echo":
      if (!stream) { res.writeHead(200, { "content-type": "application/json" }); return res.end(JSON.stringify({ choices: [{ message: { content: "eco" }, finish_reason: "stop" }] })); }
      return ok();
    default: res.writeHead(404, { "content-type": "application/json" }); return res.end('{"error":"modelo desconocido"}');
  }
});

let gw, base;
const start = (cfg) => new Promise((resolve) => {
  const s = createGateway({ upstream: `http://127.0.0.1:${mock.address().port}/v1`, logFile: null, firstByteTimeoutMs: 500, idleTimeoutMs: 600, retryAfterCapMs: 200, retries: 1, ...cfg });
  s.listen(0, "127.0.0.1", () => resolve(s));
});
const ask = async (model, { stream = true, headers = {}, extra = {} } = {}) => {
  const r = await fetch(`${base}/chat/completions`, { method: "POST", headers: { "content-type": "application/json", authorization: "Bearer clave-de-prueba", ...headers }, body: JSON.stringify({ model, stream, messages: [{ role: "user", content: "hola" }], ...extra }) });
  return r;
};
const readSSE = async (r) => {
  const text = await r.text();
  let content = "", reasoning = "", finish = null, error = null, done = false;
  for (const ev of text.split("\n\n")) {
    const line = ev.split("\n").find((l) => l.startsWith("data:")); if (!line) continue;
    const d = line.slice(5).trim(); if (d === "[DONE]") { done = true; continue; }
    const j = JSON.parse(d);
    if (j.error) error = j.error;
    for (const c of j.choices || []) { content += c.delta?.content ?? ""; reasoning += c.delta?.reasoning_content ?? ""; if (c.delta?.reasoning) reasoning += "!!reasoning-sin-normalizar"; if (c.finish_reason) finish = c.finish_reason; }
  }
  return { content, reasoning, finish, error, done };
};

test.before(async () => {
  await new Promise((r) => mock.listen(0, "127.0.0.1", r));
  gw = await start({
    fallbacks: { hang: ["ok"], late: ["ok"], nope: ["ok"], always500: [], stall: [] },
    models: { echo: { patch: { foo: "bar" }, defaultMaxTokens: 777 } },
  });
  base = `http://127.0.0.1:${gw.address().port}/v1`;
});
test.after(() => { gw.close(); mock.close(); });

test("stream: normaliza 'reasoning' y las etiquetas <think> a reasoning_content", async () => {
  const r = await ask("ok");
  assert.equal(r.status, 200);
  assert.equal(r.headers.get("x-gateway-model"), "ok");
  const o = await readSSE(r);
  assert.equal(o.content, "Hola mundo");
  assert.equal(o.reasoning, "pienso otra idea");
  assert.equal(o.finish, "stop");
  assert.ok(o.done);
});

test("sin stream: normaliza el mensaje completo", async () => {
  const r = await ask("ok", { stream: false });
  const j = await r.json();
  assert.equal(j.choices[0].message.content, "Hola");
  assert.equal(j.choices[0].message.reasoning_content, "rx");
  assert.equal(j.choices[0].message.reasoning, undefined);
});

test("un modelo colgado pasa solo al siguiente (fallback)", async () => {
  const t0 = Date.now();
  const r = await ask("hang");
  assert.equal(r.status, 200);
  assert.equal(r.headers.get("x-gateway-model"), "ok");
  assert.equal(r.headers.get("x-gateway-attempts"), "2");
  assert.ok((await readSSE(r)).content === "Hola mundo");
  assert.ok(Date.now() - t0 < 2500, "tardo demasiado");
});

test("un modelo que tarda mas que el primer byte se sustituye", async () => {
  const r = await ask("late");
  assert.equal(r.headers.get("x-gateway-model"), "ok");
});

test("429: espera Retry-After y reintenta el mismo modelo", async () => {
  calls.once429 = 0;
  const r = await ask("once429");
  assert.equal(r.status, 200);
  assert.equal(r.headers.get("x-gateway-model"), "once429");
  assert.equal(calls.once429, 2);
});

test("500 constante sin fallback: 502 con el detalle de los intentos", async () => {
  calls.always500 = 0;
  const r = await ask("always500", { stream: false });
  assert.equal(r.status, 502);
  const j = await r.json();
  assert.equal(j.error.type, "gateway_error");
  assert.equal(j.error.attempts.length, 2);
  assert.equal(calls.always500, 2);
});

test("403/404: modelo sin acceso -> usa el siguiente de la cadena", async () => {
  const r = await ask("nope");
  assert.equal(r.headers.get("x-gateway-model"), "ok");
});

test("400: error del cliente se devuelve tal cual, sin reintentos", async () => {
  calls.bad = 0;
  const r = await ask("bad", { stream: false });
  assert.equal(r.status, 400);
  assert.match(await r.text(), /peticion mala/);
  assert.equal(calls.bad, 1);
});

test("corte a mitad de respuesta: error claro en el stream, sin colgarse", async () => {
  const t0 = Date.now();
  const r = await ask("stall");
  const o = await readSSE(r);
  assert.equal(o.content, "parte1 parte2 ");
  assert.equal(o.error?.type, "gateway_stream_interrupted");
  assert.ok(o.done);
  assert.ok(Date.now() - t0 < 3000);
});

test("max_tokens por defecto, patch por modelo y reenvio de la clave", async () => {
  await ask("echo", { stream: false });
  assert.equal(seen.echo.body.max_tokens, 777);
  assert.equal(seen.echo.body.foo, "bar");
  assert.equal(seen.echo.auth, "Bearer clave-de-prueba");
  await ask("echo", { stream: false, extra: { max_tokens: 5 } });
  assert.equal(seen.echo.body.max_tokens, 5);
});

test("cola: con maxConcurrent=1 las peticiones no se solapan", async () => {
  const g = await start({ maxConcurrent: 1, firstByteTimeoutMs: 2000, idleTimeoutMs: 2000 });
  const b = `http://127.0.0.1:${g.address().port}/v1`;
  const t0 = Date.now();
  const f = () => fetch(`${b}/chat/completions`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ model: "slow", stream: true, messages: [] }) }).then((r) => r.text());
  await Promise.all([f(), f()]);
  assert.ok(Date.now() - t0 >= 590, `se solaparon (${Date.now() - t0} ms)`);
  g.close();
});

test("/health y /stats responden", async () => {
  const root = base.replace("/v1", "");
  assert.equal((await (await fetch(`${root}/health`)).json()).ok, true);
  const st = await (await fetch(`${root}/stats`)).json();
  assert.ok(st.models.hang.fallbacks >= 1);
});
