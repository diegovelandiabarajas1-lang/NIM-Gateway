// Pasarela para NVIDIA NIM (API compatible con OpenAI).
// OpenCode  -->  esta pasarela  -->  https://integrate.api.nvidia.com/v1
// Anade: tiempos maximos, reintentos, cambio automatico a otro modelo si uno se cuelga,
// cola de peticiones simultaneas, limite de salida por defecto, limpieza del razonamiento y estadisticas.
// No guarda ninguna clave: reenvia la cabecera Authorization que manda OpenCode.
import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));

export const DEFAULTS = {
  port: 8780,
  host: "0.0.0.0",
  upstream: "https://integrate.api.nvidia.com/v1",
  firstByteTimeoutMs: 45000, // tiempo maximo hasta el primer fragmento de respuesta
  idleTimeoutMs: 90000, // tiempo maximo sin fragmentos durante el stream
  nonStreamTimeoutMs: 180000, // respuesta completa cuando no hay stream
  retries: 2, // reintentos por modelo ante 429 / 5xx / red
  retryAfterCapMs: 15000,
  firstByteMsPer10kTokens: 1000, // espera extra por cada 10k tokens estimados de entrada (conversaciones largas tardan mas en empezar)
  firstByteCapMs: 120000, // tope de la espera ampliada
  allowEnvKey: false, // true = si la peticion no trae clave, usar la variable NVIDIA_API_KEY (solo si la pasarela NO es accesible desde la red)
  routeBySize: true, // elegir modelo segun el tamano de la conversacion
  largeContext: [], // modelos de contexto grande, por orden de preferencia, a los que desviar si el pedido no cabe
  contextMargin: 1.05, // margen sobre la estimacion ya calibrada
  initialTokenRatio: 1.25, // tokens reales / (caracteres/4) antes de aprender; se corrige con cada respuesta (usage.prompt_tokens)
  rejectWhenTooLarge: true, // si la conversacion no cabe en ningun modelo conocido: devolver context_length_exceeded al instante (OpenCode la compacta)
  calibrationFile: path.join(here, "logs", "calibration.json"), // donde se guarda lo aprendido entre reinicios (null = no guardar)
  outputReserve: 4096, // tokens que se reservan para la respuesta al comprobar que cabe
  contextWindows: {}, // { "modelo": tokens } sobrescribe models.json
  outputLimits: {}, // { "modelo": tokens } sobrescribe models.json
  modelsTable: {}, // se carga de models.json: { "modelo": { context, output } }
  maxConcurrent: 3, // peticiones simultaneas hacia NVIDIA
  queueTimeoutMs: 120000,
  defaultMaxTokens: 16384, // si la peticion no trae max_tokens
  normalizeReasoning: true,
  fallbacks: {}, // { "nvidia/modelo-a": ["nvidia/modelo-b", ...] }
  models: {}, // { "nvidia/modelo-a": { patch: {...}, firstByteTimeoutMs, defaultMaxTokens } }
  logFile: path.join(here, "logs", "gateway.jsonl"),
};

export function loadConfig(file = path.join(here, "config.json")) {
  let user = {}, table = {};
  try { user = JSON.parse(fs.readFileSync(file, "utf8")); } catch { /* sin config: valores por defecto */ }
  // models.json lo genera update-models.mjs a partir de models.dev (contexto y salida maximos de cada modelo)
  try { table = JSON.parse(fs.readFileSync(path.join(path.dirname(file), "models.json"), "utf8")); } catch { /* sin tabla: no se filtra por tamano */ }
  return { ...DEFAULTS, modelsTable: table, ...user };
}

// ---------- razonamiento ----------
function splitThink(st, text) {
  let content = "", reasoning = "", s = st.buf + text;
  st.buf = "";
  while (s.length) {
    const tag = st.inThink ? "</think>" : "<think>";
    const i = s.indexOf(tag);
    if (i >= 0) {
      const part = s.slice(0, i);
      if (st.inThink) reasoning += part; else content += part;
      s = s.slice(i + tag.length);
      st.inThink = !st.inThink;
      continue;
    }
    let keep = 0;
    for (let k = Math.min(tag.length - 1, s.length); k > 0; k--) if (tag.startsWith(s.slice(-k))) { keep = k; break; }
    const part = s.slice(0, s.length - keep);
    if (st.inThink) reasoning += part; else content += part;
    st.buf = s.slice(s.length - keep);
    break;
  }
  return { content, reasoning };
}

function normalizeDelta(delta, st) {
  if (!delta || typeof delta !== "object") return;
  if (typeof delta.reasoning === "string") {
    delta.reasoning_content = (delta.reasoning_content ?? "") + delta.reasoning;
    delete delta.reasoning;
  }
  if (typeof delta.content === "string" && (st.inThink || st.buf || delta.content.includes("<"))) {
    const { content, reasoning } = splitThink(st, delta.content);
    delta.content = content;
    if (reasoning) delta.reasoning_content = (delta.reasoning_content ?? "") + reasoning;
  }
}

function normalizeMessage(msg) {
  if (!msg || typeof msg !== "object") return;
  if (typeof msg.reasoning === "string" && msg.reasoning_content === undefined) { msg.reasoning_content = msg.reasoning; delete msg.reasoning; }
  if (typeof msg.content === "string" && msg.content.includes("<think>")) {
    const st = { inThink: false, buf: "" };
    const { content, reasoning } = splitThink(st, msg.content);
    msg.content = (content + (st.inThink ? "" : st.buf)).trim();
    if (reasoning || st.buf) msg.reasoning_content = ((msg.reasoning_content ?? "") + reasoning + (st.inThink ? st.buf : "")).trim();
  }
}

// ---------- utilidades ----------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const sseWrite = (res, obj) => res.write(`data: ${typeof obj === "string" ? obj : JSON.stringify(obj)}\n\n`);

class Semaphore {
  constructor(n) { this.n = n; this.q = []; }
  async acquire(timeoutMs) {
    if (this.n > 0) { this.n--; return true; }
    return new Promise((resolve) => {
      const item = { resolve };
      item.timer = setTimeout(() => { this.q = this.q.filter((x) => x !== item); resolve(false); }, timeoutMs);
      this.q.push(item);
    });
  }
  release() {
    const it = this.q.shift();
    if (it) { clearTimeout(it.timer); it.resolve(true); } else this.n++;
  }
}

export function createGateway(cfgIn = {}) {
  const cfg = { ...DEFAULTS, ...cfgIn };
  const sem = new Semaphore(cfg.maxConcurrent);
  const recent = [];
  const agg = {};

  const record = (r) => {
    recent.push(r); if (recent.length > 200) recent.shift();
    const a = (agg[r.model] ??= { requests: 0, ok: 0, errors: 0, fallbacks: 0, ttfb: [] });
    a.requests++; r.ok ? a.ok++ : a.errors++;
    if (r.finalModel !== r.model) a.fallbacks++;
    if (r.ttfbMs != null) { a.ttfb.push(r.ttfbMs); if (a.ttfb.length > 100) a.ttfb.shift(); }
    if (cfg.logFile) { try { fs.mkdirSync(path.dirname(cfg.logFile), { recursive: true }); fs.appendFileSync(cfg.logFile, JSON.stringify(r) + "\n"); } catch { /* sin log */ } }
  };
  const median = (xs) => (xs.length ? [...xs].sort((x, y) => x - y)[Math.floor(xs.length / 2)] : null);

  const mcfg = (model) => ({
    firstByteTimeoutMs: cfg.firstByteTimeoutMs,
    defaultMaxTokens: cfg.defaultMaxTokens,
    ...(cfg.models[model] || {}),
  });

  // contexto y salida maximos del modelo (Infinity / null si no se conocen: entonces no se filtra ni se recorta)
  const windowOf = (id) => cfg.contextWindows[id] ?? cfg.modelsTable[id]?.context ?? Infinity;
  const outputOf = (id) => cfg.outputLimits[id] ?? cfg.modelsTable[id]?.output ?? null;
  // ---- estimacion de tokens, calibrada con los tokens reales que devuelve NVIDIA ----
  // estimacion base = caracteres/4; "ratio" = tokens reales / estimacion base, aprendido por modelo.
  const ratios = {};
  let globalRatio = cfg.initialTokenRatio;
  if (cfg.calibrationFile) {
    try { const c = JSON.parse(fs.readFileSync(cfg.calibrationFile, "utf8")); Object.assign(ratios, c.ratios || {}); if (c.global) globalRatio = c.global; } catch { /* sin calibracion previa */ }
  }
  const ratioOf = (m) => ratios[m] ?? globalRatio;
  function learn(model, est, usage) {
    if (!usage?.prompt_tokens || est < 4000) return; // con poco texto el ruido de la plantilla distorsiona
    const r = Math.min(3, Math.max(0.5, usage.prompt_tokens / est));
    ratios[model] = ratios[model] == null ? r : 0.7 * ratios[model] + 0.3 * r;
    globalRatio = 0.9 * globalRatio + 0.1 * r;
    if (cfg.calibrationFile) { try { fs.mkdirSync(path.dirname(cfg.calibrationFile), { recursive: true }); fs.writeFileSync(cfg.calibrationFile, JSON.stringify({ global: globalRatio, ratios }, null, 2)); } catch { /* */ } }
  }
  // estimacion de la entrada (sin ratio) y salida reservada del pedido
  function baseEstimate(body) {
    const chars = JSON.stringify(body.messages ?? "").length + (body.tools ? JSON.stringify(body.tools).length : 0);
    const asked = body.max_tokens ?? body.max_completion_tokens ?? cfg.outputReserve;
    return { est: Math.round(chars / 4), reserve: Math.min(asked, cfg.outputReserve) };
  }
  // tokens que necesitaria el pedido en el modelo m (margen = 1 -> sin margen de seguridad)
  const needFor = (b, m, margin) => Math.ceil(b.est * ratioOf(m) * margin) + b.reserve;

  function buildBody(body, model) {
    const m = mcfg(model);
    const lim = outputOf(model);
    const out = { ...body, model };
    // la salida pedida nunca puede pasar del maximo del modelo (si no, NVIDIA responde 422)
    for (const k of ["max_tokens", "max_completion_tokens"]) if (lim && out[k] != null && out[k] > lim) out[k] = lim;
    if (out.max_tokens == null && out.max_completion_tokens == null && m.defaultMaxTokens) out.max_tokens = lim ? Math.min(m.defaultMaxTokens, lim) : m.defaultMaxTokens;
    return m.patch ? { ...out, ...m.patch } : out;
  }

  // Un intento contra un modelo. Devuelve {done:true} si ya respondio al cliente, o {fail, retriable, ...}.
  async function tryOnce({ req, res, auth, body, model, wantStream, t0, state }) {
    const m = mcfg(model);
    const ac = new AbortController();
    let reason = null, timer;
    const arm = (ms, why) => { clearTimeout(timer); if (ms > 0) timer = setTimeout(() => { reason = why; ac.abort(); }, ms); };
    state.abort = () => { reason = "cliente"; ac.abort(); };
    const headers = { "content-type": "application/json", accept: wantStream ? "text/event-stream" : "application/json" };
    if (auth) headers.authorization = auth;

    // La espera hasta el primer fragmento crece con el tamano de la conversacion (tokens reales esperados).
    const estTokens = state.estReal ?? Math.round(JSON.stringify(body.messages ?? "").length / 4);
    const firstByteMs = Math.min(m.firstByteTimeoutMs + Math.floor(estTokens / 10000) * (cfg.firstByteMsPer10kTokens || 0), Math.max(cfg.firstByteCapMs, m.firstByteTimeoutMs));
    state.estTokens = estTokens;
    arm(wantStream ? firstByteMs : cfg.nonStreamTimeoutMs, wantStream ? `sin respuesta en ${Math.round(firstByteMs / 1000)} s` : `sin respuesta en ${Math.round(cfg.nonStreamTimeoutMs / 1000)} s`);
    let up;
    try {
      up = await fetch(`${cfg.upstream}/chat/completions`, { method: "POST", headers, body: JSON.stringify(buildBody(body, model)), signal: ac.signal });
    } catch (e) {
      clearTimeout(timer);
      if (reason === "cliente") return { done: true, aborted: true };
      return { fail: reason || `red: ${e.cause?.code || e.message}`, retriable: true, timeout: !!reason };
    }
    if (!up.ok) {
      let text = ""; try { text = await up.text(); } catch { /* */ }
      clearTimeout(timer);
      const ra = Number(up.headers.get("retry-after"));
      return { fail: `HTTP ${up.status}`, status: up.status, bodyText: text, contentType: up.headers.get("content-type"), retryAfterMs: Number.isFinite(ra) && ra > 0 ? ra * 1000 : null, retriable: up.status === 429 || up.status >= 500, unavailable: up.status === 403 || up.status === 404 };
    }

    if (!wantStream) {
      let text;
      try { text = await up.text(); } catch (e) { clearTimeout(timer); return reason === "cliente" ? { done: true, aborted: true } : { fail: reason || "respuesta cortada", retriable: true, timeout: !!reason }; }
      clearTimeout(timer);
      let json; try { json = JSON.parse(text); } catch { return { fail: "respuesta no es JSON", retriable: true }; }
      if (cfg.normalizeReasoning) for (const c of json.choices || []) normalizeMessage(c.message);
      state.ttfbMs = Date.now() - t0; state.finish = json.choices?.[0]?.finish_reason; state.usage = json.usage;
      res.writeHead(200, { "content-type": "application/json", "x-gateway-model": model, "x-gateway-attempts": String(state.attempts.length + 1), ...(state.routedBy ? { "x-gateway-routed": state.routedBy } : {}) });
      res.end(JSON.stringify(json));
      return { done: true };
    }

    // ---- stream ----
    const reader = up.body.getReader();
    const dec = new TextDecoder();
    let buf = "", committed = false;
    const tstate = { inThink: false, buf: "" };
    const commit = () => {
      if (committed) return;
      committed = true;
      state.ttfbMs = Date.now() - t0;
      res.writeHead(200, { "content-type": "text/event-stream; charset=utf-8", "cache-control": "no-cache", connection: "keep-alive", "x-gateway-model": model, "x-gateway-attempts": String(state.attempts.length + 1), ...(state.routedBy ? { "x-gateway-routed": state.routedBy } : {}) });
    };
    const processEvent = (ev) => {
      const line = ev.split("\n").find((l) => l.startsWith("data:"));
      if (!line) { commit(); res.write(ev + "\n\n"); return; }
      const data = line.slice(5).trim();
      commit();
      if (data === "[DONE]") { res.write("data: [DONE]\n\n"); return; }
      let j; try { j = JSON.parse(data); } catch { res.write(ev + "\n\n"); return; }
      for (const c of j.choices || []) {
        if (cfg.normalizeReasoning) normalizeDelta(c.delta, tstate);
        if (c.finish_reason) state.finish = c.finish_reason;
      }
      if (j.usage) state.usage = j.usage;
      sseWrite(res, j);
    };
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        arm(cfg.idleTimeoutMs, `sin datos durante ${Math.round(cfg.idleTimeoutMs / 1000)} s`);
        buf += dec.decode(value, { stream: true }).replace(/\r\n/g, "\n");
        let i;
        while ((i = buf.indexOf("\n\n")) >= 0) { processEvent(buf.slice(0, i)); buf = buf.slice(i + 2); }
      }
      clearTimeout(timer);
      if (buf.trim()) processEvent(buf);
      if (!committed) return { fail: "respuesta vacia", retriable: true };
      res.end();
      return { done: true };
    } catch (e) {
      clearTimeout(timer);
      if (reason === "cliente") return { done: true, aborted: true };
      const why = reason || `stream cortado: ${e.message}`;
      if (!committed) return { fail: why, retriable: true, timeout: !!reason };
      // Ya se envio texto al cliente: no se puede cambiar de modelo a mitad; se cierra con un error claro.
      try { sseWrite(res, { error: { message: `Pasarela: ${why}. La respuesta quedo incompleta.`, type: "gateway_stream_interrupted" } }); sseWrite(res, "[DONE]"); res.end(); } catch { /* */ }
      state.error = why; state.interrupted = true;
      return { done: true };
    }
  }

  async function handleChat(req, res, raw) {
    let body;
    try { body = JSON.parse(raw); } catch { res.writeHead(400, { "content-type": "application/json" }); res.end(JSON.stringify({ error: { message: "JSON invalido", type: "invalid_request_error" } })); return; }
    const model = body.model;
    const wantStream = body.stream === true;
    // Solo se reenvia la clave del cliente. La clave del entorno exige activar allowEnvKey a proposito:
    // como la pasarela escucha en la red, usarla por defecto dejaria que cualquiera gaste tu clave.
    const auth = req.headers.authorization || (cfg.allowEnvKey && process.env.NVIDIA_API_KEY ? `Bearer ${process.env.NVIDIA_API_KEY}` : "");
    const t0 = Date.now();
    const state = { attempts: [], abort: null };

    // Cadena de modelos: el pedido, sus alternativas y, si hace falta, modelos de contexto grande.
    const uniq = (xs) => [...new Set(xs)];
    let chain = uniq([model, ...(cfg.fallbacks[model] || [])]);
    const be = baseEstimate(body);
    state.est = be.est; state.ratio = ratioOf(model);
    state.need = needFor(be, model, cfg.contextMargin);
    state.estReal = Math.round(be.est * ratioOf(model)); // tokens reales esperados (para la espera hasta el primer fragmento)
    if (cfg.routeBySize) {
      const fits = (m) => windowOf(m) >= needFor(be, m, cfg.contextMargin);
      const fitsBare = (m) => windowOf(m) >= needFor(be, m, 1); // sin margen: mas vale intentarlo que rechazar por poco
      if (!chain.some(fits)) chain = uniq([...chain, ...cfg.largeContext]);
      let ok = chain.filter(fits);
      if (!ok.length) ok = chain.filter(fitsBare);
      if (ok.length) { if (ok[0] !== model) state.routedBy = "contexto"; chain = ok; }
      else if (cfg.rejectWhenTooLarge && chain.every((m) => Number.isFinite(windowOf(m)))) {
        // No cabe ni sin margen en ningun modelo conocido: no tiene sentido enviar megabytes a NVIDIA para que falle.
        // Se devuelve el error estandar de "contexto excedido" para que el cliente (OpenCode) compacte la conversacion.
        const max = Math.max(...chain.map(windowOf));
        const msg = `This model's maximum context length is ${max} tokens. However, your messages resulted in about ${needFor(be, model, 1)} tokens. Please reduce the length of the messages.`;
        res.writeHead(400, { "content-type": "application/json", "x-gateway-rejected": "context_length_exceeded" });
        res.end(JSON.stringify({ error: { message: msg, type: "invalid_request_error", param: "messages", code: "context_length_exceeded" } }));
        record({ ts: new Date().toISOString(), model, finalModel: model, stream: wantStream, ok: false, status: 400, attempts: 0, est: be.est, ratio: state.ratio, need: state.need, routedBy: null, ttfbMs: null, totalMs: Date.now() - t0, finish: null, tokens: null, error: `context_length_exceeded: ~${needFor(be, model, 1)} tokens > ${max}`, clientGone: false });
        return;
      }
    }
    let clientGone = false;
    res.on("close", () => { if (!res.writableEnded) { clientGone = true; state.abort?.(); } });

    if (!(await sem.acquire(cfg.queueTimeoutMs))) {
      res.writeHead(503, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: { message: "Pasarela: cola llena, demasiadas peticiones simultaneas", type: "gateway_busy" } }));
      return;
    }
    let result = null, finalModel = model;
    try {
      outer: for (let mi = 0; mi < chain.length; mi++) {
        const m = chain[mi], hasFallback = mi < chain.length - 1;
        finalModel = m;
        for (let a = 0; a <= cfg.retries; a++) {
          if (clientGone) break outer;
          const r = await tryOnce({ req, res, auth, body, model: m, wantStream, t0, state });
          state.attempts.push({ model: m, result: r.done ? "ok" : r.fail, ms: Date.now() - t0, ...(r.bodyText ? { detail: r.bodyText.replace(/\s+/g, " ").slice(0, 300) } : {}) });
          if (r.done) { result = r; break outer; }
          // errores que no tiene sentido reintentar ni cambiar de modelo: se devuelven tal cual
          if (r.status && !r.retriable && !r.unavailable) {
            res.writeHead(r.status, { "content-type": r.contentType || "application/json" });
            res.end(r.bodyText);
            result = { done: true, passthrough: r.status };
            state.error = `HTTP ${r.status}: ${(r.bodyText || "").replace(/\s+/g, " ").slice(0, 300)}`; // motivo del rechazo de NVIDIA (sin claves ni mensajes del usuario)
            break outer;
          }
          if (r.unavailable) break; // 403/404: este modelo no esta disponible, pasa al siguiente
          if (r.timeout && hasFallback) break; // un modelo colgado: no insistir, cambiar
          if (a < cfg.retries) await sleep(Math.min(r.retryAfterMs ?? 1000 * 2 ** a, cfg.retryAfterCapMs));
        }
      }
    } finally { sem.release(); }

    const ok = !!result?.done && !result.passthrough && !state.interrupted;
    if (ok) learn(finalModel, state.est ?? 0, state.usage); // aprende cuantos tokens reales cuenta NVIDIA por cada estimado
    if (!result?.done && !clientGone) {
      const last = state.attempts.at(-1);
      res.writeHead(502, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: { message: `Pasarela: todos los intentos fallaron (${state.attempts.map((x) => `${x.model}: ${x.result}`).join("; ")})`, type: "gateway_error", attempts: state.attempts } }));
      state.error = last?.result;
    }
    record({ ts: new Date().toISOString(), model, finalModel, stream: wantStream, ok, status: result?.passthrough ?? (ok ? 200 : 502), attempts: state.attempts.length, est: state.est ?? null, ratio: state.ratio ? Math.round(state.ratio * 100) / 100 : null, need: state.need ?? null, routedBy: state.routedBy ?? null, ttfbMs: state.ttfbMs ?? null, totalMs: Date.now() - t0, finish: state.finish ?? null, tokens: state.usage ? { in: state.usage.prompt_tokens, out: state.usage.completion_tokens } : null, error: state.error ?? null, clientGone });
  }

  async function passthrough(req, res, raw) {
    const url = cfg.upstream + req.url.replace(/^\/v1/, "");
    const headers = {}; if (req.headers.authorization) headers.authorization = req.headers.authorization;
    if (req.headers["content-type"]) headers["content-type"] = req.headers["content-type"];
    try {
      const up = await fetch(url, { method: req.method, headers, body: ["GET", "HEAD"].includes(req.method) ? undefined : raw });
      res.writeHead(up.status, { "content-type": up.headers.get("content-type") || "application/json" });
      res.end(Buffer.from(await up.arrayBuffer()));
    } catch (e) { res.writeHead(502, { "content-type": "application/json" }); res.end(JSON.stringify({ error: { message: "Pasarela: " + e.message } })); }
  }

  const server = http.createServer(async (req, res) => {
    const p = req.url.split("?")[0];
    if (p === "/health") { res.writeHead(200, { "content-type": "application/json" }); res.end(JSON.stringify({ ok: true, upstream: cfg.upstream, queue: sem.q.length, free: sem.n })); return; }
    if (p === "/stats") {
      const models = Object.fromEntries(Object.entries(agg).map(([k, v]) => [k, { requests: v.requests, ok: v.ok, errors: v.errors, fallbacks: v.fallbacks, ttfbMedianMs: median(v.ttfb) }]));
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ models, recent: recent.slice(-20) }, null, 2));
      return;
    }
    let raw = "";
    for await (const c of req) { raw += c; if (raw.length > 64e6) { res.writeHead(413).end(); return; } }
    if (req.method === "POST" && p === "/v1/chat/completions") return handleChat(req, res, raw);
    if (p.startsWith("/v1/")) return passthrough(req, res, raw);
    res.writeHead(404, { "content-type": "application/json" }); res.end(JSON.stringify({ error: { message: "ruta desconocida" } }));
  });
  server.cfg = cfg;
  return server;
}

// ---------- ejecucion directa ----------
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const cfg = loadConfig();
  const server = createGateway(cfg);
  server.listen(cfg.port, cfg.host, () => {
    console.log(`Pasarela NIM en http://${cfg.host}:${cfg.port}/v1  ->  ${cfg.upstream}`);
    console.log(`Primer byte ${cfg.firstByteTimeoutMs / 1000}s | inactividad ${cfg.idleTimeoutMs / 1000}s | simultaneas ${cfg.maxConcurrent} | cambios de modelo: ${Object.keys(cfg.fallbacks).length}`);
  });
}
