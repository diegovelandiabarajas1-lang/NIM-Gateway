// Descarga de models.dev el contexto y la salida maximos de los modelos de NVIDIA y los guarda en models.json.
// Uso: node update-models.mjs        (la pasarela lo lee al arrancar)
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const res = await fetch("https://models.dev/api.json");
if (!res.ok) { console.error("No se pudo descargar models.dev:", res.status); process.exit(1); }
const nv = (await res.json()).nvidia?.models ?? {};
const table = {};
for (const [id, m] of Object.entries(nv)) {
  const context = m.limit?.context ?? 0, output = m.limit?.output ?? 0;
  if (context > 0) table[id] = { context, ...(output > 0 ? { output } : {}) };
}
fs.writeFileSync(path.join(here, "models.json"), JSON.stringify(table, null, 2) + "\n");
console.log(`models.json actualizado: ${Object.keys(table).length} modelos`);
