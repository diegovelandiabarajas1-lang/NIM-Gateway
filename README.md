# Pasarela NIM

Programa pequeño (Node, sin dependencias) que se coloca entre OpenCode y la API de NVIDIA NIM
(`https://integrate.api.nvidia.com/v1`) y evita que los modelos "no respondan" o "se corten".

```
OpenCode (Docker) --> pasarela (PC, :8780) --> NVIDIA NIM
```

No guarda ninguna clave: reenvia la cabecera `Authorization` que manda OpenCode.

## Que hace
- **Tiempos maximos:** si un modelo no da ni un fragmento en `firstByteTimeoutMs`, o se queda mudo `idleTimeoutMs` en mitad de la respuesta, corta.
- **Cambio de modelo:** si un modelo se cuelga o no esta disponible (403/404), pasa solo al siguiente de `fallbacks`. Con 429/5xx reintenta el mismo (respeta `Retry-After`).
- **Estimacion calibrada:** la estimacion base (caracteres/4) se corrige con los tokens reales que NVIDIA devuelve en cada respuesta (`usage.prompt_tokens`), por modelo, y lo aprendido se guarda en `logs/calibration.json`. Con datos reales, los tokens son ~1,23 veces la estimacion base, y la pasarela lo aprende sola.
- **Contexto excedido:** si la conversacion no cabe en ningun modelo conocido ni sin margen de seguridad, responde al instante `400 context_length_exceeded` (formato estandar de OpenAI) sin enviar nada a NVIDIA, para que el cliente (p. ej. OpenCode) compacte la conversacion. Se desactiva con `"rejectWhenTooLarge": false`. No compacta ella misma: reescribir la conversacion es cosa del cliente.
- **Enrutamiento por tamano de contexto:** estima los tokens de la peticion (mensajes + herramientas, con margen) y solo usa modelos cuya ventana de contexto la admite. Si el modelo pedido no cabe, pasa a uno que si (primero sus alternativas, despues `largeContext`). Las alternativas demasiado pequenas se saltan, sin perder tiempo en ellas. La respuesta lleva `x-gateway-model` y, si hubo desvio, `x-gateway-routed: contexto`. Los limites salen de `models.json` (se actualiza con `node update-models.mjs`, que lee models.dev).
- **Limite de salida por modelo:** `max_tokens` nunca supera el maximo del modelo (si no, NVIDIA responde 422); el valor por defecto tambien lo respeta.
- **Cola:** limita las peticiones simultaneas hacia NVIDIA (`maxConcurrent`) para no provocar 429.
- **Limite de salida:** si la peticion no trae `max_tokens`, pone `defaultMaxTokens` (evita llamadas a herramientas cortadas por un tope bajo).
- **Razonamiento:** convierte `reasoning` y las etiquetas `<think>...</think>` en `reasoning_content`, que es lo que OpenCode entiende.
- **Panel en vivo:** abre `http://localhost:8780/`. Cada fila muestra lo que el cliente **pidio** frente a lo que **respondio de verdad**, con el motivo cuando cambia (fallo del modelo o tamano de contexto), intentos, tiempos y tokens. OpenCode solo muestra el modelo que elegiste: este panel es la fuente de verdad.
- **Aviso en la respuesta:** si responde un modelo distinto al pedido, la pasarela antepone a la respuesta `[Pasarela: respondio X en lugar de Y (motivo)]`, para que lo veas dentro de OpenCode. Se desactiva con `"announceSwitch": false`.
- **Estadisticas:** `GET /stats` (admite `?n=` hasta 200 peticiones recientes, con `switchReason`) (por modelo: peticiones, errores, cambios de modelo, mediana del primer fragmento) y registro en `logs/gateway.jsonl`. Cuando NVIDIA rechaza una peticion (400, 422...) el motivo queda en el campo `error`.

## Uso
```bash
node gateway.js          # o desde iniciar-opencode.bat
```
Y en el `opencode.json` del proyecto:
```json
{
  "providers": {
    "nvidia": { "settings": { "baseURL": "http://host.docker.internal:8780/v1" } }
  }
}
```
(`127.0.0.1:8780` si OpenCode no corre en Docker). OpenCode lee esto al arrancar: reinicia su contenedor tras cambiarlo.

## Configuracion (`config.json`)
| Clave | Por defecto | Para que |
|---|---|---|
| `port` / `host` | 8780 / 0.0.0.0 | Donde escucha |
| `firstByteTimeoutMs` | 45000 | Espera maxima hasta el primer fragmento |
| `firstByteMsPer10kTokens` / `firstByteCapMs` | 1000 / 120000 | La espera del primer fragmento crece con el tamano de la conversacion (+1 s por cada 10k tokens estimados, con ese tope): las conversaciones largas tardan mas en empezar |
| `announceSwitch` | true | Anteponer a la respuesta un aviso cuando responde un modelo distinto al pedido |
| `routeBySize` | true | Elegir modelo segun el tamano de la conversacion |
| `largeContext` | [] | Modelos de contexto grande, por orden de preferencia, a los que desviar si el pedido no cabe |
| `contextMargin` / `outputReserve` | 1.05 / 4096 | Margen de seguridad sobre la estimacion ya calibrada y tokens reservados para la respuesta |
| `initialTokenRatio` | 1.25 | Tokens reales / estimacion base antes de aprender (se corrige solo con cada respuesta) |
| `rejectWhenTooLarge` | true | Devolver `context_length_exceeded` al instante si no cabe en ningun modelo conocido |
| `calibrationFile` | `logs/calibration.json` | Donde se guarda lo aprendido entre reinicios (`null` = no guardar) |
| `contextWindows` / `outputLimits` | {} | `{ "modelo": tokens }` para sobrescribir `models.json` |
| `allowEnvKey` | false | Usar `NVIDIA_API_KEY` cuando la peticion no trae clave (solo si la pasarela no es accesible desde la red) |
| `idleTimeoutMs` | 90000 | Silencio maximo durante el stream |
| `nonStreamTimeoutMs` | 180000 | Respuesta completa sin stream |
| `retries` | 2 | Reintentos por modelo (429, 5xx, red) |
| `maxConcurrent` | 3 | Peticiones simultaneas |
| `defaultMaxTokens` | 16384 | Si falta `max_tokens` |
| `normalizeReasoning` | true | Limpieza del razonamiento |
| `fallbacks` | {} | `{ "modelo": ["alternativa1", ...] }` con los ids que ve NVIDIA |
| `models` | {} | Por modelo: `firstByteTimeoutMs`, `defaultMaxTokens`, `patch` (se mezcla en el cuerpo, p. ej. para desactivar el pensamiento si el modelo lo admite) |

Los ids son los de NVIDIA, sin el prefijo del proveedor de OpenCode: `nvidia/nemotron-3-ultra-550b-a55b`, `openai/gpt-oss-20b`...

## Limites
- Si un modelo se corta **despues** de haber empezado a escribir, no se puede cambiar de modelo a mitad: la pasarela cierra el stream con un error claro (`gateway_stream_interrupted`) para que OpenCode lo reintente.
- Un modelo que "piensa" mucho antes del primer fragmento puede superar `firstByteTimeoutMs`; ajustalo por modelo en `models`.

## Pruebas
```bash
node --test test/gateway.test.js
```
Usa un servidor falso que imita los fallos (cuelgue, 429, 500, 403, corte a mitad, etiquetas `<think>`).

## Seguridad
- La pasarela **no guarda ninguna clave**: reenvia la cabecera `Authorization` que manda OpenCode. Si la peticion no trae clave, **no usa la del entorno**: como la pasarela escucha en la red, hacerlo dejaria que cualquiera que llegue al puerto gaste tu clave. Solo si la pasarela NO es accesible desde la red puedes activar `"allowEnvKey": true` para que use `NVIDIA_API_KEY`.
- Escucha en `0.0.0.0` para que un contenedor Docker llegue a ella; no tiene autenticacion propia, asi que no la expongas a internet. Para uso solo local, pon `"host": "127.0.0.1"` en `config.json`.
- `logs/gateway.jsonl` guarda modelo, tiempos, tokens y errores, pero nunca el contenido de los mensajes ni las claves.

## Licencia
[Apache License 2.0](LICENSE). Copyright 2026 Diego Velandia Barajas.
