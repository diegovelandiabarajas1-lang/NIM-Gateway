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
- **Cola:** limita las peticiones simultaneas hacia NVIDIA (`maxConcurrent`) para no provocar 429.
- **Limite de salida:** si la peticion no trae `max_tokens`, pone `defaultMaxTokens` (evita llamadas a herramientas cortadas por un tope bajo).
- **Razonamiento:** convierte `reasoning` y las etiquetas `<think>...</think>` en `reasoning_content`, que es lo que OpenCode entiende.
- **Estadisticas:** `GET /stats` (por modelo: peticiones, errores, cambios de modelo, mediana del primer fragmento) y registro en `logs/gateway.jsonl`.

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

## Licencia
Apache License 2.0.
