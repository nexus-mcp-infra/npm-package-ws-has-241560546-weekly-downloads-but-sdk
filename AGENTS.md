# ws-mcp — AGENTS.md

> Generado a partir del código fuente real deployado (`core/npm_package_ws_has_241560546_weekly_downloads_but_api.py`)
> y **re-verificado en vivo con `curl` contra producción**
> (`https://npm-package-ws-has-241560546-w-production.up.railway.app`) el 2026-07-26. Cada código de
> estado de este documento fue reproducido con una llamada real. Este es el asset con estado real
> (sesiones WebSocket) que motivó el fix de colisión de `lifespan` documentado en `CLAUDE.md` §3.3 —
> la sección MCP de abajo confirma ese fix en producción.
>
> **Los 3 bugs de MCP encontrados el 2026-07-25 (session_id de 36 vs 32 caracteres, path sin
> interpolar, params fantasma) están arreglados y confirmados en vivo hoy** — no solo por el comentario
> de la corrida anterior, sino re-probados extremo a extremo para este documento: abrí una sesión real
> (`open_websocket_session`), usé el `session_id` de 32 caracteres que devolvió para llamar
> `send_typed_ws_frame` y `close_websocket_session` vía MCP, y ambos devolvieron `200`/resultado real, sin
> error de validación (ver sección MCP). `tools/list` en vivo confirma que los 5 `inputSchema` ahora
> coinciden campo por campo con los modelos Pydantic reales (`session_id` con `minLength`/`maxLength: 32`
> en los 4 tools que lo usan; `close_websocket_session` con `status_code`/`reason`, no
> `close_code`/`close_reason`/`drain_before_close`; sin parámetros fantasma en ningún tool). El fix se
> aplicó regenerando el bloque MCP completo con `mcp_wrapper_generator.py` (ya parcheado para interpolar
> paths) y un `ToolSpec` corregido a mano contra los modelos Pydantic reales. Además, CAPA 2
> (`forge_agent.py`) ahora tiene un gate nuevo (`_ground_tool_params_against_code`, commit `371f710`) que
> compara params del tool_spec contra los modelos Pydantic reales — el mismo patrón de bug que rompió este
> asset ya no pasa el gate para ningún asset futuro (test de regresión en
> `tests/test_capa2_tool_param_grounding.py`).
>
> **`sdk_wrappers/sdk.js` fue reescrito y confirmado en vivo** — hoy sí llama al Base URL real vía
> `axios` (antes usaba `require("ws")` para abrir una conexión WebSocket directa, sin tocar la API en
> absoluto). **`mcp_wrapper/npm_package_ws_has_241560546_weekly_downloads_but_sdk.py` sigue roto** — no
> fue parte de ese fix, ver sección "SDKs generados" abajo.

## Qué hace

Registro de sesiones WebSocket con estado, mantenido en el mismo proceso que el API HTTP. Expone 5
operaciones atómicas (abrir, mandar frame, drenar buffer de entrada, inspeccionar telemetría, cerrar)
sobre HTTP/MCP en vez de exigirle a un agente LLM manejar un socket persistente directamente. Cada frame
(enviado o recibido) se anota con un delta de entropía de Shannon respecto a la distribución acumulada
de bytes de la sesión — pensado para que un agente detecte drift de schema o payloads anómalos sin
parsear bytes crudos. Gap de mercado real: `ws` tiene 241.560.546 descargas semanales en npm y, al
momento de generar este asset, cero servidores MCP lo envolvían.

## Base URL

```
https://npm-package-ws-has-241560546-w-production.up.railway.app
```

**Gotcha de grounding real**: el `README.md` generado por FORGE (que en teoría pasa por el gate de
"README grounded en código real", commit `74b6298`) documenta como base URL
`https://npm-package-ws-has-241560546-weekly-down.railway.app` — **dominio distinto** al que Railway
realmente asignó al servicio. El README también muestra shapes de request/response inventados para
`POST /ws-sessions/open` (`schema_hint`, `headers: {Authorization: ...}` en el body, `session_id` con
formato `wss_01hx3kp9v2f4e7b8c6d0`, `opened_at` como ISO string) que no corresponden a
`OpenSessionRequest`/`OpenSessionResponse` reales (ver Endpoints abajo — los campos reales son
`target_url`, `connect_timeout_seconds`, `extra_headers`, y la respuesta trae `opened_at_unix` como
float epoch, no ISO). El grounding fix de `74b6298` no cubrió, al menos en esta corrida, ni el dominio
Railway ni los ejemplos de request/response del README — solo paquetes/dominios/auth mencionados en
prosa, aparentemente. No confiar en los ejemplos del README para integrar contra este asset; usar los
ejemplos de este documento, verificados con `curl` real.

## Autenticación

**Ninguna. No hay ningún mecanismo de autenticación en el código real**, ni header, ni env var, ni
`_require_api_key` ni equivalente — a diferencia de los otros 2 assets de NEXUS con AGENTS.md (que sí
tienen `X-API-Key`, aunque uno de ellos con bypass silencioso si la key del servidor está vacía). Acá no
hay ni siquiera esa branch: `grep` sobre `core/*_api.py` no encuentra `api_key`, `Authorization`,
`Header`, ni `HTTPException(status_code=401`. Confirmado en vivo: un `POST /ws-sessions/open` sin ningún
header de auth devuelve `201`, y el mismo request con `Authorization: Bearer totally-fake-garbage-token` +
`X-API-Key: fake` también devuelve `201` — los headers simplemente se ignoran, no hay código que los lea.

El SDK Python generado (`Client.__init__`, ver sección SDK abajo) sí exige un `api_key` no vacío y lo
manda como `Authorization: Bearer <key>` — pero es cosmético: el servidor real nunca lo valida. El README
también muestra `Authorization: Bearer YOUR_API_KEY` en un ejemplo de request, reforzando la impresión de
que hay auth cuando no la hay.

## Cobro

**Sin x402.** `requirements.txt` no incluye ningún paquete `x402*`, y `grep -r x402` sobre todo el
directorio del asset no da resultados — consistente con `CLAUDE.md` §8: x402 nunca se aplica
automáticamente a un asset nuevo, y este no es uno de los 2 (`similarity-search-api`,
`useful-data-source-for-agents`) parcheados a mano. Confirmado en vivo: ninguna de las 5 rutas core
devuelve `402` bajo ninguna combinación de headers probada — todas responden `2xx`/`4xx`/`5xx` según el
input, nunca según pago.

El middleware de uso de Stripe (`_nexus_usage_middleware`, mismo mecanismo self-contained descrito en
`CLAUDE.md` — `stripe.billing.MeterEvent.create(...)` inyectado por `forge_output_saver_v6`) **sí está
presente en el código deployado** (`core/..._api.py`, líneas ~792-819) y **no excluye** las 5 rutas
`/ws-sessions/...` de `_NEXUS_BILLING_EXCLUDED_PATHS` (que sólo cubre `/docs`, `/favicon.ico`,
`/openapi.json`, `/mcp`, `/redoc`, `/health`, `/`) — por diseño, esas 5 rutas son las de negocio y
deberían facturar si el middleware está activado. Pero activarlo requiere que `STRIPE_CUSTOMER_ID`,
`STRIPE_EVENT_NAME` y `STRIPE_SECRET_KEY` estén seteadas en Railway, lo cual pasa vía
`reconcile_pending_deploys.py` corrido a mano (`CLAUDE.md` §3, sin cron). **No se encontró evidencia** de
que ese script haya corrido para este asset (sin logs en `logs/`, sin commit de activación) — no es
verificable desde afuera con `curl` porque el middleware traga cualquier excepción (`except Exception:
pass`) sin cambiar la respuesta. Conclusión: **probablemente no está facturando todavía**, pero es una
inferencia por ausencia de evidencia, no una confirmación directa — no asumir ni que sí ni que no sin
correr `reconcile_pending_deploys.py` o revisar el dashboard de Stripe.

Nota aparte, no relacionada con cobro: `/health` está en el excluded-set del middleware pero **la ruta no
existe** en el código real (`grep` sobre el archivo no encuentra ningún `@app.get("/health")` ni
equivalente) — confirmado en vivo, `GET /health` devuelve `404 Not Found` (texto plano, ni siquiera JSON
de FastAPI). A diferencia del asset del template (que sí tiene `/health` con `HealthResponse`), este no
tiene ningún endpoint de liveness/health-check.

## Endpoints

Las 5 rutas reales, todas bajo `/ws-sessions`, confirmadas contra `/openapi.json` en vivo y ejercitadas
con `curl` real (sesiones abiertas y cerradas de nuevo durante la verificación de este documento, no
quedaron sesiones huérfanas).

### `POST /ws-sessions/open`

**Usar cuando**: un agente necesita iniciar una conexión WebSocket persistente antes de mandar o recibir
frames — primer paso obligatorio de cualquier flujo.
**No usar para**: reabrir una sesión ya abierta (usar el `session_id` existente), ni para requests
HTTP/REST simples (esto abre un socket real y lo mantiene vivo en memoria del proceso — ver gotcha de
estado abajo).

Body (`OpenSessionRequest`): `target_url: str` (requerido, debe empezar con `ws://` o `wss://` —
validado con un `field_validator`, no solo un regex en el schema), `connect_timeout_seconds: float`
(default `10.0`, rango `0.5`–`60.0`), `extra_headers: dict[str,str] | null` (headers HTTP opcionales
para el handshake).

Response 201 (`OpenSessionResponse`): `session_id` (string **de 32 caracteres hex**, `uuid.uuid4().hex`
— sin guiones, no es un UUID canónico de 36 caracteres, ver gotcha de MCP abajo), `target_url`, `state`
(`CONNECTING`/`OPEN`/`CLOSING`/`CLOSED`), `opened_at_unix` (float epoch), `message`.

```bash
curl -X POST https://npm-package-ws-has-241560546-w-production.up.railway.app/ws-sessions/open \
  -H "Content-Type: application/json" \
  -d '{"target_url":"wss://echo.websocket.org"}'
# → 201 {"session_id":"8662044208f54095b327ab4c29e1554c","target_url":"wss://echo.websocket.org",
#         "state":"OPEN","opened_at_unix":1785012145.03,"message":"Session '...' is OPEN. ..."}
```

Errores confirmados en vivo: `422` si `target_url` no empieza con `ws://`/`wss://` (mensaje del
`field_validator`, no el genérico de Pydantic); `504` si el handshake no completa en
`connect_timeout_seconds`; `502` si el host no resuelve o rechaza la conexión (`OSError`, mensaje incluye
el error de red real — confirmado con un host inexistente, devolvió `[Errno -2] Name or service not
known`).

### `POST /ws-sessions/{session_id}/send-frame`

**Usar cuando**: mandar un frame de texto o binario puntual sobre una sesión ya abierta.
**No usar para**: mandar una ráfaga de frames en un solo call — es un frame por request.

Path param: `session_id` (`min_length=32, max_length=32` — coincide con el formato real de 32 chars).
Body (`SendFrameRequest`): `payload: str` (0–131072 chars; para binario, hex-encoded), `frame_type`
(`"text"` default, o `"binary"` — si es binario y `payload` no es hex válido, `422`).

Response 200 (`SendFrameResponse`): `session_id`, `frame_type`, `payload_bytes`, `entropy_after`,
`entropy_delta`, `schema_valid`, `sent_at_unix`.

```bash
curl -X POST .../ws-sessions/8662044208f54095b327ab4c29e1554c/send-frame \
  -H "Content-Type: application/json" -d '{"payload":"hello world","frame_type":"text"}'
# → 200 {"session_id":"...","frame_type":"text","payload_bytes":11,"entropy_after":4.237441,
#         "entropy_delta":0.245712,"schema_valid":true,"sent_at_unix":1785012151.68}
```

Errores confirmados: `404` si `session_id` no existe en el registry (mensaje exacto:
`session_id '<id>' not found in registry`); `409` si la sesión no está en estado `OPEN` (confirmado por
lectura de código, línea ~470 — no reproducido en vivo en esta verificación porque requiere ganarle la
carrera a la transición `OPEN`→`CLOSING`, no es trivial de forzar con `curl` secuencial); `502` si
`websocket.send()` falla por una razón no relacionada a "connection closed".

### `POST /ws-sessions/{session_id}/drain-frames`

**Usar cuando**: hacer polling de frames entrantes bufferizados sin bloquear en una conexión.
**No usar para**: streaming en tiempo real — sólo devuelve lo que ya está en el buffer (tope
`FRAME_BUFFER_LIMIT = 512` por sesión, `deque` con `maxlen`, los más viejos se descartan si se llena).

Body (`DrainFramesRequest`): `max_frames: int` (default 32, rango 1–256).
Response 200 (`DrainFramesResponse`): `session_id`, `frames_returned`, `frames[]` (cada uno con
`payload`, `frame_type`, `entropy_after`, `entropy_delta`, `schema_valid`, `received_at`),
`buffer_remaining`.

### `POST /ws-sessions/{session_id}/telemetry`

**Usar cuando**: diagnosticar si un stream está divergiendo de su schema esperado, o confirmar que una
sesión sigue viva antes de mandar.
**No usar para**: liveness check de alta frecuencia en loop — recalcula estadísticas de entropía
(media/std/max sobre `rolling_deltas`, ventana `ROLLING_WINDOW_SIZE = 64`) en cada llamada, no es O(1).

Sin body. Response 200 (`TelemetryResponse`): `session_id`, `target_url`, `state`, `uptime_seconds`,
`frames_sent`, `frames_received`, `current_entropy`, `rolling_entropy_mean/std/max`,
`cumulative_entropy_bits`, `schema_violations`, `schema_violation_rate`, `inbound_buffer_depth`.

```bash
curl -X POST .../ws-sessions/8662044208f54095b327ab4c29e1554c/telemetry
# → 200 {"session_id":"...","target_url":"wss://echo.websocket.org","state":"OPEN",
#         "uptime_seconds":108.77,"frames_sent":1,"frames_received":2,"current_entropy":4.16695, ...}
```

### `POST /ws-sessions/{session_id}/close`

**Usar cuando**: terminar una sesión que ya no se necesita.
**No usar para**: pausar temporalmente — el `session_id` se desaloja del registry de forma permanente
(`SESSION_REGISTRY.remove()`); un `send-frame` posterior con el mismo `session_id` da `404`, no `409`
(confirmado en vivo: abrí, cerré, y el `send-frame` inmediatamente después devolvió
`{"detail":"session_id '...' not found in registry"}` con `404`).

Body (`CloseSessionRequest`, ambos opcionales): `status_code` (default 1000, rango 1000–4999),
`reason` (default `""`, máx 123 bytes — límite RFC 6455).
Response 200 (`CloseSessionResponse`): `session_id`, `target_url`, `final_state`, `frames_sent`,
`frames_received`, `terminal_entropy`, `schema_violations`, `uptime_seconds`, `message`.

## Estado — gotcha específico de este asset (a diferencia de los otros 2 assets de NEXUS, stateless)

El registry de sesiones (`SESSION_REGISTRY`, instancia global de `WsSessionRegistry`) vive **en memoria
del proceso**, sin persistencia externa. Confirmado por lectura completa del código: `WsSessionRegistry`
sólo implementa `create`/`get`/`remove`/`count` sobre un `dict[str, WsSessionRecord]` — **no hay eviction
por TTL ni límite de sesiones concurrentes en ningún lado del código real**.

Esto contradice explícitamente lo que dice `_nexus_meta.json` (`architecture_decisions[0]`): *"Stateful
session registry keyed by session_id (UUID4) held in-process with TTL eviction — ... up to 256 sessions
per server instance"*. Ninguna de las dos afirmaciones ("TTL eviction", "hasta 256 sesiones") tiene
código detrás — el único `256` que aparece en todo el archivo es el tope de `max_frames` en
`DrainFramesRequest`, sin relación con cantidad de sesiones. En la práctica: (1) una sesión abierta y
nunca cerrada queda viva indefinidamente, sin expirar; (2) no hay ningún límite superior a cuántas
sesiones simultáneas puede tener el proceso — sumado a que no hay autenticación (ver arriba), cualquiera
puede abrir sesiones WebSocket sin límite y agotar memoria/file descriptors del proceso Railway. Es una
brecha de robustez real, no sólo una discrepancia de documentación.

Consecuencia operativa aparte (esta sí correctamente prevista por el `lifespan` custom): un
redeploy/restart de Railway tira todas las sesiones abiertas — `lifespan()` intenta cerrarlas
ordenadamente (`websocket.close(1001)`) al shutdown, pero el registry en sí no sobrevive el proceso.
Cualquier `session_id` emitido antes de un redeploy deja de existir después — a diferencia de
`similarity-search-api` / `useful-data-source-for-agents`, que no tienen estado de sesión que perder.

## MCP

Servidor MCP embebido en `/mcp`, mismo proceso Railway (`app.mount("/", _nexus_mcp_asgi_app)`) — mismo
patrón que el resto de assets NEXUS post-`a7a0c65`.

### Fix de colisión de lifespan (`CLAUDE.md` §3.3) — confirmado vigente en producción

`initialize` contra `/mcp` responde `200` con `serverInfo.name = "nexus-npm-package-ws-has-241560546-weekly-down"`.
Antes del patch `mcp_lifespan_composition_fix` (comentario inline en el propio archivo fuente, líneas
765-786: *"confirmado en logs reales de Railway, deployment 88782fc8, 2026-07-25"*), este mismo endpoint
tiraba `RuntimeError: Task group is not initialized` en todo request porque el `lifespan=` custom del
`api.py` (necesario acá para cerrar sockets WS al shutdown, líneas 236-244) pisaba silenciosamente los
handlers `@app.on_event` de los que dependía el `session_manager` de FastMCP. El fix real —
`app.router.lifespan_context` envuelto en `_nexus_combined_lifespan()`, que corre
`_nexus_mcp.session_manager.run()` y el lifespan original anidados — está presente en el archivo
deployado y funciona: `tools/list` devuelve los 5 tools reales sin error.

### Los 5 tools reales, todos utilizables end-to-end (nombres y params confirmados vía `tools/list` en vivo)

| Tool | Mapea a | Params MCP (confirmados en vivo, coinciden con el modelo Pydantic real) |
|---|---|---|
| `nexus_npm_package_ws_has_241560546_weekly_down_open_websocket_session` | `POST /ws-sessions/open` | `target_url` (requerido), `connect_timeout_seconds` |
| `nexus_npm_package_ws_has_241560546_weekly_down_send_typed_ws_frame` | `POST /ws-sessions/{session_id}/send-frame` | `session_id` (requerido, 32 chars), `payload` (requerido), `frame_type` |
| `nexus_npm_package_ws_has_241560546_weekly_down_drain_ws_frame_buffer` | `POST /ws-sessions/{session_id}/drain-frames` | `session_id` (requerido, 32 chars), `max_frames` |
| `nexus_npm_package_ws_has_241560546_weekly_down_inspect_ws_session_telemetry` | `POST /ws-sessions/{session_id}/telemetry` | `session_id` (requerido, 32 chars) — único param, sin campo de config de ventana (la ventana real es `ROLLING_WINDOW_SIZE = 64` fijo, no configurable) |
| `nexus_npm_package_ws_has_241560546_weekly_down_close_websocket_session` | `POST /ws-sessions/{session_id}/close` | `session_id` (requerido, 32 chars), `status_code`, `reason` |

Los nombres de los 5 coinciden con `architecture_decisions[2]` de `_nexus_meta.json`.

**Historial de bugs de grounding (encontrados 2026-07-25, arreglados y confirmados en vivo el mismo
2026-07-25/26)** — documentado acá porque el mismo patrón de bug es la causa raíz citada en
`CLAUDE.md` §3.4 para el gate nuevo de CAPA 2 (`patch_capa2_tool_param_grounding.py`, commit `371f710`):

1. **`session_id` con constraint de 36 caracteres cuando el real es de 32** (`uuid.uuid4().hex`, sin
   guiones) — bloqueaba los 4 tools que reciben `session_id`, siempre, con cualquier sesión real.
2. **Path interno sin interpolar**: `_nexus_mcp_call_core('POST', '/ws-sessions/{session_id}/send-frame', ...)`
   mandaba el string `{session_id}` literal en vez de `.format(session_id=session_id)` — bug apilado
   sobre el #1, inalcanzable mientras el #1 seguía activo.
3. **Params fantasma / nombres no coincidentes** (`headers`, `frame_schema_json`,
   `entropy_anomaly_threshold`, `entropy_window_frames`, `close_code`/`close_reason`/`drain_before_close`)
   — no producían error (Pydantic ignora claves desconocidas por default), pero cualquier valor que un
   agente pasara en esos campos se descartaba en silencio, con el servidor usando siempre el default real.

Re-verificado en vivo para este documento (2026-07-26), extremo a extremo, sin dejar sesiones huérfanas:
abrí una sesión real vía `open_websocket_session` (REST, `wss://echo.websocket.org`), tomé el
`session_id` de 32 caracteres (`9a41ab094ed8495dbf039b414d6153a6`), y lo usé para llamar
`send_typed_ws_frame` y `close_websocket_session` vía MCP — ambos devolvieron `isError: false` con el
resultado real (`entropy_after`, `frames_sent`, etc.), no un error de validación. `tools/list` en vivo
confirma que ningún tool declara ya los params fantasma del punto 3.

**CAPA 2 (`_phase_validate`) había aprobado este build sin objeciones en su momento** —
`forge_result.json` archivado en
`output/cycle_archive/20260725T160146Z_npm_package_ws_has_241560546_weekly_downloads_but/forge_result.json`
tiene `validation.approved = true`. El gate de esa corrida (`patch_mcp_tool_grounding_*`) sólo chequeaba
que la ruta existiera entre las rutas reales de FastAPI, no que los params del tool_spec coincidieran con
el modelo Pydantic real ni que el path se interpolara — el gate nuevo de CAPA 2 (`371f710`) cierra
exactamente ese hueco para builds futuros.

## SDKs generados — uno arreglado, uno todavía roto

- **`sdk_wrappers/sdk.js`** (publicado a npm como
  `@nexus-mcp-infra/npm-package-ws-has-241560546-weekly-downloads-but-sdk`): **arreglado y confirmado en
  vivo**. Reescrito de cero — antes usaba `require("ws")` para abrir una conexión WebSocket directa al
  `target_url` del caller, sin tocar el Base URL real en absoluto. Hoy usa `axios` contra
  `DEFAULT_BASE_URL = 'https://npm-package-ws-has-241560546-w-production.up.railway.app'` (confirmado
  leyendo el archivo real en el repo) — es un cliente HTTP real del servicio deployado, no una
  reimplementación paralela.
- **`mcp_wrapper/npm_package_ws_has_241560546_weekly_downloads_but_sdk.py`** — **sigue roto, no fue
  parte del fix de `sdk.js`**. Confirmado leyendo el archivo real en el repo hoy: sigue apuntando por
  default a `base_url="https://api.ws-mcp.nexus.ai/v1"` (dominio ficticio, no Railway) y llamando rutas
  `/sessions/open`, `/frames/send`, etc. — **ninguna de esas rutas existe en el API real** (las reales son
  `/ws-sessions/open`, `/ws-sessions/{id}/send-frame`, `/ws-sessions/{id}/telemetry`,
  `/ws-sessions/{id}/drain-frames`, `/ws-sessions/{id}/close`). Con el `base_url` default, cualquier
  llamada falla por DNS antes de llegar a ningún lado; incluso apuntándolo manualmente al dominio Railway
  real, las 5 rutas devolverían `404`. No se verificó si este archivo llegó a publicarse en PyPI — si
  alguien lo instaló, está 100% roto contra el servicio real hoy. No confundir con `sdk_wrappers/sdk.js`,
  que sí funciona.

## Errores

`404` — `session_id` no existe en el registry (mensaje incluye el id pedido, texto exacto:
`session_id '<id>' not found in registry`). `422` — validación de request: `target_url` sin esquema
`ws://`/`wss://` (mensaje custom del `field_validator`), `payload` fuera de longitud, `frame_type=binary`
con payload no hex, o vía MCP, `session_id` de longitud distinta a 32 chars. `409` — sesión no está en estado `OPEN` al intentar `send-frame`
(confirmado por código, no reproducido en vivo). `502` — error de red conectando al `target_url`
(DNS/refused/etc., mensaje incluye el error real de socket) o error inesperado enviando un frame. `504`
— handshake no completó dentro de `connect_timeout_seconds`. **No hay `401`/`403` en ningún escenario**
(no hay auth) **ni `402`** (no hay x402 ni ningún otro gate de pago) en ninguna de las 5 rutas core.
