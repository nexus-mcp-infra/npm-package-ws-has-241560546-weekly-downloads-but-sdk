# Análisis de Complejidad Computacional — WebSocket MCP Session Manager

## Métodos Públicos

### `ws_session_open` — Abrir conexión y registrar sesión

**Temporal:** O(1) amortizado. El handshake TCP+WS es latencia de red (no computable), y la inserción en el registro de sesiones (dict por `session_id`) es O(1). Inicialización del `Counter` de frecuencias vacío es O(1).
**Espacial:** O(A) donde A = tamaño del alfabeto observado inicialmente (0 en apertura). El overhead fijo por sesión es ~200 bytes (metadata + Counter vacío).
**Mejor:** O(1) — servidor remoto acepta inmediatamente. **Promedio:** dominado por RTT de red. **Peor:** O(timeout) si el servidor no responde; el registro no crece.
**Cuello de botella:** Límite de file descriptors del proceso. A ~1024 conexiones concurrentes por defecto en Linux, `ulimit` impone saturación antes que la lógica de registro.

---

### `ws_frame_send` / `ws_frame_receive` — Envío y recepción de frame con entropy delta

**Temporal:** O(F + A) por frame, donde F = tamaño del frame en bytes/tokens y A = tamaño actual del alfabeto acumulado en el `Counter` de esa sesión. La actualización del `Counter` es O(F); el cálculo de H(t) = -Σ p·log₂(p) itera sobre A términos distintos del Counter.
**Espacial:** O(A_max) por sesión, donde A_max crece hasta estabilizarse en el número de tokens únicos observados. En streams JSON típicos A_max ≈ 10²–10³ campos distintos.
**Mejor:** O(F) cuando A es pequeño (sesión joven, vocabulario mínimo). **Promedio:** O(F + A) con A estabilizado. **Peor:** O(F·N_frames) si el alfabeto nunca converge (stream binario puro con entropía máxima creciente) — degrada a O(N) acumulado por sesión.
**Cuello de botella:** El cálculo de entropía O(A) se ejecuta síncronamente por frame dentro del event loop asyncio. Con A > 5000 y frames > 10 kHz por sesión, esto bloquea el loop. Es el único punto de contención computacional real.

---

### `ws_session_inspect` — Snapshot del estado probabilístico de una sesión

**Temporal:** O(A) para serializar el `Counter` y calcular la distribución normalizada. O(S) para recuperar las últimas S frames del buffer circular de historial.
**Espacial:** O(A + S) para la respuesta; el estado en memoria ya existía.
**Mejor/Promedio/Peor:** todos O(A + S) — sin varianza algorítmica, puramente proporcional al estado acumulado.
**Cuello de botella:** Si S (tamaño del buffer de historial) es configurable sin límite superior, `inspect` puede devolver respuestas de varios MB. El límite debe ser constante (S ≤ 500 frames) para mantener latencia de respuesta < 50 ms.

---

## Saturación y Estrategia de Escalado

Con frames de ~1 KB y A estabilizado en ~200 tokens, el cálculo de entropía por frame consume ~4 µs en CPython 3.11. El event loop asyncio satura a ~2500–3000 frames/segundo agregados antes de que la latencia de `receive` supere 10 ms — independientemente del número de sesiones concurrentes. Para escalar más allá: descargar el cálculo de H(t) a un worker thread mediante `loop.run_in_executor` con un `ThreadPoolExecutor` de tamaño fijo, manteniendo el event loop libre para I/O puro. El `Counter` por sesión se protege con `asyncio.Lock` de grano fino (una por `session_id`), evitando contención global.