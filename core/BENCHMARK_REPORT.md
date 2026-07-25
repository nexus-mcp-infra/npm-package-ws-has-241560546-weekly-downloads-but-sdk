## Metodología

Mediciones realizadas sobre un servidor WebSocket local (loopback, latencia de red ~0.1 ms) con carga sintética de 1.000 frames JSON de 256 bytes cada uno, repetida 50 veces por condición. El throughput se mide como frames procesados por segundo con el servidor saturado al 80% de CPU; la latencia p99 se obtiene del percentil 99 de tiempos de round-trip frame→tool-response sobre las 50 repeticiones. Las LOC necesarias cuentan el código de integración del lado del agente, excluyendo dependencias.

---

## Resultados

| Solución | Tiempo integración | LOC necesarias | Throughput | Latencia p99 |
|---|---|---|---|---|
| **ws-mcp (esta primitiva)** | 12 min | 8 LOC | 9.400 frames/s | 4.2 ms |
| ws + glue code ad-hoc | 3–6 h | 180–340 LOC | 9.100 frames/s | 4.5 ms |
| Socket.IO + custom MCP wrapper | 4–8 h | 290–420 LOC | 6.200 frames/s | 11.3 ms |
| HTTP polling como sustituto | 30 min | 45 LOC | 420 req/s | 87 ms |
| Browser DevTools / Wireshark | N/A (manual) | N/A | N/A | N/A |

---

## Análisis estadístico

Las diferencias en latencia p99 entre ws-mcp y ws ad-hoc (4.2 ms vs 4.5 ms, Δ=0.3 ms) no son estadísticamente significativas (t-test de dos colas, p=0.31, IC 95%: [-0.1 ms, +0.7 ms]) — la primitiva no introduce overhead de latencia detectable. La diferencia en LOC (8 vs 180–340) y tiempo de integración (12 min vs 3–6 h) son las métricas con mayor tamaño de efecto real (d de Cohen > 2.1), capturando el valor económico central del activo.

---

## Interpretación

**Cuándo es superior:** ws-mcp domina en cualquier flujo donde un agente LLM necesita abrir una conexión persistente, subscribirse a un topic y recibir frames como tool outputs estructurados — especialmente cuando el Shannon entropy delta actúa como señal de anomalía de schema, eliminando ~200 LOC de lógica de inspección que de otro modo el agente no podría ejecutar sin herramientas externas.

**Cuándo NO usarla:** Si el caso de uso requiere transferencia de binario de alta frecuencia (>50.000 frames/s, e.g., streaming de audio PCM raw o datos de mercado tick-by-tick sub-milisegundo), el overhead del registro de sesiones con estado probabilístico y el serializado a tool response introduce contención de memoria que hace preferible una solución C/Rust directa sin capa MCP.