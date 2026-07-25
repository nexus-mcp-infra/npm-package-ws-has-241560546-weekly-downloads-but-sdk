# Justificación Matemática: WebSocket MCP Session Primitive

## 1. Máximo 5 Endpoints (Hick's Law)

$$T = b \cdot \log_2(n + 1)$$

Con $n = 5$ tools (`ws_connect`, `ws_send_frame`, `ws_subscribe_topic`, `ws_inspect_session`, `ws_close`), el tiempo de selección del agente es $T = b \cdot \log_2(6) \approx 2.58b$. Añadir una sexta tool (e.g., `ws_reconnect`) eleva el espacio de decisión un 15% sin reducir ambigüedad funcional — el agente confunde `reconnect` con `connect`. La superficie mínima maximiza la tasa de elección correcta por llamada de inferencia.

## 2. Pricing Per-Call vs. Por Asiento

$$\varepsilon_d = \frac{\partial Q / Q}{\partial P / P}$$

Las conexiones WebSocket tienen duración variable (segundos a horas); una suscripción fija penaliza cargas de trabajo burst y subsidia sesiones idle, desalineando incentivos. El modelo per-call captura elasticidad real: a $0.002 USD por frame analizado con entropia, el costo escala con valor entregado (novelty score procesado), no con tiempo de asiento. Para $|\varepsilon_d| > 1$ (demanda elástica típica de infraestructura dev-tools), el per-call maximiza volumen total $P \cdot Q$ frente a precio fijo.

## 3. Estructura de Datos: Counter de Frecuencias por Sesión

La distribución de probabilidad de tokens por sesión se mantiene como `Counter` acumulado, con actualización en $O(k)$ por frame donde $k$ es el número de campos JSON distintos observados. El cálculo de entropía $H = -\sum p_i \log_2 p_i$ sobre el Counter actualizado es $O(|\text{vocab}|)$, acotado en práctica por el schema del protocolo objetivo (típicamente $|\text{vocab}| < 256$ campos). Una estructura alternativa (histograma fijo o sliding window) destruiría la propiedad de convergencia de la distribución empírica hacia la distribución real del stream — necesaria para que el delta sea una señal estadísticamente válida.

## 4. Invariante Matemático Central

$$\Delta H(t) = H(t) - H(t-1), \quad H(t) = -\sum_{i} \frac{c_i(t)}{N(t)} \log_2 \frac{c_i(t)}{N(t)}$$

donde $c_i(t)$ es la frecuencia acumulada del token $i$ tras $N(t)$ frames totales. El invariante que hace la solución correcta es que $H(t)$ converge monótonamente hacia la entropía verdadera del proceso generador a medida que $N(t) \to \infty$ (por la ley de los grandes números para distribuciones empíricas). Por tanto, $\Delta H(t) \to 0$ cuando el stream sigue su schema habitual, y $|\Delta H(t)| \gg 0$ es condición necesaria (aunque no suficiente) de anomalía o cambio de schema — propiedad que no depende del dominio de aplicación sino de la teoría de la información.

## 5. Límites Teóricos del Sistema

**Latencia de detección:** El delta $\Delta H$ es estadísticamente significativo solo tras $N(t) \geq 1/p_{\min}$ frames, donde $p_{\min}$ es la probabilidad del token menos frecuente del schema real. Para schemas con campos raros ($p_{\min} \sim 0.001$), el warm-up requiere $\sim 1000$ frames antes de que las anomalías sean detectables — el sistema no puede garantizar detección en frame 1.

**Estado en proceso único:** El acoplamiento `session_registry + socket vivo + Counter` dentro del mismo proceso asyncio impone un límite de escalabilidad horizontal: no es posible distribuir sesiones entre workers sin serializar estado probabilístico en cada frame, introduciendo latencia $O(\text{network RTT})$ que destruye la utilidad del tracker en tiempo real. El sistema escala verticalmente, no horizontalmente.