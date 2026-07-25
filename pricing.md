# Pricing

El modelo de tarifa decreciente por volumen existe porque el coste marginal de mantener este sistema no escala linealmente con el uso: la infraestructura más cara (el registro de sesiones con estado probabilístico vivo, el proceso que sostiene conexiones WebSocket abiertas y actualiza distribuciones de frecuencia por `session_id`) es fija por instancia desplegada, no por llamada individual. Cobrar suscripciones fijas por tier penalizaría al agente que hace un uso esporádico pero intenso en ráfagas — exactamente el patrón de consumo de los workflows de IA — y subvencionaría al suscriptor que paga el tier alto pero apenas usa la primitiva. El pricing por operación refleja la realidad técnica: cada invocación a `subscribe_websocket_topic` o `score_frame_entropy_delta` tiene un coste de cómputo medible y discreto; el resto es amortización de infraestructura que ya está corriendo.

La tarifa decreciente en volumen no es un descuento comercial arbitrario — es una señal de que el sistema aprende a ser más eficiente con sesiones de larga duración. Un agente que mantiene una conexión abierta durante miles de frames genera un histograma de frecuencias más estable, lo que reduce el coste amortizado de cada cálculo de entropía Shannon posterior: `H(t)` converge más rápido cuando la distribución acumulada tiene más masa. Cobrar el mismo precio marginal al frame mil que al frame diez ignoraría esa ganancia de eficiencia computacional real; trasladarla al precio es lo técnicamente honesto.

Finalmente, la ausencia de compromiso mínimo responde al perfil del consumidor real: equipos de ingeniería que descubren esta primitiva mientras construyen un agente específico, la integran para un pipeline concreto y no quieren negociar un contrato antes de saber si el diferenciador — la detección de divergencia de schema vía entropía por sesión — resuelve su problema. El uso temprano, sin fricción de onboarding financiero, es la única forma de que un desarrollador llegue al momento en que `entropy_delta` dispara una alerta real en su stream de producción y entiende por qué esto no puede reemplazarse con un wrapper stateless sobre `ws.send()`.

| Calls / month | Price per call |
|---|---|
| 0 - 100 | Free |
| 101 - 10,000 | $0.0025 |
| 10,001 - 100,000 | $0.0018 |
| 100,001 - 1,000,000 | $0.0012 |
| 1,000,001 - 10,000,000 | $0.0008 |
| 10,000,001+ | $0.0005 |