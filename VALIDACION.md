# Evidencia de validación — 9 de noviembre de 2026

## Compilación y pruebas

Ejecutado con Java 21.0.5 y Maven Wrapper 3.10.0, conservando Spring Boot 4.1.2:

```powershell
.\mvnw.cmd -B -ntp verify -P live-readonly
```

Resultado: **BUILD SUCCESS**, 51 pruebas unitarias/HTTP más 1 prueba de integración real de solo lectura, sin fallos, errores ni pruebas omitidas.

- `TopicPolicyTest`: nombres, tópicos críticos/internos/adicionales, bandera de escritura, configuraciones y credenciales obligatorias.
- `KafkaAdminServiceTest`: brokers/controller, listado de tópicos, particiones/replicas/ISR, creación, replicación inválida, retención, incremento, confirmación de DELETE, redacción de configuración sensible, grupos/asignaciones/offsets y cálculo de lag (incluyendo ausencia de commits, grupos inactivos, offsets vencidos o adelantados).
- `KafkaGatewayTest`: llamadas AdminClient, modificación incremental SET, creación/eliminación/incremento simulados, espera acotada, filtrado de grupos y traducción de errores 400/403/404/409/503.
- `MsKafkaAdminApplicationTests`: contexto Spring, Basic válido/inválido, rol ADMIN, health público mínimo, CSRF real con cookie y token, JSON inválido/desconocido, DELETE confirmado y errores sin trazas.
- `WritesDisabledHttpTest`: las cuatro rutas de escritura bloqueadas antes de llamar a Kafka; las lecturas siguen disponibles.
- `ReadOnlyKafkaIT`: servidor HTTP real en loopback/puerto aleatorio con credenciales efímeras y AdminClient real. Solo GET; no hay operaciones mutantes ni consumidores.

Se ejecutó además el JAR con usuario y contraseña vacíos, puerto aleatorio y bootstrap inaccesible de prueba: **rechazó el arranque por falta de credenciales**, según el error explícito de configuración. No se usaron credenciales reales para estas verificaciones.

Los primeros intentos detectaron incompatibilidades de imports/constructores de fixtures con Kafka/JUnit actuales y dos errores en preparación/verificación de mocks; se corrigieron antes de la ejecución exitosa.

## Lecturas del clúster existente

Observación inicial a las **12:15 de America/Santiago**, 9 de noviembre de 2026:

| Consulta | Resultado observado |
| --- | --- |
| Brokers | IDs 1, 2 y 3 en localhost:19092, localhost:29092 y localhost:39092. |
| Controller | Broker 3 en esa observación; puede cambiar. |
| `bd.requests.events.v1` | 3 particiones, 3 réplicas e ISR de 3 por partición; retention.ms=604800000, min.insync.replicas=2. |
| `bd.requests.analytics.v1` | 3 particiones, 3 réplicas e ISR de 3 por partición; retention.ms=86400000, min.insync.replicas=2. |
| `bd-report-v1` | Estado Stable; consulta de miembros, asignaciones y commits correcta. |
| Lag partición 0 de events | Commit 2, latest 2, lag 0. |
| Lag partición 1 de events | Commit 2, latest 2, lag 0. |
| Lag partición 2 de events | Sin commit; latest 0; lag null con NO_COMMITTED_OFFSET. |
| Lag total | null, complete=false: no se inventa cero para una partición sin commit. |
| Health | UP sin detalles sensibles. |
| GET administrativo sin Basic | HTTP 401. |

La prueba comprobó compatibilidad efectiva de las consultas usando kafka-clients 4.2.2 (BOM) contra el broker 3.9.1 existente. No cambió versiones del clúster ni el modo ZooKeeper. El servicio no se unió al grupo ni afectó su consumo.

Evidencia generada en `target/live-readonly-evidence.json`; reportes en `target/surefire-reports/` y `target/failsafe-reports/`; log de Maven en `target/validation.log`. Son artefactos locales ignorados por Git, sin las credenciales efímeras. Una nueva ejecución reemplaza la evidencia con la observación de ese momento.

## Límites de esta validación

- Las escrituras se verificaron **con AdminClient simulado**, no sobre el clúster existente. No se crearon, eliminaron, purgaron ni modificaron tópicos reales para probar el servicio.
- No se prueba una reasignación de réplicas ni se expone esa operación.
- No se detuvieron brokers ni se simuló caída del clúster real; timeout y Kafka no disponible se probaron con futuros/excepciones simulados.
- No se construyó/ejecutó la imagen Docker ni se desplegó en AWS. Dockerfile y configuración están preparados, sin afirmar una validación externa no realizada.
- No se modificaron otros proyectos, tablas Supabase, RabbitMQ, Requests, Report, BFF, React, infraestructura ni Entra ID. No se leyeron archivos .env reales.
- La carpeta inicial ms-kafka-admin no contenía `.git`. No se inicializó un repositorio, ni se hicieron commits/pushes. Los archivos se editaron exclusivamente dentro de esa carpeta.

## Archivos

Modificados: `pom.xml` (solo perfil opcional de integración, sin agregar dependencias), `application.properties`, `.gitignore` y el test inicial `MsKafkaAdminApplicationTests`.

Creados: `AdminProperties`, `AdminConfiguration`, `AdminCredentials`, `SecurityConfiguration`, `AdminDtos`, `TopicPolicy`, `KafkaAdminService`, `KafkaGateway`, `KafkaHealthIndicator`, `KafkaAdminController`, `AdminApiException` y `ApiExceptionHandler`; las cinco clases de prueba adicionales; `.env.example`, `Dockerfile`, `.dockerignore`, `README.md`, `DEMO-EV5-EV6.md` y este documento. La clase principal Initializr se conserva.

Ver el procedimiento de escrituras manuales restringidas a un tópico nuevo en [DEMO-EV5-EV6.md](DEMO-EV5-EV6.md).
