# ms-kafka-admin — BarrioDigital EV5/EV6

API administrativa independiente sobre el clúster Kafka existente. Conserva Java 21, Spring Boot 4.1.2 y Maven Wrapper 3.10.0 del proyecto Initializr. No usa PostgreSQL, JPA ni Flyway, ni crea infraestructura. No consume/publica mensajes ni modifica offsets de consumidores.

## Arquitectura

```text
Cliente administrativo — HTTP Basic + rol ADMIN + CSRF para escrituras
        -> KafkaAdminController (REST / validación)
        -> KafkaAdminService + TopicPolicy (reglas / protección / lag)
        -> KafkaGateway (AdminClient / espera acotada / errores)
        -> Kafka 3.9.1 existente (sin conexión directa a ZooKeeper)
```

- `config`: propiedades y un AdminClient compartido, cerrado al terminar la aplicación.
- `security`: credenciales obligatorias desde entorno, contraseña codificada con BCrypt en memoria y autorización ADMIN.
- `dto`: contratos REST sin exponer clases internas de Kafka.
- `service`: consultas, reglas de escritura, validación de configuraciones y cálculo de lag.
- `kafka`: comunicación oficial con Kafka y HealthIndicator sin detalles sensibles.
- `exception`: errores HTTP seguros, sin stack traces ni mensajes internos del broker.

El BOM resuelve Spring Kafka 4.1.2, kafka-clients 4.2.2 y Spring Security 7.1.2. No se sobrescriben versiones ni se actualiza el broker a Kafka 4.x. Las consultas a los brokers 3.9.1 fueron verificadas mediante HTTP real; las escrituras se prueban con Mockito para conservar el clúster. Ver [VALIDACION.md](VALIDACION.md).

## Protección y seguridad

`KAFKA_ADMIN_WRITES_ENABLED=false` por defecto permite consultar pero rechaza escrituras con 403. Cambiarlo requiere reiniciar el servicio; no existe un endpoint para habilitarlo.

Siempre están protegidos `bd.requests.events.v1`, `bd.requests.analytics.v1` y los nombres que empiezan por `__`. La lista `KAFKA_ADMIN_PROTECTED_TOPICS` agrega protección; no puede quitar esas restricciones permanentes. Su valor inicial también protege `bd.local.smoke.v1`. Antes de modificar recursos existentes se comprueba además la marca `isInternal` de Kafka. No existe un parámetro de fuerza que omita la protección.

Toda operación administrativa requiere HTTP Basic y rol ADMIN. La cuenta se configura con `KAFKA_ADMIN_USERNAME` y `KAFKA_ADMIN_PASSWORD`, sin valores predeterminados. La aplicación se niega a iniciar si faltan o si la contraseña tiene menos de 16 caracteres o más de 72 bytes UTF-8 (límite BCrypt). No imprime contraseñas. Usar una contraseña aleatoria y única; no guardar credenciales en scripts, Git ni comandos compartidos.

**CSRF permanece habilitado.** Obtener `/api/kafka/admin/csrf` con autenticación, conservar la cookie de sesión y enviar el token en el encabezado indicado para POST/PATCH/DELETE. HTTP Basic puede ser enviado automáticamente por navegadores, por lo que ser una API no justifica desactivar CSRF. Sesión con expiración de 15 minutos, cookie HttpOnly y SameSite=Strict. Reobtener token si caduca o se reinicia la aplicación. No se habilita CORS global.

DELETE exige, adicionalmente, `X-Confirm-Topic` con el nombre exacto del tópico. Tener credenciales, CSRF y escritura habilitada no permite eliminar tópicos protegidos.

Solo `/actuator/health` y sus grupos de salud son públicos, con un estado mínimo. Los demás endpoints Actuator no se exponen. Se mantienen los encabezados de seguridad de Spring. La API queda dentro de la red Docker.

## Variables

`.env.example` documenta las variables; **el servicio no carga archivos .env automáticamente**. Usar variables del proceso o el mecanismo de secretos del despliegue.

| Variable | Valor predeterminado | Función |
| --- | --- | --- |
| `SERVER_PORT` | `8089` | Puerto HTTP. |
| `SERVER_ADDRESS` | `0.0.0.0` | Interfaz del contenedor, sin publicar en el host. |
| `KAFKA_BOOTSTRAP_SERVERS` | `10.0.2.174:19092,10.0.2.174:29092,10.0.2.174:39092` | Listeners privados de la EC2 Kafka. |
| `KAFKA_ADMIN_USERNAME` | ninguno | Cuenta administrativa obligatoria. |
| `KAFKA_ADMIN_PASSWORD` | ninguno | Contraseña obligatoria; 16 caracteres mínimo, 72 bytes UTF-8 máximo. |
| `KAFKA_ADMIN_WRITES_ENABLED` | `false` | Habilita operaciones sobre tópicos no protegidos. |
| `KAFKA_ADMIN_PROTECTED_TOPICS` | `bd.local.smoke.v1` | Nombres adicionales separados por comas. |
| `KAFKA_ADMIN_TIMEOUT_MS` | `5000` | Timeout por llamada AdminClient, rango 500–30000 ms. |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | Protocolo hacia brokers locales; adaptar a TLS/SASL si corresponde. |
| `SESSION_COOKIE_SECURE` | `false` | `true` en despliegues HTTPS. |

Cada espera se limita al timeout configurado y AdminClient recibe `default.api.timeout.ms`; `request.timeout.ms` no supera 3000 ms. Una consulta compuesta puede realizar varias llamadas: lag hasta cuatro esperas y cluster hasta tres. No hay reintentos infinitos de peticiones HTTP. Los reintentos de protocolo son responsabilidad del cliente y quedan acotados por el timeout de la API.

El AdminClient incorpora las propiedades de `spring.kafka`, incluidas las propiedades SSL/SASL suministradas externamente. La autenticación HTTP no sustituye las ACL de Kafka. En el laboratorio PLAINTEXT la protección de esta API no protege accesos directos al broker o a Kafbat.

## Pruebas aisladas

```bash
./mvnw -B -ntp test
```

La prueba opt-in de lectura de clúster requiere un Kafka exclusivo de pruebas.

## API REST

Base: `/api/kafka/admin`.

| Método y ruta | Resultado / función AdminClient |
| --- | --- |
| `GET /cluster` | clusterId, controller, brokers e instante; `describeCluster`. |
| `GET /brokers` | IDs, hosts, puertos y rack conocidos; `describeCluster`. |
| `GET /topics` | Nombres, marca interna y protección; `listTopics`. |
| `GET /topics/{name}` | Líder, réplicas, ISR y subreplicación por partición; `describeTopics`. |
| `POST /topics` | Crear tópico no protegido; `createTopics`. HTTP 201 tras ACK. |
| `DELETE /topics/{name}` | Eliminación confirmada del tópico no protegido; `deleteTopics`. HTTP 202 tras ACK. |
| `GET /topics/{name}/config` | Configuraciones, origen, readOnly y sensitive; valores sensibles ocultos; `describeConfigs`. |
| `PATCH /topics/{name}/config` | Cambio incremental de propiedades permitidas; `incrementalAlterConfigs` con SET. |
| `POST /topics/{name}/partitions` | Incremento del total de particiones; `createPartitions`. |
| `GET /consumer-groups` | Grupos de consumidores y estado; `listGroups`, filtrando protocolos de consumidores. |
| `GET /consumer-groups/{groupId}` | Estado, coordinador, miembros y asignaciones; `describeConsumerGroups`. |
| `GET /consumer-groups/{groupId}/offsets` | Offsets confirmados y particiones asignadas sin commit; `listConsumerGroupOffsets`. |
| `GET /consumer-groups/{groupId}/lag` | Commit, earliest, latest y lag por partición; `listOffsets`. |
| `GET /csrf` | Token y nombre de encabezado para la sesión autenticada. |

No se exponen reset de offsets, deleteRecords, eliminación de grupos, cambios de brokers, reasignaciones de réplicas ni escritura de mensajes.

Crear tópico (ejemplo de contrato para un nombre de prueba propio):

```json
{"name":"bd.demo.admin.example","partitions":3,"replicationFactor":3,"retentionMs":3600000,"cleanupPolicy":"delete","minInsyncReplicas":2}
```

Modificar retención:

```json
{"values":{"retention.ms":"1800000"}}
```

Incrementar particiones:

```json
{"totalPartitions":4}
```

Validaciones: nombre ASCII con letras, dígitos, `.`, `_`, `-`, longitud 1–249, distinto de `.` y `..`; creación con 1–1000 particiones, replicación positiva no mayor que brokers disponibles y min.insync.replicas entre 1 y el factor de replicación. Para tópicos con factores heterogéneos, el límite de min ISR usa el mínimo de sus particiones. El broker vuelve a validar ante cambios concurrentes del clúster.

Solo se admite cambiar `retention.ms` (positivo y finito), `cleanup.policy` (`delete`, `compact`, `delete,compact` o `compact,delete`) y `min.insync.replicas`. Se rechazan campos JSON desconocidos, propiedades fuera de esa lista, valores negativos/inválidos y reducción o repetición del total de particiones. El incremento puede cambiar la distribución futura de claves: nunca usar los tópicos de negocio para demostrarlo.

`ACKNOWLEDGED` indica que Kafka aceptó la operación, no que todos los brokers hayan actualizado inmediatamente su metadata. Volver a consultar para verificar propagación. Una respuesta 503 por timeout puede ocurrir después de un cambio aplicado: inspeccionar antes de reintentar; no se afirma idempotencia de todas las operaciones.

## Consumer groups, offsets y lag

El offset confirmado identifica el próximo registro a consumir. Para una partición con commit válido dentro del intervalo del log, `lag = latestOffset - committedOffset`. Es distancia entre offsets, no una métrica inventada de CPU, mensajes físicos o tiempo de retraso; compactación y registros transaccionales pueden producir huecos. `latest` se consulta con el aislamiento predeterminado READ_UNCOMMITTED; no mide exclusivamente registros de negocio visibles a un consumidor READ_COMMITTED.

Se unen las particiones con commits y las asignaciones actuales de miembros. Un grupo inactivo conserva sus commits y puede tener lag calculable. Un grupo vacío sin commits devuelve lista vacía y `totalLag=null`, no cero. Sin un commit, no se conoce la posición efectiva elegida por auto.offset.reset: se devuelve `NO_COMMITTED_OFFSET` y lag null, incluso si el log está vacío. Si el commit quedó antes del inicio retenido, se informa `OFFSET_EXPIRED`; si supera el último offset, `OFFSET_AHEAD_OF_LOG`. Offsets de log no disponibles producen `LOG_OFFSET_UNAVAILABLE` cuando la respuesta puede representarse; fallos de las consultas se traducen a error HTTP.

`totalLag` solo se suma si todas las particiones conocidas tienen lag válido y el total cabe en Long; en caso contrario `complete=false` y total null. Las consultas de grupo, commits e inicio/fin no son una instantánea atómica: observar `observedAt` y considerar rebalances/producción concurrente. No se adivinan tópicos de un grupo inactivo sin asignaciones ni offsets. No se modifican commits para medir lag.

## Errores y salud

| HTTP | Significado |
| --- | --- |
| 400 | Payload, nombre, configuración o incremento inválido. |
| 401 | Autenticación ausente/incorrecta al acceder al API. |
| 403 | Falta de rol/CSRF, escrituras desactivadas, recurso protegido, confirmación incorrecta o ACL Kafka denegada. |
| 404 | Tópico o grupo inexistente. |
| 409 | Tópico existente o conflicto de estado de particiones. |
| 503 | Kafka no disponible, fallo administrativo no recuperado o timeout. |
| 500 | Fallo interno inesperado, sin detalles internos en la respuesta. |

Una escritura sin CSRF puede rechazarse antes de evaluar HTTP Basic. El orden de protecciones puede producir 403 en ese caso. Los errores JSON incluyen status, code y message; los de negocio incluyen timestamp. Los logs registran operación/categoría de fallo y actor/tópico de escrituras aceptadas, sin contraseñas ni payloads de mensajes.

`/actuator/health` comprueba comunicación con Kafka; `health/readiness` incluye Kafka y `health/liveness` no requiere su disponibilidad. Health UP indica acceso a metadata, **no certifica que todos los ISR estén completos**. Inspeccionar tópicos para eso. No se prueban fallos de brokers deteniendo servicios existentes.

## Réplicas y tolerancia a fallos

El factor se define al crear. Aumentar particiones no cambia la replicación de las existentes. Cambiar factor requiere un plan de reasignación de réplicas; no se simula con una propiedad de tópico. Fuera de esta API, un operador debe preparar un plan explícito de brokers por partición, revisar capacidad/ISR, aplicar con herramientas Kafka oficiales (por ejemplo kafka-reassign-partitions), verificar finalización y retirar throttles temporales. No se ejecutó ningún plan ni se habilita esta operación en el servicio.

El administrador se conecta a varios bootstrap servers y Kafka descubre la metadata. Las tres réplicas y min ISR=2 pertenecen a la infraestructura existente; AdminClient no puede garantizar disponibilidad si faltan brokers suficientes. El controller informado en este clúster corresponde al modo ZooKeeper; no se usan APIs de quorum KRaft.

## Despliegue AWS

El Compose central de `infra-barriodigital/aws/compose.apps.yml` construye esta
imagen a partir del checkout en la EC2 Apps. Usar la guía `README.AWS.md` de
infraestructura para preparar las variables y ejecutar el despliegue por SSM.

Compose inyecta `KAFKA_BOOTSTRAP_SERVERS` y la cuenta HTTP. Mantiene el puerto 8089 interno, readiness con Kafka y escritura deshabilitada en AWS.

## Referencias y demostración

La guía paso a paso para EV5/EV6 está en [DEMO-EV5-EV6.md](DEMO-EV5-EV6.md). Los resultados reales están en [VALIDACION.md](VALIDACION.md).

APIs oficiales: [Kafka Admin](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/admin/Admin.html), [Spring Security CSRF](https://docs.spring.io/spring-security/reference/servlet/exploits/csrf.html) y [Spring Boot Actuator](https://docs.spring.io/spring-boot/reference/actuator/endpoints.html).
