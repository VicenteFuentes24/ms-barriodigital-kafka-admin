# Demostración segura EV5 y EV6

Esta guía es un procedimiento **manual**. Las operaciones de escritura siguientes no fueron ejecutadas contra el clúster durante la implementación. Solo usar un tópico nuevo, exclusivo de la demostración; nunca events, analytics, smoke ni tópicos internos.

## 1. Iniciar el administrador en modo consulta

Compilar y arrancar según [README.md](README.md), con credenciales privadas y `KAFKA_ADMIN_WRITES_ENABLED=false`. No es necesario cambiar Requests, Report, RabbitMQ, BFF o React. Mantener el clúster existente en funcionamiento.

En otra terminal PowerShell, preparar autenticación sin poner la contraseña en el historial de comandos:

```powershell
$base = 'http://127.0.0.1:8089/api/kafka/admin'
$credential = Get-Credential -Message 'Credenciales configuradas en ms-kafka-admin'
$basic = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes(
    $credential.UserName + ':' + $credential.GetNetworkCredential().Password))
$headers = @{ Authorization = 'Basic ' + $basic }
$session = New-Object Microsoft.PowerShell.Commands.WebRequestSession

Invoke-RestMethod "$base/cluster" -Headers $headers -WebSession $session
Invoke-RestMethod "$base/brokers" -Headers $headers -WebSession $session
Invoke-RestMethod "$base/topics" -Headers $headers -WebSession $session
```

HTTP sin TLS se usa únicamente contra loopback local. En AWS usar URL HTTPS privada/restringida. No imprimir `$headers`, `$basic` ni la contraseña ni incluirlos en capturas de la presentación.

## 2. Mostrar particiones, réplicas, configuración y grupos

```powershell
$events = Invoke-RestMethod "$base/topics/bd.requests.events.v1" -Headers $headers -WebSession $session
$events.partitions | Format-Table partition,leader,replicas,isr,underReplicated
Invoke-RestMethod "$base/topics/bd.requests.events.v1/config" -Headers $headers -WebSession $session |
    Where-Object { $_.name -in @('retention.ms','cleanup.policy','min.insync.replicas') }

Invoke-RestMethod "$base/topics/bd.requests.analytics.v1" -Headers $headers -WebSession $session
Invoke-RestMethod "$base/consumer-groups" -Headers $headers -WebSession $session
Invoke-RestMethod "$base/consumer-groups/bd-report-v1" -Headers $headers -WebSession $session
Invoke-RestMethod "$base/consumer-groups/bd-report-v1/offsets" -Headers $headers -WebSession $session
$lag = Invoke-RestMethod "$base/consumer-groups/bd-report-v1/lag" -Headers $headers -WebSession $session
$lag.partitions | Format-Table topic,partition,committedOffset,earliestOffset,latestOffset,lag,status
$lag | Select-Object groupId,totalLag,complete,observedAt
```

Explicar: tres particiones distribuyen el trabajo; las tres réplicas mantienen copias; líder e ISR pueden cambiar. min ISR=2 con productores acks=all condiciona las confirmaciones. El grupo puede estar Stable o inactivo según Report. El administrador observa, pero no se une a bd-report-v1, no consume y no cambia offsets. Un lag null no significa cero: revisar el motivo por partición.

Comparar con Kafbat UI: mismos tópicos, líderes, réplicas, offsets y grupo, considerando que las consultas ocurren en instantes distintos. Kafbat también permite ver mensajes; esta API no expone payloads. No usar controles destructivos de Kafbat sobre tópicos de negocio. Las protecciones de esta API no se trasladan automáticamente a otras herramientas.

## 3. Habilitar una sesión controlada de escritura

En la terminal donde corre el administrador, Ctrl+C y reiniciar **solo ms-kafka-admin**:

```powershell
$env:KAFKA_ADMIN_WRITES_ENABLED = 'true'
java -jar .\target\ms-kafka-admin-0.0.1-SNAPSHOT.jar
```

En la terminal cliente, renovar sesión y obtener CSRF:

```powershell
$session = New-Object Microsoft.PowerShell.Commands.WebRequestSession
$csrf = Invoke-RestMethod "$base/csrf" -Headers $headers -WebSession $session
$headers[$csrf.headerName] = $csrf.token
$topic = 'bd.demo.kafka-admin.' + [guid]::NewGuid().ToString('N')
$createdByThisSession = $false
```

Los tópicos de negocio siguen bloqueados aunque la escritura esté habilitada. No demostrar esa protección enviando escrituras al clúster compartido: ya está cubierta por pruebas automatizadas con Kafka simulado.

## 4. Crear el tópico exclusivo de la demostración

```powershell
$body = @{
    name=$topic
    partitions=3
    replicationFactor=3
    retentionMs=3600000
    cleanupPolicy='delete'
    minInsyncReplicas=2
} | ConvertTo-Json
$creation = Invoke-RestMethod -Method Post -Uri "$base/topics" -Headers $headers -WebSession $session -ContentType 'application/json' -Body $body
if ($creation.status -eq 'ACKNOWLEDGED' -and $creation.topic -eq $topic) {
    $createdByThisSession = $true
}
Invoke-RestMethod "$base/topics/$topic" -Headers $headers -WebSession $session
```

Un 409 significa que ya existe: elegir otro nombre nuevo, no reutilizar ni eliminar el anterior. Si hay timeout/503, comprobar el estado antes de reintentar; el cambio puede haberse aplicado. Si no se puede acreditar la pertenencia del tópico a esta sesión, no ejecutar el borrado de abajo.

## 5. Cambiar retención y, opcionalmente, incrementar particiones

```powershell
$change = @{ values=@{ 'retention.ms'='1800000' } } | ConvertTo-Json
Invoke-RestMethod -Method Patch -Uri "$base/topics/$topic/config" -Headers $headers -WebSession $session -ContentType 'application/json' -Body $change
Invoke-RestMethod "$base/topics/$topic/config" -Headers $headers -WebSession $session |
    Where-Object { $_.name -eq 'retention.ms' }

# Opcional para demostrar el incremento, solo sobre el tópico recién creado:
$increase = @{ totalPartitions=4 } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "$base/topics/$topic/partitions" -Headers $headers -WebSession $session -ContentType 'application/json' -Body $increase
Invoke-RestMethod "$base/topics/$topic" -Headers $headers -WebSession $session
```

La retención está expresada en milisegundos. Su aplicación a segmentos no equivale a purga instantánea. No se reducen particiones ni se cambia el factor de replicación mediante ese endpoint. Para replicación se necesita una reasignación de réplicas, fuera del alcance de esta API. No se utiliza producción de mensajes para demostrar estas operaciones.

## 6. Eliminar únicamente el tópico propio, con confirmación

```powershell
if (-not $createdByThisSession -or -not $topic.StartsWith('bd.demo.kafka-admin.')) {
    throw 'No hay evidencia de creación de este tópico por la sesión; no borrar.'
}
$confirmation = Read-Host "Escriba el nombre exacto para eliminar únicamente $topic"
if ($confirmation -cne $topic) { throw 'Confirmación incorrecta; no se envió DELETE.' }
$deleteHeaders = @{}
foreach ($key in $headers.Keys) { $deleteHeaders[$key] = $headers[$key] }
$deleteHeaders['X-Confirm-Topic'] = $confirmation
Invoke-RestMethod -Method Delete -Uri "$base/topics/$topic" -Headers $deleteHeaders -WebSession $session
```

Esperar HTTP 202/ACKNOWLEDGED y comprobar después que GET del tópico devuelve 404 y desaparece de Kafbat. La metadata puede tardar en propagarse; no borrar otros recursos para resolver una demora. DELETE también depende de la configuración del broker existente; este servicio no la cambia.

## 7. Desactivar escritura y cerrar la sesión

Ctrl+C en el administrador y reiniciar:

```powershell
$env:KAFKA_ADMIN_WRITES_ENABLED = 'false'
java -jar .\target\ms-kafka-admin-0.0.1-SNAPSHOT.jar
```

La aplicación vuelve al modo consulta. Al terminar, Ctrl+C y limpiar variables sensibles en ambas terminales (`$headers=$null`, `$deleteHeaders=$null`, `$basic=$null`, `$credential=$null`, `$env:KAFKA_ADMIN_PASSWORD=$null`). No se eliminan volúmenes, mensajes ni tópicos preexistentes.

## Puntos para la presentación

- **RabbitMQ** conserva tareas, notificaciones, acknowledgements y DLQ de EP4. Este servicio no lo administra ni lo reemplaza.
- **Kafka** conserva un log por partición, permite múltiples consumer groups, offsets y retención independiente del consumo. Requests produce; Report proyecta/persiste y publica analítica; Kafka Admin administra metadata y recursos permitidos.
- **Disponibilidad**: bootstrap múltiple, réplicas e ISR; mostrar metadata sin detener brokers para demostrarlo. Kafka Admin no inventa métricas de CPU/memoria ni promete tolerancia a fallos basada solo en health UP.
- **Seguridad**: Basic bajo TLS en despliegue, ADMIN, CSRF, contraseña obligatoria, escrituras desactivadas, tópicos protegidos y confirmación exacta de DELETE.
- **Límites**: los resultados son observaciones, no una instantánea transaccional; las escrituras pueden propagarse de forma eventual. No hay modificación de offsets ni simulación de reasignación de réplicas.
