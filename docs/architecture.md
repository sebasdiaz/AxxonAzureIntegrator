# AxxonAzureIntegrator — Arquitectura

Integrador de datos bidireccional near-real-time en Azure. Replica la funcionalidad de
Microsoft Dual Write (F&O ↔ Dataverse) pero desacoplado, extensible a cualquier sistema
y sin desarrollo sobre F&O.

## Decisiones de diseño

| # | Decisión | Razón |
|---|----------|-------|
| 1 | Sync **asíncrono near-real-time** (segundos), no síncrono como Dual Write | Resiliencia: si un destino está caído, los eventos esperan en la cola (ver decisión 11 para el mecanismo concreto). El modelo síncrono de Dual Write requiere hooks en el kernel de F&O y acopla la disponibilidad de ambos sistemas. Se acepta consistencia eventual. |
| 2 | Captura en F&O vía **data events nativos → Service Bus** | Cero desarrollo X++. F&O soporta Service Bus como endpoint de business/data events, con el secret en Key Vault. |
| 3 | Escritura en F&O vía **OData** sobre data entities | Tampoco requiere X++. Upsert idempotente con clave de entidad. |
| 4 | **Azure Service Bus** como backbone (no Event Grid solo) | Sesiones (orden por registro), dead-letter queue, duplicate detection, entrega programada (backoff). |
| 5 | **Sync inicial por DMF**, nunca por data events | Límite documentado: ~5.000 eventos/5 min y ~50.000/hora por ambiente F&O. |
| 6 | Mapas de entidades como **configuración versionada** (Cosmos DB), no código | Un consultor configura una integración sin deploy. Equivalente a las table maps de Dual Write. |
| 7 | Tabla de **cross-reference de IDs** propia | Dual Write depende de GUIDs compartidos F&O/Dataverse; con un tercer sistema eso no existe. |
| 8 | Todo sistema se integra implementando **`IConnector`** | La extensibilidad más allá de F&O/Dataverse es objetivo de primer orden. |
| 9 | Estado de sync **por vínculo lógico** (par del xref), no por lado | Si el estado se guarda por `(sistema, registro)`, cada dirección solo ve su propia historia: el eco por contenido nunca dispara (esquemas distintos, claves distintas) y un conflicto real F&O↔Dataverse nunca se detecta. El vínculo — identificado por el `PairKey` del par de entidades — es la unidad donde eco y last-writer-wins tienen sentido. |
| 10 | **Cola de ingesta + re-publish normalizado** delante del topic | Los endpoints de business events de F&O y los service endpoints de Dataverse **no permiten estampar `SessionId` ni `MessageId`**; publicar directo a una suscripción con sesiones dejaría mensajes inconsumibles. El salto de ingesta parsea al `ChangeEvent` normalizado, estampa `SessionId` (orden por registro) y `MessageId` determinístico (dedup real), y aísla los payloads corruptos en su propia DLQ. |
| 11 | Errores **permanentes → DLQ directa; transitorios → re-encolado programado con backoff** | El trigger de Service Bus reintenta sin backoff (y las retry policies de Functions no aplican a este trigger): con el destino caído, `MaxDeliveryCount` se quema en segundos y todo termina en la DLQ — lo contrario de "esperar en la cola". El re-encolado con `ScheduledEnqueueTime` implementa la espera; el desorden que introduce lo absorbe el last-writer-wins. |

## Diagrama

```mermaid
flowchart TB
    FO["Dynamics 365 F&O"] <--> CONN
    DV["Dataverse"] <--> CONN
    OTROS["Otros sistemas (futuro)"] <--> CONN
    CONN["Capa de conectores (IConnector + parsers)"] --> INGEST
    INGEST["Service Bus queue 'ingest'\npayloads nativos · DLQ de imparseables"] --> INGF
    INGF["IngestProcessor (Function)\nparse → SessionId → MessageId determinístico"] --> SB
    SB["Service Bus topic 'changes'\nChangeEvent normalizado · sesiones · dedup · DLQ"] --> ENGINE
    ENGINE["Motor de sincronización (Functions)\nfiltro → vínculo → eco → conflicto → mapeo → upsert"] --> CONN
    ENGINE -.-> MAPS[("Config de mapas\nCosmos DB")]
    ENGINE -.-> XREF[("Vínculos xref + estado\nCosmos DB")]
    ENGINE -.-> AI[("App Insights")]
```

## Flujo vivo (ejemplo F&O → Dataverse)

1. Un usuario modifica un cliente en F&O. El data event (activado sobre la data entity,
   con change tracking habilitado) se publica a la cola `ingest`. El payload tiene forma
   `RemoteExecutionContext` (operación en `MessageName`, campos en
   `InputParameters.Target`, `PreImage` en updates).
2. `IngestProcessor` (Function, sin sesiones) identifica el sistema origen, parsea con
   su `IChangeEventParser` al `ChangeEvent` normalizado y re-publica al topic `changes`
   con `SessionId = sistema:entidad:registro` y `MessageId` determinístico (mismo evento
   → mismo ID → el duplicate detection absorbe re-envíos). Un payload imparseable es
   error permanente y muere en la DLQ de `ingest`.
3. `ChangeEventProcessor` (Function con trigger de Service Bus, sesiones habilitadas)
   recibe el evento: dos cambios al mismo registro se procesan en orden.
4. `SyncPipeline` ejecuta por cada mapa activo:
   - **Filtro por empresa** (equivalente al filtro por legal entity de Dual Write).
   - **Resolución del vínculo**: el `IXrefStore` busca el `XrefLink` por
     `(PairKey, sistema, registro)`. El vínculo une los dos registros del par y carga
     el `SyncState` del último sync — compartido por ambas direcciones.
   - **Supresión de eco** (`EchoGuard`): descarta si lo originó el usuario de
     integración (defensa primaria), o si el evento viene del sistema donde el motor
     escribió por última vez y la proyección del payload sobre los campos escritos
     coincide en hash con lo escrito (defensa en profundidad).
   - **Conflictos**: last-writer-wins sobre el vínculo completo — el `OccurredAt` del
     evento se compara contra el del último escritor *de cualquiera de los dos lados*.
     Descarta rezagados y resuelve ediciones concurrentes F&O↔Dataverse.
   - **Mapeo** (`MappingEngine`): campos, value maps (option sets; sin traducción y sin
     default = error permanente), transformaciones con nombre, defaults.
   - **Upsert idempotente** vía el `IConnector` destino; si el vínculo no existía, el
     conector resuelve por clave natural antes de crear (Service Bus es at-least-once:
     nunca hacer create ciego).
   - **Actualizar el vínculo**: nuevo `SyncState` con el hash de *lo escrito* (esquema
     del destino), los campos escritos, y sistema/`OccurredAt` del escritor.
5. Excepciones transitorias (destino caído, throttling) → re-encolado programado con
   backoff exponencial; permanentes (config/datos inválidos) → DLQ inmediata con razón.
   La DLQ se revisa y reprocesa desde el portal de administración (fase 4).

## Vínculos y estado de sincronización (xref)

La unidad de estado es el **vínculo** (`XrefLink`): el par de registros que un par de
entidades mantiene sincronizados, identificado por `PairKey` (identidad canónica del
par, independiente de la dirección — los mapas A→B y B→A comparten `PairKey`).

El `SyncState` del vínculo guarda:

| Campo | Uso |
|-------|-----|
| `WrittenToSystem` | Un evento que llega *desde* este sistema es candidato a eco. |
| `WrittenFields` + `WrittenPayloadHash` | El evento-eco se proyecta sobre los campos escritos (mismo esquema: el del sistema que reporta) y se compara por hash. Clave ausente se proyecta como null — recupera el gotcha de F&O que omite datetimes NULL. |
| `LastWriterSystem` + `LastWriterOccurredAt` | Last-writer-wins entre sistemas. Un delete deja tombstone (vínculo con estado, sin campos) para que un update rezagado del otro lado no resucite el registro. |

Notas de diseño:

- **Precisión del last-writer-wins**: `OccurredAt` viene de relojes de dos sistemas
  distintos; el skew acota la precisión a segundos. Aceptable para consistencia
  eventual; ediciones simultáneas dentro de la ventana de skew se resuelven de forma
  arbitraria pero convergente (el estado del vínculo es uno solo).
- **Persistencia**: dos documentos espejo por vínculo en Cosmos (uno por lado), con
  partition key `lookupKey = pairKey|system|recordId` → point reads O(1) desde
  cualquier lado y distribución pareja durante el sync inicial. Consistencia entre
  espejos por ETag; las sesiones hacen único al escritor por lado, así que la
  contención real es solo entre direcciones y se resuelve con retry optimista.
- **Hash de contenido**: se calcula sobre el payload *mapeado* (lo que se escribió),
  nunca sobre el crudo del origen — un cambio en un campo no mapeado no genera tráfico.
  La normalización de representación de valores (números, fechas) entre lo escrito y lo
  que el sistema reporta queda para la fase 2; un falso negativo no pierde datos porque
  la defensa por identidad contiene el loop y el upsert es idempotente.

## Sync inicial + cutover

1. Activar la captura de eventos **antes** de la copia masiva (los cambios durante la
   copia quedan encolados).
2. Bulk copy con `ExportAsync` del conector origen (F&O: DMF package API; Dataverse:
   Web API paginada), orquestado con Durable Functions.
3. Al terminar, drenar la cola: el last-writer-wins del pipeline descarta los eventos
   más viejos que el registro copiado.

## Restricciones conocidas (documentación oficial)

- Data events F&O: ~5.000 eventos/5 min, ~50.000/hora por ambiente. Updates más caros
  que creates/deletes. Dimensionar y alertar sobre este límite.
- Change tracking **obligatorio** en la data entity para que los data events disparen.
- Los datetime en NULL **se omiten** del payload del data event → el motor de mapeo
  trata "clave ausente" ≠ "valor null" y nunca pisa el destino por ausencia; el
  EchoGuard proyecta ausente-como-null al comparar.
- Los endpoints de eventos de F&O y Dataverse **no estampan `SessionId`/`MessageId`**
  → de ahí la cola de ingesta (decisión 10). Validar con un spike temprano el formato
  exacto que cada uno entrega en la cola.
- F&O tiene retry propio + log de errores con "Resend" para fallos de entrega al bus
  (el `MessageId` determinístico de la ingesta dedupea esos re-envíos).
- Endpoint de Service Bus en F&O requiere: namespace propio (tier Standard+ para
  topics), secret en Key Vault, app registration de Entra ID con acceso Get/List.
- Las retry policies de Azure Functions **no aplican al trigger de Service Bus**: el
  backoff se implementa re-encolando con `ScheduledEnqueueTime` (decisión 11).
- Catch-up por polling contra F&O: OData no expone change tracking incremental
  directo; validar la estrategia (DMF incremental / recurring integrations) antes de
  apoyarse en `PullChangesAsync` para F&O.

## Estructura de la solución

| Proyecto | Rol |
|----------|-----|
| `Axxon.Integrator.Core` | Contratos (`IConnector`, stores), modelos (`ChangeEvent`, `EntityMap`, `XrefLink`), motor (`SyncPipeline`, `MappingEngine`, `EchoGuard`). Sin dependencias de Azure: testeable unitariamente. |
| `Axxon.Integrator.Connectors.FinOps` | Parser de data events + escritura OData + export DMF. |
| `Axxon.Integrator.Connectors.Dataverse` | Parser de service endpoints + escritura Web API + change tracking. |
| `Axxon.Integrator.SyncEngine` | Azure Functions (isolated, .NET 8): `IngestProcessor` (normaliza y estampa sesión/dedup) y `ChangeEventProcessor` (ejecuta el pipeline). |
| `infra/` | Bicep: Service Bus (cola `ingest` + topic `changes`), Cosmos DB, Function App, Key Vault, App Insights. |

## Fases

1. **MVP unidireccional**: Dataverse → F&O, una entidad (clientes). Valida el pipeline
   completo, incluida la ingesta (spike del formato real de los payloads) y la
   clasificación de errores permanente/transitorio.
2. **Bidireccional**: F&O → Dataverse + eco + conflictos sobre el estado por vínculo.
   Endurecer la normalización de valores del hash de contenido.
3. **Sync inicial** con cutover (DMF / Durable Functions).
4. **Portal de administración**: pausar/reanudar mapas, DLQ, reprocesos, métricas.
5. **Tercer conector** (SQL o REST genérico): prueba de fuego de la abstracción `IConnector`.
