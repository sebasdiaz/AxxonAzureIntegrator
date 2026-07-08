using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace Axxon.Integrator.SyncEngine.Functions;

/// <summary>
/// Punto de entrada del flujo vivo: consume los eventos de cambio que F&O (data events)
/// y Dataverse (service endpoints) publican en el topic. Sesiones habilitadas para
/// garantizar orden por registro; los reintentos agotados van a la dead-letter queue.
/// </summary>
public sealed class ChangeEventProcessor(ILogger<ChangeEventProcessor> logger)
{
    [Function(nameof(ChangeEventProcessor))]
    public Task RunAsync(
        [ServiceBusTrigger("%Sync:ChangesTopic%", "%Sync:EngineSubscription%",
            Connection = "ServiceBusConnection", IsSessionsEnabled = true)]
        ServiceBusReceivedMessage message,
        CancellationToken ct)
    {
        logger.LogInformation("Evento recibido: MessageId={MessageId} SessionId={SessionId}",
            message.MessageId, message.SessionId);

        // TODO(MVP):
        // 1. Identificar el sistema origen (application property del mensaje o suscripción dedicada).
        // 2. Parsear con el IChangeEventParser correspondiente -> ChangeEvent.
        // 3. Delegar en SyncPipeline.ProcessAsync(evt, ct).
        // Excepciones no controladas => reintento de Service Bus => DLQ al agotarse.
        throw new NotImplementedException("Cablear parser + SyncPipeline. Fase 1 (MVP).");
    }
}
