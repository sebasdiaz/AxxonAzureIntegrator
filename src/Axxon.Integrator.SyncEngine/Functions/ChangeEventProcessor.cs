using Azure.Messaging.ServiceBus;
using Axxon.Integrator.Core.Model;
using Axxon.Integrator.Core.Sync;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace Axxon.Integrator.SyncEngine.Functions;

/// <summary>
/// Segundo salto del flujo vivo: consume los <see cref="ChangeEvent"/> ya normalizados
/// que el <see cref="IngestProcessor"/> publica en el topic. Sesiones habilitadas para
/// garantizar orden por registro (SessionId estampado en la ingesta).
///
/// Manejo de errores (ver architecture.md):
/// - Permanentes (mapa/valor inválido, payload corrupto) → dead-letter inmediato con
///   razón; reintentarlos solo quema entregas.
/// - Transitorios (destino caído, throttling) → re-encolar una copia con
///   ScheduledEnqueueTime y backoff exponencial, completando el original; el
///   last-writer-wins del pipeline absorbe el desorden que introduce el re-encolado.
/// TODO(fase 1): implementar esa clasificación; hoy toda excepción sigue el camino
/// default reintento inmediato → DLQ al agotar MaxDeliveryCount.
/// </summary>
public sealed class ChangeEventProcessor(SyncPipeline pipeline, ILogger<ChangeEventProcessor> logger)
{
    [Function(nameof(ChangeEventProcessor))]
    public async Task RunAsync(
        [ServiceBusTrigger("%Sync:ChangesTopic%", "%Sync:EngineSubscription%",
            Connection = "ServiceBusConnection", IsSessionsEnabled = true)]
        ServiceBusReceivedMessage message,
        CancellationToken ct)
    {
        var evt = message.Body.ToObjectFromJson<ChangeEvent>()
            ?? throw new FormatException($"Mensaje {message.MessageId} sin ChangeEvent deserializable.");

        logger.LogInformation("Evento recibido: {System}/{Entity}/{RecordId} {Operation} SessionId={SessionId} ({CorrelationId})",
            evt.SourceSystem, evt.EntityName, evt.SourceRecordId, evt.Operation, message.SessionId, evt.CorrelationId);

        try
        {
            await pipeline.ProcessAsync(evt, ct);
        }
        catch (Exception ex)
        {
            // Resumen en una línea antes del stack trace del host: qué evento falló,
            // en qué entrega va (DeliveryCount de MaxDeliveryCount) y el error del
            // destino — la respuesta a "¿por qué no se insertó?" sin bucear en la DLQ.
            logger.LogError("Sync fallida para {System}/{Entity}/{RecordId} {Operation} (entrega {DeliveryCount}, {CorrelationId}): {Error}",
                evt.SourceSystem, evt.EntityName, evt.SourceRecordId, evt.Operation, message.DeliveryCount, evt.CorrelationId, ex.Message);
            throw;
        }
    }
}
