using System.Security.Cryptography;
using System.Text;
using Azure.Messaging.ServiceBus;
using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Model;
using Axxon.Integrator.Core.Sync;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace Axxon.Integrator.SyncEngine.Functions;

/// <summary>
/// Primer salto del flujo vivo. F&O (data events) y Dataverse (service endpoints)
/// publican sus payloads nativos en la cola 'ingest' — sus endpoints no permiten
/// estampar SessionId ni MessageId, así que no pueden publicar directo a un topic con
/// sesiones. Acá se parsea al <see cref="ChangeEvent"/> normalizado y se re-publica al
/// topic 'changes' con:
///  - SessionId = sistema:entidad:registro → orden por registro en el motor.
///  - MessageId determinístico → el duplicate detection del topic dedupea en serio
///    (re-envíos del "Resend" de F&O, doble entrega at-least-once de la ingesta).
/// Un payload imparseable es error permanente: va a la DLQ de 'ingest' sin quemar
/// reintentos útiles.
/// </summary>
public sealed class IngestProcessor(
    IEnumerable<IChangeEventParser> parsers,
    ServiceBusSender changesTopicSender,
    ILogger<IngestProcessor> logger)
{
    private readonly IReadOnlyDictionary<string, IChangeEventParser> _parsersBySystem =
        parsers.ToDictionary(p => p.SystemName, StringComparer.OrdinalIgnoreCase);

    [Function(nameof(IngestProcessor))]
    public async Task RunAsync(
        [ServiceBusTrigger("%Sync:IngestQueue%", Connection = "ServiceBusConnection")]
        ServiceBusReceivedMessage message,
        CancellationToken ct)
    {
        var parser = SelectParser(message);
        var evt = parser.Parse(message.Body);

        var outgoing = new ServiceBusMessage(BinaryData.FromObjectAsJson(evt))
        {
            SessionId = $"{evt.SourceSystem}:{evt.EntityName}:{evt.SourceRecordId}",
            MessageId = DeterministicMessageId(evt),
            CorrelationId = evt.CorrelationId,
            ContentType = "application/json",
        };

        await changesTopicSender.SendMessageAsync(outgoing, ct);

        logger.LogInformation("Ingesta normalizada: {System}/{Entity}/{RecordId} {Operation} ({CorrelationId})",
            evt.SourceSystem, evt.EntityName, evt.SourceRecordId, evt.Operation, evt.CorrelationId);
    }

    private IChangeEventParser SelectParser(ServiceBusReceivedMessage message) =>
        // TODO(MVP): F&O y Dataverse no ponen application properties custom en el
        // mensaje; identificar el origen por la forma del payload (BusinessEventId /
        // RemoteExecutionContext) o por colas de ingesta dedicadas por sistema.
        throw new NotImplementedException("Selección de parser por origen. Fase 1 (MVP).");

    /// <summary>
    /// Mismo evento → mismo MessageId, para que el duplicate detection del topic
    /// (ventana de 10 min) absorba re-entregas y re-envíos del origen.
    /// </summary>
    private static string DeterministicMessageId(ChangeEvent evt)
    {
        var composite = $"{evt.SourceSystem}|{evt.EntityName}|{evt.SourceRecordId}|{(int)evt.Operation}|{evt.OccurredAt.UtcTicks}|{EchoGuard.ComputeHash(evt.Data)}";
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(composite)));
    }
}
