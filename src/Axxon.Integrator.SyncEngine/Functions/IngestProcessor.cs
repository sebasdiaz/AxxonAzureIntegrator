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
    private readonly IReadOnlyList<IChangeEventParser> _parsers = [.. parsers];

    [Function(nameof(IngestProcessor))]
    public async Task RunAsync(
        [ServiceBusTrigger("%Sync:IngestQueue%", Connection = "ServiceBusConnection")]
        ServiceBusReceivedMessage message,
        CancellationToken ct)
    {
        ChangeEvent evt;
        try
        {
            var parser = SelectParser(message);
            evt = parser.Parse(message.Body);
        }
        catch (Exception ex)
        {
            // El error puntual y un prefijo del payload en una sola línea: lo primero
            // que se necesita ver en consola cuando un mensaje termina en la DLQ.
            logger.LogError("Ingesta fallida para el mensaje {MessageId}: {Error} | Payload: {Payload}",
                message.MessageId, ex.Message, Truncate(message.Body.ToString(), 600));
            throw;
        }

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

    /// <summary>
    /// F&O y Dataverse no ponen application properties custom en el mensaje: el origen
    /// se identifica dejando que cada parser huela el payload (<see cref="IChangeEventParser.CanParse"/>).
    /// Gana el primero en orden de registro; si nadie lo reconoce es error permanente
    /// (payload ajeno o corrupto) y muere en la DLQ de 'ingest'.
    /// </summary>
    private IChangeEventParser SelectParser(ServiceBusReceivedMessage message) =>
        _parsers.FirstOrDefault(p => p.CanParse(message.Body))
        ?? throw new FormatException(
            $"Ningún parser registrado ({string.Join(", ", _parsers.Select(p => p.SystemName))}) reconoce el payload del mensaje {message.MessageId}.");

    private static string Truncate(string text, int max) =>
        text.Length <= max ? text : text[..max] + "…";

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
