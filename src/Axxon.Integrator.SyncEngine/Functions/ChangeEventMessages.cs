using System.Security.Cryptography;
using System.Text;
using Azure.Messaging.ServiceBus;
using Axxon.Integrator.Core.Model;
using Axxon.Integrator.Core.Sync;

namespace Axxon.Integrator.SyncEngine.Functions;

/// <summary>
/// Convenciones de publicación de un <see cref="ChangeEvent"/> al topic 'changes',
/// compartidas por la ingesta (data events) y los runs agendados (pull):
///  - SessionId = sistema:entidad:registro → orden por registro en el motor, e
///    intercalado correcto cuando un mismo registro llega por las dos vías.
///  - MessageId determinístico → el duplicate detection del topic dedupea re-envíos
///    del origen y el solapamiento del pull incremental (watermark inclusiva).
/// </summary>
public static class ChangeEventMessages
{
    public static ServiceBusMessage Envelope(ChangeEvent evt) => new(BinaryData.FromObjectAsJson(evt))
    {
        SessionId = $"{evt.SourceSystem}:{evt.EntityName}:{evt.SourceRecordId}",
        MessageId = DeterministicMessageId(evt),
        CorrelationId = evt.CorrelationId,
        ContentType = "application/json",
    };

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
