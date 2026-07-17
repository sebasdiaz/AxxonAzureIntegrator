using Azure.Messaging.ServiceBus;
using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Model;
using Axxon.Integrator.Core.Sync;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace Axxon.Integrator.SyncEngine.Functions;

/// <summary>
/// Ejecuta los runs que encola el <see cref="ScheduleDispatcher"/>. Sesiones
/// habilitadas (SessionId = mapa): un mapa nunca corre dos runs a la vez, sin
/// necesidad de leases. El mapa se relee acá — entre el tick y el consumo pudo
/// pausarse, borrarse o perder la programación, y en ese caso el run se descarta.
/// Un run que falla sigue el camino estándar: reintento del trigger → DLQ al agotar
/// MaxDeliveryCount; como la watermark solo avanza al final, el reintento re-trae
/// todo y no pierde nada.
/// </summary>
public sealed class ScheduledRunProcessor(
    IEntityMapStore mapStore,
    ScheduledRunService runService,
    ILogger<ScheduledRunProcessor> logger)
{
    [Function(nameof(ScheduledRunProcessor))]
    public async Task RunAsync(
        [ServiceBusTrigger("%Sync:ScheduledRunsQueue%", Connection = "ServiceBusConnection", IsSessionsEnabled = true)]
        ServiceBusReceivedMessage message,
        CancellationToken ct)
    {
        var request = message.Body.ToObjectFromJson<ScheduledRunRequest>()
            ?? throw new FormatException($"Mensaje {message.MessageId} sin ScheduledRunRequest deserializable.");

        var map = await mapStore.GetAsync(request.MapName, ct);
        if (map is null || map.Status != MapStatus.Active || map.Schedule is null)
        {
            logger.LogInformation("Run agendado descartado: el mapa {Map} ya no existe, está pausado o perdió la programación.",
                request.MapName);
            return;
        }

        try
        {
            await runService.RunAsync(map, request.ScheduledFor, ct);
        }
        catch (Exception ex)
        {
            logger.LogError("Run agendado fallido para {Map} (ocurrencia {Due:O}, entrega {DeliveryCount}): {Error}",
                request.MapName, request.ScheduledFor, message.DeliveryCount, ex.Message);
            throw;
        }
    }
}
