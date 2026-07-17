using Azure.Messaging.ServiceBus;
using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Model;
using Axxon.Integrator.Core.Sync;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace Axxon.Integrator.SyncEngine.Functions;

/// <summary>Mensaje de la cola 'scheduled-runs': qué mapa correr y qué ocurrencia del cron lo disparó.</summary>
public sealed record ScheduledRunRequest(string MapName, DateTimeOffset ScheduledFor);

/// <summary>Sender hacia la cola 'scheduled-runs' (wrapper: el ServiceBusSender pelado del contenedor es el del topic 'changes').</summary>
public sealed record ScheduledRunsSender(ServiceBusSender Sender);

/// <summary>
/// Reloj de los mapas agendados. Los timer triggers son estáticos al deploy, así que
/// no hay "un timer por mapa": un único tick por minuto evalúa el cron de cada mapa
/// activo y encola un <see cref="ScheduledRunRequest"/> por ocurrencia vencida.
///
/// El tick es stateless (ventana de 1 minuto hacia atrás) y la cola pone las
/// garantías: MessageId determinístico por ocurrencia → dedupe si dos ticks solapan
/// ventana; SessionId = mapa → los runs de un mismo mapa se procesan en serie aunque
/// uno tarde más que su intervalo. Si el host estuvo caído, las ocurrencias perdidas
/// se saltean a propósito — el run incremental arranca de la watermark y se pone al
/// día solo.
/// </summary>
public sealed class ScheduleDispatcher(
    IEntityMapStore mapStore,
    ScheduledRunsSender runsQueue,
    ILogger<ScheduleDispatcher> logger)
{
    [Function(nameof(ScheduleDispatcher))]
    public async Task RunAsync([TimerTrigger("0 * * * * *")] TimerInfo timer, CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;
        var windowStart = now.AddMinutes(-1);

        foreach (var map in await mapStore.GetAllAsync(ct))
        {
            if (map.Status != MapStatus.Active || map.Schedule is null)
            {
                continue;
            }

            DateTimeOffset? due;
            try
            {
                due = ScheduleEvaluator.LastDueInWindow(map.Schedule.Cron, windowStart, now);
            }
            catch (FormatException ex)
            {
                // Config inválida: se avisa y se sigue con el resto de los mapas — un
                // cron roto no puede frenar la agenda de los demás.
                logger.LogWarning("Cron inválido en el mapa {Map}: {Error}", map.Name, ex.Message);
                continue;
            }
            if (due is null)
            {
                continue;
            }

            var message = new ServiceBusMessage(BinaryData.FromObjectAsJson(new ScheduledRunRequest(map.Name, due.Value)))
            {
                SessionId = map.Name.ToLowerInvariant(),
                MessageId = $"{map.Name.ToLowerInvariant()}|{due.Value.UtcTicks}",
                ContentType = "application/json",
            };
            await runsQueue.Sender.SendMessageAsync(message, ct);

            logger.LogInformation("Run agendado encolado: {Map} (ocurrencia {Due:O})", map.Name, due.Value);
        }
    }
}
