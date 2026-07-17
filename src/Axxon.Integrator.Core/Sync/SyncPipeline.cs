using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Model;
using Microsoft.Extensions.Logging;

namespace Axxon.Integrator.Core.Sync;

/// <summary>
/// Orquestación de un evento de cambio de punta a punta:
/// filtro por empresa → resolución del vínculo (xref) → supresión de eco →
/// conflicto (last-writer-wins sobre el vínculo) → mapeo → upsert idempotente →
/// actualizar vínculo y estado.
/// Lo invoca el trigger de Service Bus del SyncEngine; acá no hay nada de Azure
/// para poder testearlo unitariamente.
/// </summary>
public sealed class SyncPipeline(
    IEntityMapStore mapStore,
    IXrefStore xrefStore,
    EchoGuard echoGuard,
    MappingEngine mappingEngine,
    IReadOnlyDictionary<string, IConnector> connectors,
    ISyncHistoryStore historyStore,
    ILogger<SyncPipeline> logger)
{
    /// <param name="deliveryCount">
    /// Número de entrega de Service Bus del mensaje (1 = primer intento); queda en el
    /// histórico para distinguir el primer fallo de los reintentos.
    /// </param>
    public async Task ProcessAsync(ChangeEvent evt, int deliveryCount, CancellationToken ct)
    {
        var maps = await mapStore.GetMapsForSourceAsync(evt.SourceSystem, evt.EntityName, ct);
        if (maps.Count == 0)
        {
            logger.LogDebug("Sin mapas activos para {System}/{Entity}, se descarta {CorrelationId}",
                evt.SourceSystem, evt.EntityName, evt.CorrelationId);
            return;
        }

        foreach (var map in maps)
        {
            try
            {
                await ProcessMapAsync(map, evt, deliveryCount, ct);
            }
            catch (Exception ex)
            {
                // El histórico registra el intento fallido y la excepción sigue su
                // camino (reintento del trigger → DLQ al agotar MaxDeliveryCount).
                await RecordAsync(map, evt, deliveryCount, SyncOutcome.Failed, targetRecordId: null, ex.Message, ct);
                throw;
            }
        }
    }

    private async Task ProcessMapAsync(EntityMap map, ChangeEvent evt, int deliveryCount, CancellationToken ct)
    {
        if (map.Companies.Count > 0)
        {
            if (evt.Company is not null && !map.Companies.Contains(evt.Company, StringComparer.OrdinalIgnoreCase))
            {
                return;
            }
            // Un delete sin empresa pasa el filtro: los deletes por ausencia de los
            // runs agendados no pueden conocer la empresa de un registro que ya no
            // existe, y si hay vínculo en el xref es porque este par ya sincronizó.
            if (evt.Company is null && evt.Operation != ChangeOperation.Delete)
            {
                return;
            }
        }

        // El vínculo se resuelve por PairKey: los dos sentidos de un bidireccional
        // comparten vínculo y estado, así el eco y el last-writer-wins cruzan sistemas.
        var link = await xrefStore.GetLinkAsync(map.PairKey, evt.SourceSystem, evt.SourceRecordId, ct);

        if (echoGuard.IsEcho(evt, link))
        {
            logger.LogInformation("Eco suprimido para {Map} {RecordId} ({CorrelationId})",
                map.Name, evt.SourceRecordId, evt.CorrelationId);
            await RecordAsync(map, evt, deliveryCount, SyncOutcome.EchoSuppressed, link?.RecordIdIn(map.TargetSystem), error: null, ct);
            return;
        }

        // Last-writer-wins sobre el vínculo completo: compara contra el último escritor
        // de cualquiera de los dos lados. Descarta rezagados (drenaje post sync inicial)
        // y resuelve conflictos concurrentes F&O/Dataverse. El skew de reloj entre
        // sistemas acota la precisión a segundos; suficiente para consistencia eventual.
        if (link?.State is { } state && evt.OccurredAt < state.LastWriterOccurredAt)
        {
            logger.LogWarning("Evento descartado por last-writer-wins para {Map} {RecordId}: {EventTime} < {LastWriter} de {WriterSystem}",
                map.Name, evt.SourceRecordId, evt.OccurredAt, state.LastWriterOccurredAt, state.LastWriterSystem);
            await RecordAsync(map, evt, deliveryCount, SyncOutcome.DiscardedByLastWriterWins, link.RecordIdIn(map.TargetSystem), error: null, ct);
            return;
        }

        if (!connectors.TryGetValue(map.TargetSystem, out var target))
        {
            throw new InvalidOperationException($"No hay conector registrado para el sistema destino '{map.TargetSystem}'.");
        }

        var targetRecordId = link?.RecordIdIn(map.TargetSystem);
        var payload = mappingEngine.Apply(map, evt, targetRecordId);

        if (evt.Operation == ChangeOperation.Delete)
        {
            await target.DeleteAsync(payload, ct);
            await RecordAsync(map, evt, deliveryCount, SyncOutcome.Deleted, targetRecordId, error: null, ct);
            if (link is not null)
            {
                // Tombstone: el vínculo se conserva con el estado del delete para que un
                // update rezagado del otro lado pierda por last-writer-wins en vez de
                // resucitar el registro.
                await xrefStore.SaveLinkAsync(link with
                {
                    State = new SyncState
                    {
                        WrittenToSystem = map.TargetSystem,
                        WrittenFields = [],
                        WrittenPayloadHash = string.Empty,
                        LastWriterSystem = evt.SourceSystem,
                        LastWriterOccurredAt = evt.OccurredAt,
                    },
                }, ct);
            }
            return;
        }

        var result = await target.UpsertAsync(payload, ct);

        link ??= new XrefLink
        {
            PairKey = map.PairKey,
            SystemA = evt.SourceSystem,
            RecordIdA = evt.SourceRecordId,
            SystemB = map.TargetSystem,
            RecordIdB = result.TargetRecordId,
        };

        // El hash se calcula sobre lo escrito (esquema del destino): es contra esto que
        // el EchoGuard comparará el evento-eco que el destino emita por esta escritura.
        await xrefStore.SaveLinkAsync(link with
        {
            State = new SyncState
            {
                WrittenToSystem = map.TargetSystem,
                WrittenFields = [.. payload.Fields.Keys],
                WrittenPayloadHash = EchoGuard.ComputeHash(payload.Fields),
                LastWriterSystem = evt.SourceSystem,
                LastWriterOccurredAt = evt.OccurredAt,
            },
        }, ct);

        logger.LogInformation("Sincronizado {Map}: {Source}/{RecordId} -> {Target} ({CorrelationId})",
            map.Name, evt.SourceSystem, evt.SourceRecordId, map.TargetSystem, evt.CorrelationId);

        await RecordAsync(map, evt, deliveryCount,
            result.Created ? SyncOutcome.Created : SyncOutcome.Updated, result.TargetRecordId, error: null, ct);
    }

    /// <summary>
    /// Registro best-effort en el histórico: informar el desenlace nunca puede tumbar
    /// (ni re-encolar) una sincronización que ya ocurrió — a lo sumo se pierde el
    /// renglón y queda el warning en App Insights.
    /// </summary>
    private async Task RecordAsync(EntityMap map, ChangeEvent evt, int deliveryCount,
        SyncOutcome outcome, string? targetRecordId, string? error, CancellationToken ct)
    {
        try
        {
            await historyStore.AppendAsync(new SyncHistoryEntry
            {
                MapName = map.Name,
                ProcessedAt = DateTimeOffset.UtcNow,
                Outcome = outcome,
                Operation = evt.Operation,
                SourceSystem = evt.SourceSystem,
                SourceRecordId = evt.SourceRecordId,
                TargetSystem = map.TargetSystem,
                TargetRecordId = targetRecordId,
                Company = evt.Company,
                OccurredAt = evt.OccurredAt,
                Error = error,
                DeliveryCount = deliveryCount,
                CorrelationId = evt.CorrelationId,
            }, ct);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "No se pudo registrar el histórico de {Map} para {RecordId} ({CorrelationId})",
                map.Name, evt.SourceRecordId, evt.CorrelationId);
        }
    }
}
