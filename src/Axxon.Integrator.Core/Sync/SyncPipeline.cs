using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Model;
using Microsoft.Extensions.Logging;

namespace Axxon.Integrator.Core.Sync;

/// <summary>
/// Orquestación de un evento de cambio de punta a punta:
/// filtro por empresa → supresión de eco → conflicto (last-writer-wins) →
/// resolución de identidad (xref) → mapeo → upsert idempotente → actualizar xref y estado.
/// Lo invoca el trigger de Service Bus del SyncEngine; acá no hay nada de Azure
/// para poder testearlo unitariamente.
/// </summary>
public sealed class SyncPipeline(
    IEntityMapStore mapStore,
    IXrefStore xrefStore,
    EchoGuard echoGuard,
    MappingEngine mappingEngine,
    IReadOnlyDictionary<string, IConnector> connectors,
    ILogger<SyncPipeline> logger)
{
    public async Task ProcessAsync(ChangeEvent evt, CancellationToken ct)
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
            await ProcessMapAsync(map, evt, ct);
        }
    }

    private async Task ProcessMapAsync(EntityMap map, ChangeEvent evt, CancellationToken ct)
    {
        if (map.Companies.Count > 0 && (evt.Company is null || !map.Companies.Contains(evt.Company, StringComparer.OrdinalIgnoreCase)))
        {
            return;
        }

        if (await echoGuard.IsEchoAsync(map, evt, ct))
        {
            logger.LogInformation("Eco suprimido para {Map} {RecordId} ({CorrelationId})",
                map.Name, evt.SourceRecordId, evt.CorrelationId);
            return;
        }

        // Last-writer-wins: un evento más viejo que el último sync es un rezagado
        // (típico al drenar la cola tras el sync inicial) y no debe pisar datos nuevos.
        var state = await xrefStore.GetSyncStateAsync(map.Name, evt.SourceSystem, evt.SourceRecordId, ct);
        if (state is not null && evt.OccurredAt < state.LastSyncedAt)
        {
            logger.LogWarning("Evento rezagado descartado para {Map} {RecordId}: {EventTime} < {LastSynced}",
                map.Name, evt.SourceRecordId, evt.OccurredAt, state.LastSyncedAt);
            return;
        }

        if (!connectors.TryGetValue(map.TargetSystem, out var target))
        {
            throw new InvalidOperationException($"No hay conector registrado para el sistema destino '{map.TargetSystem}'.");
        }

        var targetRecordId = await xrefStore.ResolveAsync(evt.SourceSystem, map.Name, evt.SourceRecordId, map.TargetSystem, ct);
        var payload = mappingEngine.Apply(map, evt, targetRecordId);

        if (evt.Operation == ChangeOperation.Delete)
        {
            await target.DeleteAsync(payload, ct);
        }
        else
        {
            var result = await target.UpsertAsync(payload, ct);
            if (targetRecordId is null)
            {
                await xrefStore.LinkAsync(map.Name, evt.SourceSystem, evt.SourceRecordId, map.TargetSystem, result.TargetRecordId, ct);
            }
        }

        await xrefStore.SetSyncStateAsync(map.Name, evt.SourceSystem, evt.SourceRecordId,
            new SyncState(EchoGuard.ComputeHash(evt.Data), evt.OccurredAt, evt.SourceSystem), ct);

        logger.LogInformation("Sincronizado {Map}: {Source}/{RecordId} -> {Target} ({CorrelationId})",
            map.Name, evt.SourceSystem, evt.SourceRecordId, map.TargetSystem, evt.CorrelationId);
    }
}
