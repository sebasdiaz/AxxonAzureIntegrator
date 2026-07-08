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

        // El vínculo se resuelve por PairKey: los dos sentidos de un bidireccional
        // comparten vínculo y estado, así el eco y el last-writer-wins cruzan sistemas.
        var link = await xrefStore.GetLinkAsync(map.PairKey, evt.SourceSystem, evt.SourceRecordId, ct);

        if (echoGuard.IsEcho(evt, link))
        {
            logger.LogInformation("Eco suprimido para {Map} {RecordId} ({CorrelationId})",
                map.Name, evt.SourceRecordId, evt.CorrelationId);
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
    }
}
