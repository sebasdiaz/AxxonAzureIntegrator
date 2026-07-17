using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Model;
using Microsoft.Extensions.Logging;

namespace Axxon.Integrator.Core.Sync;

/// <summary>
/// Un run agendado de un mapa, de punta a punta: pull del origen desde la watermark →
/// publicar cada cambio como <see cref="ChangeEvent"/> al flujo normalizado →
/// detección de deletes por ausencia contra el xref → avanzar la watermark.
///
/// El run NO escribe en el destino: publica eventos y deja que el pipeline los procese
/// como a cualquier data event (sesiones por registro, eco, last-writer-wins, xref,
/// histórico, DLQ). Por eso la watermark avanza cuando los eventos quedaron
/// *encolados*, no procesados: si el run muere a mitad, el próximo re-trae el
/// solapamiento y lo absorben la dedupe y el upsert idempotente.
/// Lo invoca el ScheduledRunProcessor del SyncEngine; acá no hay nada de Azure.
/// </summary>
public sealed class ScheduledRunService(
    IReadOnlyDictionary<string, IConnector> connectors,
    IWatermarkStore watermarkStore,
    IXrefStore xrefStore,
    IChangeEventPublisher publisher,
    ILogger<ScheduledRunService> logger)
{
    /// <param name="scheduledFor">
    /// Ocurrencia del cron que disparó el run. Es el OccurredAt de los deletes por
    /// ausencia (el momento real del borrado en el origen es incognoscible).
    /// </param>
    public async Task RunAsync(EntityMap map, DateTimeOffset scheduledFor, CancellationToken ct)
    {
        if (map.Schedule is not { } schedule)
        {
            logger.LogWarning("Run agendado ignorado: el mapa {Map} ya no tiene programación.", map.Name);
            return;
        }
        if (!connectors.TryGetValue(map.SourceSystem, out var source))
        {
            throw new InvalidOperationException($"No hay conector registrado para el sistema origen '{map.SourceSystem}'.");
        }

        var query = new EntityQuery { EntityName = map.SourceEntity, Companies = map.Companies };
        var since = schedule.Mode == ScheduledRunMode.Incremental
            ? await watermarkStore.GetAsync(map.SourceSystem, WatermarkScope(map), ct) ?? Watermark.Start(map.SourceSystem, WatermarkScope(map))
            : Watermark.Start(map.SourceSystem, WatermarkScope(map));

        // En full export sin filtro de empresas, el propio pull enumera las claves
        // vivas; si hay filtro no sirve (la ausencia debe medirse contra TODO el
        // origen, o los registros de otras empresas parecerían borrados).
        var canReuseLiveIds = schedule.Mode == ScheduledRunMode.FullExport && map.Companies.Count == 0;
        var liveIds = schedule.DetectDeletes && canReuseLiveIds
            ? new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            : null;

        var published = 0;
        var maxOccurred = DateTimeOffset.MinValue;
        await foreach (var evt in source.PullChangesAsync(query, since, ct))
        {
            await publisher.PublishAsync(evt, ct);
            liveIds?.Add(evt.SourceRecordId);
            if (evt.OccurredAt > maxOccurred)
            {
                maxOccurred = evt.OccurredAt;
            }
            published++;
        }

        var deletes = 0;
        if (schedule.DetectDeletes)
        {
            if (liveIds is null)
            {
                liveIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var keysScan = new EntityQuery { EntityName = map.SourceEntity, KeysOnly = true };
                await foreach (var evt in source.PullChangesAsync(keysScan, Watermark.Start(map.SourceSystem, WatermarkScope(map)), ct))
                {
                    liveIds.Add(evt.SourceRecordId);
                }
            }
            deletes = await PublishDeletesAsync(map, liveIds, scheduledFor, ct);
        }

        // Token genérico: ISO 8601 del OccurredAt máximo publicado. Los conectores lo
        // interpretan como "modificado desde"; sin eventos nuevos, la watermark no se
        // toca (avanzarla al reloj del run perdería cambios con reloj rezagado).
        if (schedule.Mode == ScheduledRunMode.Incremental && published > 0)
        {
            await watermarkStore.SaveAsync(
                new Watermark(map.SourceSystem, WatermarkScope(map), maxOccurred.ToString("O"), DateTimeOffset.UtcNow), ct);
        }

        logger.LogInformation("Run agendado de {Map} ({Mode}): {Published} cambios publicados, {Deletes} deletes por ausencia.",
            map.Name, schedule.Mode, published, deletes);
    }

    /// <summary>
    /// Deletes por ausencia: lo que el xref del par conoce del lado origen y no está
    /// entre las claves vivas se publica como Delete. Los tombstones (deletes ya
    /// propagados) se saltean para no re-emitir en cada run.
    /// </summary>
    private async Task<int> PublishDeletesAsync(EntityMap map, IReadOnlySet<string> liveIds, DateTimeOffset scheduledFor, CancellationToken ct)
    {
        var deletes = 0;
        await foreach (var link in xrefStore.GetLinksForPairAsync(map.PairKey, ct))
        {
            var sourceRecordId = link.RecordIdIn(map.SourceSystem);
            if (sourceRecordId is null || liveIds.Contains(sourceRecordId) || link.State?.IsTombstone == true)
            {
                continue;
            }

            await publisher.PublishAsync(new ChangeEvent
            {
                SourceSystem = map.SourceSystem,
                EntityName = map.SourceEntity,
                SourceRecordId = sourceRecordId,
                Operation = ChangeOperation.Delete,
                OccurredAt = scheduledFor,
                Company = null, // la empresa de un registro ausente es incognoscible
                Data = new Dictionary<string, object?>(),
            }, ct);
            deletes++;
        }
        return deletes;
    }

    /// <summary>
    /// La watermark vive por mapa, no por entidad: dos mapas agendados sobre la misma
    /// entidad origen avanzan cada uno su propio progreso.
    /// </summary>
    private static string WatermarkScope(EntityMap map) => $"map:{map.Name}";
}
