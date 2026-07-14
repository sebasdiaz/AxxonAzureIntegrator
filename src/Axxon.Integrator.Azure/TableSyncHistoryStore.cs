using Azure.Data.Tables;
using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Model;

namespace Axxon.Integrator.Azure;

/// <summary>
/// Histórico de sincronización sobre Azure Table Storage (mismo storage account que
/// los mapas). PartitionKey = nombre del mapa (saneado: Tables prohíbe / \ # ?), así
/// la consulta de la pestaña Histórico es una lectura de partición. RowKey = ticks
/// invertidos de ProcessedAt + sufijo único: el orden natural de la partición queda
/// del más nuevo al más viejo, con paginación por rango sin ordenar en memoria.
/// Retención: pendiente (Tables no tiene TTL); ver architecture.md.
/// </summary>
public sealed class TableSyncHistoryStore(TableClient table) : ISyncHistoryStore
{
    /// <summary>Los errores de Dataverse/F&O traen respuestas enteras; el renglón guarda lo diagnosticable.</summary>
    private const int MaxErrorLength = 4000;

    private volatile bool _tableEnsured;

    public async Task AppendAsync(SyncHistoryEntry entry, CancellationToken ct)
    {
        if (!_tableEnsured)
        {
            await table.CreateIfNotExistsAsync(ct);
            _tableEnsured = true;
        }

        var rowKey = $"{InvertedTicks(entry.ProcessedAt)}-{Guid.NewGuid():N}";
        var entity = new TableEntity(SanitizeKey(entry.MapName), rowKey)
        {
            // MapName real aparte: la PartitionKey está saneada y no es reversible.
            [nameof(SyncHistoryEntry.MapName)] = entry.MapName,
            [nameof(SyncHistoryEntry.ProcessedAt)] = entry.ProcessedAt,
            [nameof(SyncHistoryEntry.Outcome)] = entry.Outcome.ToString(),
            [nameof(SyncHistoryEntry.Operation)] = entry.Operation.ToString(),
            [nameof(SyncHistoryEntry.SourceSystem)] = entry.SourceSystem,
            [nameof(SyncHistoryEntry.SourceRecordId)] = entry.SourceRecordId,
            [nameof(SyncHistoryEntry.TargetSystem)] = entry.TargetSystem,
            [nameof(SyncHistoryEntry.TargetRecordId)] = entry.TargetRecordId,
            [nameof(SyncHistoryEntry.Company)] = entry.Company,
            [nameof(SyncHistoryEntry.OccurredAt)] = entry.OccurredAt,
            [nameof(SyncHistoryEntry.Error)] = entry.Error is { Length: > MaxErrorLength } e ? e[..MaxErrorLength] + "…" : entry.Error,
            [nameof(SyncHistoryEntry.DeliveryCount)] = entry.DeliveryCount,
            [nameof(SyncHistoryEntry.CorrelationId)] = entry.CorrelationId,
        };

        await table.AddEntityAsync(entity, ct);
    }

    public async Task<IReadOnlyList<SyncHistoryEntry>> GetForMapAsync(string mapName, int take, DateTimeOffset? before, CancellationToken ct)
    {
        if (!_tableEnsured)
        {
            // Sin tabla todavía = sin historial; la pestaña del portal no debe fallar
            // en un ambiente recién aprovisionado donde el motor aún no escribió.
            await table.CreateIfNotExistsAsync(ct);
            _tableEnsured = true;
        }

        var partition = SanitizeKey(mapName);
        var filter = $"PartitionKey eq '{partition.Replace("'", "''")}'";
        if (before is { } cursor)
        {
            // Ticks invertidos: "anterior a X" = RowKey mayor. El borde exacto se
            // resuelve abajo con el filtro estricto por ProcessedAt.
            filter += $" and RowKey ge '{InvertedTicks(cursor)}'";
        }

        var entries = new List<SyncHistoryEntry>(take);
        await foreach (var entity in table.QueryAsync<TableEntity>(filter, maxPerPage: Math.Min(take, 1000), cancellationToken: ct))
        {
            var entry = ToEntry(entity);
            if (before is not null && entry.ProcessedAt >= before)
            {
                continue;
            }
            entries.Add(entry);
            if (entries.Count == take)
            {
                break;
            }
        }
        return entries;
    }

    private static SyncHistoryEntry ToEntry(TableEntity entity) => new()
    {
        MapName = entity.GetString(nameof(SyncHistoryEntry.MapName)) ?? entity.PartitionKey,
        ProcessedAt = entity.GetDateTimeOffset(nameof(SyncHistoryEntry.ProcessedAt)) ?? default,
        Outcome = Enum.TryParse<SyncOutcome>(entity.GetString(nameof(SyncHistoryEntry.Outcome)), out var outcome) ? outcome : SyncOutcome.Failed,
        Operation = Enum.TryParse<ChangeOperation>(entity.GetString(nameof(SyncHistoryEntry.Operation)), out var op) ? op : ChangeOperation.Update,
        SourceSystem = entity.GetString(nameof(SyncHistoryEntry.SourceSystem)) ?? "",
        SourceRecordId = entity.GetString(nameof(SyncHistoryEntry.SourceRecordId)) ?? "",
        TargetSystem = entity.GetString(nameof(SyncHistoryEntry.TargetSystem)) ?? "",
        TargetRecordId = entity.GetString(nameof(SyncHistoryEntry.TargetRecordId)),
        Company = entity.GetString(nameof(SyncHistoryEntry.Company)),
        OccurredAt = entity.GetDateTimeOffset(nameof(SyncHistoryEntry.OccurredAt)) ?? default,
        Error = entity.GetString(nameof(SyncHistoryEntry.Error)),
        DeliveryCount = entity.GetInt32(nameof(SyncHistoryEntry.DeliveryCount)) ?? 1,
        CorrelationId = entity.GetString(nameof(SyncHistoryEntry.CorrelationId)) ?? "",
    };

    /// <summary>D19 con cero a la izquierda: el orden lexicográfico del RowKey coincide con el numérico.</summary>
    private static string InvertedTicks(DateTimeOffset moment) =>
        (DateTimeOffset.MaxValue.UtcTicks - moment.UtcTicks).ToString("D19");

    /// <summary>Los nombres de mapa admiten cualquier texto; las claves de Tables no. No reversible: el MapName real viaja como propiedad.</summary>
    private static string SanitizeKey(string mapName)
    {
        var sanitized = mapName.ToCharArray();
        for (var i = 0; i < sanitized.Length; i++)
        {
            if (sanitized[i] is '/' or '\\' or '#' or '?' || char.IsControl(sanitized[i]))
            {
                sanitized[i] = '_';
            }
        }
        return new string(sanitized);
    }
}
