using System.Text.Json;
using System.Text.Json.Serialization;
using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Model;

namespace Axxon.Integrator.Core.Stores;

/// <summary>
/// Histórico de sincronización sobre archivos locales: un archivo JSONL por mapa
/// (una línea = un intento), append-only. Es el store de desarrollo; producción usa
/// Table Storage con el mismo registro. JSONL y no un array JSON para que el append
/// sea O(1) y el archivo se pueda seguir con tail.
/// </summary>
public sealed class JsonFileSyncHistoryStore(string directory) : ISyncHistoryStore
{
    public static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) },
    };

    // Serializa los appends del proceso; entre procesos el riesgo es solo de
    // desarrollo local (motor y portal comparten carpeta pero solo el motor escribe).
    private readonly SemaphoreSlim _writeLock = new(1, 1);

    public async Task AppendAsync(SyncHistoryEntry entry, CancellationToken ct)
    {
        Directory.CreateDirectory(directory);
        var line = JsonSerializer.Serialize(entry, SerializerOptions) + Environment.NewLine;

        await _writeLock.WaitAsync(ct);
        try
        {
            await File.AppendAllTextAsync(PathFor(entry.MapName), line, ct);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async Task<IReadOnlyList<SyncHistoryEntry>> GetForMapAsync(string mapName, int take, DateTimeOffset? before, CancellationToken ct)
    {
        var file = PathFor(mapName);
        if (!File.Exists(file))
        {
            return [];
        }

        var entries = new List<SyncHistoryEntry>();
        foreach (var line in await File.ReadAllLinesAsync(file, ct))
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }
            entries.Add(JsonSerializer.Deserialize<SyncHistoryEntry>(line, SerializerOptions)
                ?? throw new FormatException($"Línea de histórico ilegible en '{file}'."));
        }

        return [.. entries
            .Where(e => before is null || e.ProcessedAt < before)
            .OrderByDescending(e => e.ProcessedAt)
            .Take(take)];
    }

    private string PathFor(string mapName)
    {
        if (mapName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
        {
            throw new ArgumentException($"Nombre de mapa inválido como archivo: '{mapName}'.", nameof(mapName));
        }
        return Path.Combine(directory, $"{mapName}.jsonl");
    }
}
