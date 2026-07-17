using System.Text.Json;
using System.Text.Json.Serialization;
using Axxon.Integrator.Core.Abstractions;

namespace Axxon.Integrator.Core.Stores;

/// <summary>
/// Xref sobre archivos JSON locales, para desarrollo (en producción: Cosmos, fase 1).
/// Reproduce el diseño de documentos espejo: el mismo vínculo se persiste una vez por
/// lado, con nombre de archivo derivado de la lookup key <c>pairKey|system|recordId</c> —
/// el GetLink desde cualquier lado es una lectura directa, como el point read de Cosmos.
/// Sin concurrencia por ETag: en desarrollo alcanza con que las sesiones hagan único al
/// escritor por lado.
/// </summary>
public sealed class JsonFileXrefStore(string directory) : IXrefStore
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) },
    };

    public async Task<XrefLink?> GetLinkAsync(string pairKey, string system, string recordId, CancellationToken ct)
    {
        var file = PathFor(pairKey, system, recordId);
        if (!File.Exists(file))
        {
            return null;
        }

        await using var stream = File.OpenRead(file);
        return await JsonSerializer.DeserializeAsync<XrefLink>(stream, SerializerOptions, ct);
    }

    public async Task SaveLinkAsync(XrefLink link, CancellationToken ct)
    {
        Directory.CreateDirectory(directory);
        string[] mirrors =
        [
            PathFor(link.PairKey, link.SystemA, link.RecordIdA),
            PathFor(link.PairKey, link.SystemB, link.RecordIdB),
        ];
        foreach (var file in mirrors)
        {
            await using var stream = File.Create(file);
            await JsonSerializer.SerializeAsync(stream, link, SerializerOptions, ct);
        }
    }

    public async IAsyncEnumerable<XrefLink> GetLinksForPairAsync(string pairKey,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        if (!Directory.Exists(directory))
        {
            yield break;
        }

        // Barrido del directorio filtrando por el PairKey deserializado (el nombre de
        // archivo está saneado y no sirve como filtro exacto). Los espejos comparten
        // contenido: dedupe por la identidad del lado A.
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var file in Directory.EnumerateFiles(directory, "*.json"))
        {
            ct.ThrowIfCancellationRequested();
            XrefLink? link;
            await using (var stream = File.OpenRead(file))
            {
                link = await JsonSerializer.DeserializeAsync<XrefLink>(stream, SerializerOptions, ct);
            }

            if (link is null ||
                !string.Equals(link.PairKey, pairKey, StringComparison.OrdinalIgnoreCase) ||
                !seen.Add($"{link.SystemA}|{link.RecordIdA}"))
            {
                continue;
            }
            yield return link;
        }
    }

    /// <summary>Lookup key como nombre de archivo, con los caracteres inválidos (los ':' y '|' del PairKey) aplanados a '-'.</summary>
    private string PathFor(string pairKey, string system, string recordId)
    {
        var lookupKey = $"{pairKey}|{system}|{recordId}".ToLowerInvariant();
        var invalid = Path.GetInvalidFileNameChars();
        var sanitized = new string([.. lookupKey.Select(c => invalid.Contains(c) ? '-' : c)]);
        return Path.Combine(directory, $"{sanitized}.json");
    }
}
