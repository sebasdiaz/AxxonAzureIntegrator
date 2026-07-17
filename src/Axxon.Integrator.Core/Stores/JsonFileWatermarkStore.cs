using System.Text.Json;
using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Model;

namespace Axxon.Integrator.Core.Stores;

/// <summary>
/// Watermarks sobre archivos JSON locales, para desarrollo (en producción: Cosmos).
/// Un archivo por clave <c>system|entityName</c>; los mapas agendados usan como
/// entityName el scope <c>map:&lt;nombre&gt;</c> — cada mapa avanza su propio progreso
/// aunque compartan entidad de origen.
/// </summary>
public sealed class JsonFileWatermarkStore(string directory) : IWatermarkStore
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    public async Task<Watermark?> GetAsync(string system, string entityName, CancellationToken ct)
    {
        var file = PathFor(system, entityName);
        if (!File.Exists(file))
        {
            return null;
        }

        await using var stream = File.OpenRead(file);
        return await JsonSerializer.DeserializeAsync<Watermark>(stream, SerializerOptions, ct);
    }

    public async Task SaveAsync(Watermark watermark, CancellationToken ct)
    {
        Directory.CreateDirectory(directory);
        await using var stream = File.Create(PathFor(watermark.System, watermark.EntityName));
        await JsonSerializer.SerializeAsync(stream, watermark, SerializerOptions, ct);
    }

    private string PathFor(string system, string entityName)
    {
        var key = $"{system}|{entityName}".ToLowerInvariant();
        var invalid = Path.GetInvalidFileNameChars();
        var sanitized = new string([.. key.Select(c => invalid.Contains(c) ? '-' : c)]);
        return Path.Combine(directory, $"{sanitized}.json");
    }
}
