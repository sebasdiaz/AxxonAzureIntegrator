using System.Text.Json;
using System.Text.Json.Serialization;
using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Model;

namespace Axxon.Integrator.Core.Stores;

/// <summary>
/// Store de mapas sobre archivos JSON locales (un archivo por mapa, nombre = nombre
/// del mapa). Es el store de desarrollo y la referencia del formato del documento:
/// el store de blobs de producción persiste exactamente el mismo JSON con el mismo
/// nombre. Enums como texto y camelCase para que el documento sea legible/editable
/// a mano.
/// </summary>
public sealed class JsonFileEntityMapStore(string directory) : IEntityMapStore
{
    public static readonly JsonSerializerOptions SerializerOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) },
    };

    public async Task<IReadOnlyList<EntityMap>> GetAllAsync(CancellationToken ct)
    {
        if (!Directory.Exists(directory))
        {
            return [];
        }

        var maps = new List<EntityMap>();
        foreach (var file in Directory.EnumerateFiles(directory, "*.json"))
        {
            await using var stream = File.OpenRead(file);
            var map = await JsonSerializer.DeserializeAsync<EntityMap>(stream, SerializerOptions, ct)
                ?? throw new FormatException($"El archivo de mapa '{file}' no contiene un EntityMap válido.");
            maps.Add(map);
        }
        return maps;
    }

    public async Task<IReadOnlyList<EntityMap>> GetMapsForSourceAsync(string sourceSystem, string sourceEntity, CancellationToken ct) =>
        [.. (await GetAllAsync(ct)).Where(m =>
            m.Status == MapStatus.Active &&
            string.Equals(m.SourceSystem, sourceSystem, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(m.SourceEntity, sourceEntity, StringComparison.OrdinalIgnoreCase))];

    public async Task<EntityMap?> GetAsync(string name, CancellationToken ct)
    {
        var file = PathFor(name);
        if (!File.Exists(file))
        {
            return null;
        }

        await using var stream = File.OpenRead(file);
        return await JsonSerializer.DeserializeAsync<EntityMap>(stream, SerializerOptions, ct);
    }

    public async Task SaveAsync(EntityMap map, CancellationToken ct)
    {
        Directory.CreateDirectory(directory);
        await using var stream = File.Create(PathFor(map.Name));
        await JsonSerializer.SerializeAsync(stream, map, SerializerOptions, ct);
    }

    private string PathFor(string mapName)
    {
        if (mapName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
        {
            throw new ArgumentException($"Nombre de mapa inválido como archivo: '{mapName}'.", nameof(mapName));
        }
        return Path.Combine(directory, $"{mapName}.json");
    }
}
