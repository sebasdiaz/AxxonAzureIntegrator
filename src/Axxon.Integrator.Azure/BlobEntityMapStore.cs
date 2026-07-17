using Azure;
using Azure.Storage.Blobs;
using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Model;
using Axxon.Integrator.Core.Stores;

namespace Axxon.Integrator.Azure;

/// <summary>
/// Store de mapas en producción: un blob JSON por mapa en el container 'entity-maps'
/// (decisión 13 revisada: blob en lugar de Cosmos — los mapas son archivos planos y
/// el versioning nativo del blob service conserva la historia de cada guardado, que
/// el file store no tiene). Mismo documento y mismo nombre de archivo que
/// <see cref="JsonFileEntityMapStore"/>: export/import entre dev y producción sigue
/// siendo copiar el archivo.
///
/// Volumen esperado: decenas de mapas de 1-2 KB — listar el container es trivial,
/// pero NO por evento: el motor debe consumir esto detrás de un caché en memoria con
/// refresh (TODO fase 1, junto con el cableado del SyncPipeline).
/// </summary>
public sealed class BlobEntityMapStore(BlobContainerClient container) : IEntityMapStore
{
    public async Task<IReadOnlyList<EntityMap>> GetAllAsync(CancellationToken ct)
    {
        var maps = new List<EntityMap>();
        await foreach (var blob in container.GetBlobsAsync(cancellationToken: ct))
        {
            if (!blob.Name.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var content = await container.GetBlobClient(blob.Name).DownloadContentAsync(ct);
            maps.Add(content.Value.Content.ToObjectFromJson<EntityMap>(JsonFileEntityMapStore.SerializerOptions)
                ?? throw new FormatException($"El blob '{blob.Name}' no contiene un EntityMap válido."));
        }
        return maps;
    }

    public async Task<IReadOnlyList<EntityMap>> GetMapsForSourceAsync(string sourceSystem, string sourceEntity, CancellationToken ct) =>
        [.. (await GetAllAsync(ct)).Where(m => m.MatchesSource(sourceSystem, sourceEntity))];

    public async Task<EntityMap?> GetAsync(string name, CancellationToken ct)
    {
        try
        {
            var content = await container.GetBlobClient(BlobNameFor(name)).DownloadContentAsync(ct);
            return content.Value.Content.ToObjectFromJson<EntityMap>(JsonFileEntityMapStore.SerializerOptions);
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }

    public async Task SaveAsync(EntityMap map, CancellationToken ct) =>
        // Overwrite directo: el versioning del blob service conserva cada versión
        // anterior (consultable y restaurable desde el portal de Azure). Concurrencia
        // condicional por ETag: cuando el portal sea multiusuario (fase 4).
        await container.GetBlobClient(BlobNameFor(map.Name)).UploadAsync(
            BinaryData.FromObjectAsJson(map, JsonFileEntityMapStore.SerializerOptions),
            overwrite: true,
            ct);

    public Task DeleteAsync(string name, CancellationToken ct) =>
        // El versioning del blob service conserva las versiones previas incluso tras
        // el delete: un mapa eliminado por error se restaura desde el portal de Azure.
        container.GetBlobClient(BlobNameFor(name)).DeleteIfExistsAsync(cancellationToken: ct);

    /// <summary>Misma validación de nombre que el file store, para que un mapa sea portable entre ambos.</summary>
    private static string BlobNameFor(string mapName)
    {
        if (mapName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
        {
            throw new ArgumentException($"Nombre de mapa inválido como archivo: '{mapName}'.", nameof(mapName));
        }
        return $"{mapName}.json";
    }
}
