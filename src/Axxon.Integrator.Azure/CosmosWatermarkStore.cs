using System.Net;
using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Model;
using Microsoft.Azure.Cosmos;

namespace Axxon.Integrator.Azure;

/// <summary>
/// Watermarks sobre Cosmos DB (misma base 'integrator' que el xref, contenedor
/// 'watermarks', PK /id): un documento por clave <c>system|entityName</c>, point read
/// y upsert sin condición — el escritor es único por diseño (las sesiones de la cola
/// scheduled-runs serializan los runs de un mapa).
/// El <see cref="CosmosClient"/> debe construirse con <see cref="CosmosXrefStore.ClientOptions"/>.
/// </summary>
public sealed class CosmosWatermarkStore(Container container) : IWatermarkStore
{
    public async Task<Watermark?> GetAsync(string system, string entityName, CancellationToken ct)
    {
        var id = IdFor(system, entityName);
        try
        {
            var doc = (await container.ReadItemAsync<WatermarkDocument>(id, new PartitionKey(id), cancellationToken: ct)).Resource;
            return new Watermark(doc.System, doc.EntityName, doc.Token, doc.UpdatedAt);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task SaveAsync(Watermark watermark, CancellationToken ct)
    {
        var id = IdFor(watermark.System, watermark.EntityName);
        var doc = new WatermarkDocument
        {
            Id = id,
            System = watermark.System,
            EntityName = watermark.EntityName,
            Token = watermark.Token,
            UpdatedAt = watermark.UpdatedAt,
        };
        await container.UpsertItemAsync(doc, new PartitionKey(id), cancellationToken: ct);
    }

    /// <summary>Mismo saneo que la lookup key del xref (id de Cosmos prohíbe / \ # ?).</summary>
    private static string IdFor(string system, string entityName)
    {
        var key = $"{system}|{entityName}".ToLowerInvariant().ToCharArray();
        for (var i = 0; i < key.Length; i++)
        {
            if (key[i] is '/' or '\\' or '#' or '?' || char.IsControl(key[i]))
            {
                key[i] = '-';
            }
        }
        return new string(key);
    }

    private sealed record WatermarkDocument
    {
        public required string Id { get; init; }
        public required string System { get; init; }
        public required string EntityName { get; init; }
        public required string Token { get; init; }
        public required DateTimeOffset UpdatedAt { get; init; }
    }
}
