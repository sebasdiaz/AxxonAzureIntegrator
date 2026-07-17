using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;
using Axxon.Integrator.Core.Abstractions;
using Microsoft.Azure.Cosmos;

namespace Axxon.Integrator.Azure;

/// <summary>
/// Xref sobre Cosmos DB (base 'integrator', contenedor 'xref', PK /lookupKey), según
/// el diseño del Bicep: dos documentos espejo por vínculo — uno por lado, con
/// id = lookupKey = '&lt;pairKey&gt;|&lt;system&gt;|&lt;recordId&gt;' en minúsculas — así el GetLink
/// desde cualquier lado es un point read O(1) y la distribución entre particiones es
/// pareja incluso durante un sync inicial masivo.
///
/// Concurrencia optimista: el ETag del vínculo lleva codificado el espejo del que se
/// leyó ('lookupKey#etag'; opaco para el pipeline, que solo lo hace viajar de vuelta).
/// El Save aplica If-Match sobre ese espejo — un escritor concurrente dispara 412 y el
/// reintento del trigger relee — y replica al otro lado sin condición: el escritor es
/// único por sesión de Service Bus, la contención entre espejos es rara por diseño.
///
/// La base y el contenedor los aprovisiona el Bicep (o az CLI en desarrollo); acá no
/// se crean porque con identidad el plano de datos no tiene permiso de management.
/// </summary>
public sealed class CosmosXrefStore(Container container) : IXrefStore
{
    /// <summary>Separador lado#etag: no aparece en lookupKey (saneado) ni en un ETag de Cosmos.</summary>
    private const char ETagSeparator = '#';

    /// <summary>
    /// Opciones con las que debe construirse el <see cref="CosmosClient"/> que provee el
    /// contenedor: los documentos se serializan con System.Text.Json en camelCase (el
    /// serializador por defecto del SDK es Newtonsoft y no respetaría los atributos).
    /// </summary>
    public static CosmosClientOptions ClientOptions => new()
    {
        UseSystemTextJsonSerializerWithOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        },
    };

    public async Task<XrefLink?> GetLinkAsync(string pairKey, string system, string recordId, CancellationToken ct)
    {
        var key = LookupKeyFor(pairKey, system, recordId);
        try
        {
            var doc = (await container.ReadItemAsync<XrefDocument>(key, new PartitionKey(key), cancellationToken: ct)).Resource;
            return new XrefLink
            {
                PairKey = doc.PairKey,
                SystemA = doc.SystemA,
                RecordIdA = doc.RecordIdA,
                SystemB = doc.SystemB,
                RecordIdB = doc.RecordIdB,
                State = doc.State,
                ETag = $"{key}{ETagSeparator}{doc.ETag}",
            };
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task SaveLinkAsync(XrefLink link, CancellationToken ct)
    {
        var (readKey, etag) = ParseETag(link.ETag);

        // El espejo condicionado primero: si el If-Match falla (412), el otro lado
        // queda sin tocar y el vínculo entero sigue consistente para el reintento.
        var keys = new[]
        {
            LookupKeyFor(link.PairKey, link.SystemA, link.RecordIdA),
            LookupKeyFor(link.PairKey, link.SystemB, link.RecordIdB),
        };
        foreach (var key in keys.OrderByDescending(k => k == readKey))
        {
            var doc = new XrefDocument
            {
                Id = key,
                LookupKey = key,
                PairKey = link.PairKey,
                SystemA = link.SystemA,
                RecordIdA = link.RecordIdA,
                SystemB = link.SystemB,
                RecordIdB = link.RecordIdB,
                State = link.State,
            };
            var options = key == readKey ? new ItemRequestOptions { IfMatchEtag = etag } : null;
            await container.UpsertItemAsync(doc, new PartitionKey(key), options, ct);
        }
    }

    public async IAsyncEnumerable<XrefLink> GetLinksForPairAsync(string pairKey,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        // Query cross-partition (la PK es /lookupKey): es el barrido de la detección de
        // deletes de los mapas agendados, no un camino caliente. Los espejos comparten
        // contenido: dedupe por la identidad del lado A.
        var query = new QueryDefinition("SELECT * FROM c WHERE c.pairKey = @pairKey")
            .WithParameter("@pairKey", pairKey.ToLowerInvariant());
        using var iterator = container.GetItemQueryIterator<XrefDocument>(query);

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        while (iterator.HasMoreResults)
        {
            foreach (var doc in await iterator.ReadNextAsync(ct))
            {
                if (!seen.Add($"{doc.SystemA}|{doc.RecordIdA}"))
                {
                    continue;
                }
                yield return new XrefLink
                {
                    PairKey = doc.PairKey,
                    SystemA = doc.SystemA,
                    RecordIdA = doc.RecordIdA,
                    SystemB = doc.SystemB,
                    RecordIdB = doc.RecordIdB,
                    State = doc.State,
                    ETag = $"{doc.LookupKey}{ETagSeparator}{doc.ETag}",
                };
            }
        }
    }

    /// <summary>Misma lookup key que <c>JsonFileXrefStore</c>, saneada para id de Cosmos (prohíbe / \ # ?).</summary>
    private static string LookupKeyFor(string pairKey, string system, string recordId)
    {
        var key = $"{pairKey}|{system}|{recordId}".ToLowerInvariant().ToCharArray();
        for (var i = 0; i < key.Length; i++)
        {
            if (key[i] is '/' or '\\' or '#' or '?' || char.IsControl(key[i]))
            {
                key[i] = '-';
            }
        }
        return new string(key);
    }

    private static (string? ReadKey, string? ETag) ParseETag(string? composite)
    {
        if (string.IsNullOrEmpty(composite))
        {
            return (null, null);
        }
        var separator = composite.IndexOf(ETagSeparator);
        return separator < 0 ? (null, null) : (composite[..separator], composite[(separator + 1)..]);
    }

    /// <summary>Documento espejo tal como se persiste; <c>_etag</c> lo asigna Cosmos en cada escritura.</summary>
    private sealed record XrefDocument
    {
        public required string Id { get; init; }
        public required string LookupKey { get; init; }
        public required string PairKey { get; init; }
        public required string SystemA { get; init; }
        public required string RecordIdA { get; init; }
        public required string SystemB { get; init; }
        public required string RecordIdB { get; init; }
        public Core.Abstractions.SyncState? State { get; init; }

        [JsonPropertyName("_etag")]
        public string? ETag { get; init; }
    }
}
