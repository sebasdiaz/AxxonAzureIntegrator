using System.Collections.Concurrent;
using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Model;

namespace Axxon.Integrator.Core.Connectors;

/// <summary>
/// Decorador de <see cref="IConnector"/> que cachea en memoria las llamadas de
/// metadata (entidades, empresas, campos, option sets). El diseñador del portal las
/// dispara en ráfaga en cada visita y en cada cambio de combo — sin caché eso golpea
/// los service protection limits de Dataverse (429). Con el decorador, cada dato se
/// lee del origen una vez y se sirve de memoria hasta vencer el TTL.
///
/// Concurrencia vía Lazy&lt;Task&gt; (dos pedidos simultáneos comparten un único
/// request); los fallos no se cachean — el próximo pedido reintenta. Las operaciones
/// de datos (pull, upsert, delete, export) pasan directo, nunca se cachean.
/// </summary>
public sealed class MetadataCachingConnector(IConnector inner, TimeSpan? ttl = null) : IConnector
{
    /// <summary>
    /// La metadata cambia a ritmo humano (alguien agrega un campo en el ambiente):
    /// 15 minutos elimina las ráfagas sin obligar a reiniciar el portal para ver un
    /// esquema nuevo.
    /// </summary>
    private static readonly TimeSpan DefaultTtl = TimeSpan.FromMinutes(15);

    private readonly AsyncCache<IReadOnlyList<string>> _entities = new(ttl ?? DefaultTtl);
    private readonly AsyncCache<IReadOnlyList<string>> _companies = new(ttl ?? DefaultTtl);
    private readonly AsyncCache<EntityMetadata> _metadata = new(ttl ?? DefaultTtl);
    private readonly AsyncCache<IReadOnlyDictionary<string, string>> _optionSets = new(ttl ?? DefaultTtl);

    public string SystemName => inner.SystemName;

    public Task<IReadOnlyList<string>> ListEntitiesAsync(CancellationToken ct) =>
        _entities.GetOrAddAsync("*", () => inner.ListEntitiesAsync(ct));

    public Task<IReadOnlyList<string>> ListCompaniesAsync(CancellationToken ct) =>
        _companies.GetOrAddAsync("*", () => inner.ListCompaniesAsync(ct));

    public Task<EntityMetadata> GetMetadataAsync(string entityName, CancellationToken ct) =>
        _metadata.GetOrAddAsync(entityName.Trim(), () => inner.GetMetadataAsync(entityName, ct));

    public Task<IReadOnlyDictionary<string, string>> GetOptionSetAsync(string entityName, string fieldName, CancellationToken ct) =>
        _optionSets.GetOrAddAsync($"{entityName.Trim()}|{fieldName.Trim()}", () => inner.GetOptionSetAsync(entityName, fieldName, ct));

    // Operaciones de datos: siempre al origen.
    public IAsyncEnumerable<ChangeEvent> PullChangesAsync(EntityQuery query, Watermark since, CancellationToken ct) =>
        inner.PullChangesAsync(query, since, ct);

    public Task<UpsertResult> UpsertAsync(EntityPayload payload, CancellationToken ct) => inner.UpsertAsync(payload, ct);

    public Task DeleteAsync(EntityPayload payload, CancellationToken ct) => inner.DeleteAsync(payload, ct);

    public IAsyncEnumerable<EntityPayload> ExportAsync(EntityQuery query, CancellationToken ct) => inner.ExportAsync(query, ct);

    /// <summary>
    /// Caché async por clave: los pedidos concurrentes de la misma clave comparten el
    /// mismo Task (un solo request al origen) y una entrada fallida se desaloja para
    /// que el siguiente pedido reintente en vez de servir la excepción cacheada.
    /// </summary>
    private sealed class AsyncCache<TValue>(TimeSpan ttl)
    {
        private readonly ConcurrentDictionary<string, Entry> _entries = new(StringComparer.OrdinalIgnoreCase);

        public async Task<TValue> GetOrAddAsync(string key, Func<Task<TValue>> factory)
        {
            while (true)
            {
                var entry = _entries.GetOrAdd(key, _ => new Entry(new Lazy<Task<TValue>>(factory), DateTimeOffset.UtcNow));
                if (DateTimeOffset.UtcNow - entry.CreatedAt > ttl)
                {
                    _entries.TryRemove(new KeyValuePair<string, Entry>(key, entry));
                    continue;
                }

                try
                {
                    return await entry.Value.Value;
                }
                catch
                {
                    _entries.TryRemove(new KeyValuePair<string, Entry>(key, entry));
                    throw;
                }
            }
        }

        private sealed record Entry(Lazy<Task<TValue>> Value, DateTimeOffset CreatedAt);
    }
}
