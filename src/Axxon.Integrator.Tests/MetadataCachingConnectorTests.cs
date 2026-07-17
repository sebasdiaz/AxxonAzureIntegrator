using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Connectors;
using Axxon.Integrator.Core.Model;
using Xunit;

namespace Axxon.Integrator.Tests;

/// <summary>
/// El decorador de caché de metadata: una sola lectura al origen por dato (aun con
/// pedidos concurrentes), claves separadas por entidad/campo, fallos sin cachear y
/// TTL vencido que relee.
/// </summary>
public sealed class MetadataCachingConnectorTests
{
    [Fact]
    public async Task Repeated_calls_hit_the_source_once()
    {
        var inner = new CountingConnector();
        var cached = new MetadataCachingConnector(inner);

        await cached.ListEntitiesAsync(CancellationToken.None);
        await cached.ListEntitiesAsync(CancellationToken.None);
        await cached.ListCompaniesAsync(CancellationToken.None);
        await cached.ListCompaniesAsync(CancellationToken.None);
        await cached.GetMetadataAsync("account", CancellationToken.None);
        await cached.GetMetadataAsync("account", CancellationToken.None);
        await cached.GetOptionSetAsync("account", "statecode", CancellationToken.None);
        await cached.GetOptionSetAsync("account", "statecode", CancellationToken.None);

        Assert.Equal(1, inner.EntityListCalls);
        Assert.Equal(1, inner.CompanyListCalls);
        Assert.Equal(1, inner.MetadataCalls);
        Assert.Equal(1, inner.OptionSetCalls);
    }

    [Fact]
    public async Task Concurrent_requests_share_a_single_source_call()
    {
        var inner = new CountingConnector { Delay = TimeSpan.FromMilliseconds(50) };
        var cached = new MetadataCachingConnector(inner);

        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => cached.ListCompaniesAsync(CancellationToken.None)));

        Assert.Equal(1, inner.CompanyListCalls);
    }

    [Fact]
    public async Task Distinct_entities_and_fields_are_cached_separately()
    {
        var inner = new CountingConnector();
        var cached = new MetadataCachingConnector(inner);

        await cached.GetMetadataAsync("account", CancellationToken.None);
        await cached.GetMetadataAsync("contact", CancellationToken.None);
        await cached.GetOptionSetAsync("account", "statecode", CancellationToken.None);
        await cached.GetOptionSetAsync("account", "industrycode", CancellationToken.None);

        Assert.Equal(2, inner.MetadataCalls);
        Assert.Equal(2, inner.OptionSetCalls);
    }

    [Fact]
    public async Task Failures_are_not_cached_and_the_next_call_retries()
    {
        var inner = new CountingConnector { FailFirstCompanyList = true };
        var cached = new MetadataCachingConnector(inner);

        await Assert.ThrowsAsync<HttpRequestException>(() => cached.ListCompaniesAsync(CancellationToken.None));
        var companies = await cached.ListCompaniesAsync(CancellationToken.None);

        Assert.Equal(["usmf"], companies);
        Assert.Equal(2, inner.CompanyListCalls);
    }

    [Fact]
    public async Task Expired_ttl_reloads_from_the_source()
    {
        var inner = new CountingConnector();
        var cached = new MetadataCachingConnector(inner, ttl: TimeSpan.Zero);

        await cached.ListEntitiesAsync(CancellationToken.None);
        await Task.Delay(20);
        await cached.ListEntitiesAsync(CancellationToken.None);

        Assert.Equal(2, inner.EntityListCalls);
    }

    private sealed class CountingConnector : IConnector
    {
        public TimeSpan Delay { get; init; }
        public bool FailFirstCompanyList { get; init; }

        public int EntityListCalls { get; private set; }
        public int CompanyListCalls { get; private set; }
        public int MetadataCalls { get; private set; }
        public int OptionSetCalls { get; private set; }

        public string SystemName => "dataverse";

        public async Task<IReadOnlyList<string>> ListEntitiesAsync(CancellationToken ct)
        {
            EntityListCalls++;
            await Task.Delay(Delay, ct);
            return ["account", "contact"];
        }

        public async Task<IReadOnlyList<string>> ListCompaniesAsync(CancellationToken ct)
        {
            CompanyListCalls++;
            await Task.Delay(Delay, ct);
            if (FailFirstCompanyList && CompanyListCalls == 1)
            {
                throw new HttpRequestException("429 Too Many Requests");
            }
            return ["usmf"];
        }

        public async Task<EntityMetadata> GetMetadataAsync(string entityName, CancellationToken ct)
        {
            MetadataCalls++;
            await Task.Delay(Delay, ct);
            return new EntityMetadata
            {
                EntityName = entityName,
                KeyFields = ["id"],
                Fields = new Dictionary<string, string> { ["name"] = "String" },
            };
        }

        public async Task<IReadOnlyDictionary<string, string>> GetOptionSetAsync(string entityName, string fieldName, CancellationToken ct)
        {
            OptionSetCalls++;
            await Task.Delay(Delay, ct);
            return new Dictionary<string, string> { ["1"] = "Activo" };
        }

        public IAsyncEnumerable<ChangeEvent> PullChangesAsync(EntityQuery query, Watermark since, CancellationToken ct) => throw new NotImplementedException();
        public Task<UpsertResult> UpsertAsync(EntityPayload payload, CancellationToken ct) => throw new NotImplementedException();
        public Task DeleteAsync(EntityPayload payload, CancellationToken ct) => throw new NotImplementedException();
        public IAsyncEnumerable<EntityPayload> ExportAsync(EntityQuery query, CancellationToken ct) => throw new NotImplementedException();
    }
}
