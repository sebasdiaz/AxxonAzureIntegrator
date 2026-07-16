using System.Net;
using Axxon.Integrator.Azure;
using Axxon.Integrator.Core.Abstractions;
using Microsoft.Azure.Cosmos;
using Xunit;

namespace Axxon.Integrator.Tests;

/// <summary>
/// Integración del xref sobre Cosmos: espejos, point reads desde ambos lados y
/// concurrencia optimista por ETag. Corre solo con AXXON_COSMOS_ENDPOINT y
/// AXXON_COSMOS_KEY en el ambiente (apuntando a una cuenta de desarrollo con la
/// base 'integrator' y el contenedor 'xref' ya creados); sin ellas cada test
/// retorna sin ejercitar nada, para que la suite no dependa de infraestructura.
/// </summary>
public sealed class CosmosXrefStoreTests
{
    private static CosmosXrefStore? CreateStoreOrSkip()
    {
        var endpoint = Environment.GetEnvironmentVariable("AXXON_COSMOS_ENDPOINT");
        var key = Environment.GetEnvironmentVariable("AXXON_COSMOS_KEY");
        if (string.IsNullOrWhiteSpace(endpoint) || string.IsNullOrWhiteSpace(key))
        {
            return null;
        }

        var client = new CosmosClient(endpoint, key, CosmosXrefStore.ClientOptions);
        return new CosmosXrefStore(client.GetContainer("integrator", "xref"));
    }

    /// <summary>PairKey único por corrida: el contenedor de desarrollo es compartido.</summary>
    private static XrefLink NewSampleLink() => new()
    {
        PairKey = $"dataverse:test-{Guid.NewGuid():N}|finops:customersv3",
        SystemA = "finops",
        RecordIdA = "9b2c1de0-1234-4c9a-8f00-aaaaaaaaaaaa",
        SystemB = "dataverse",
        RecordIdB = "1b2c3d4e-0000-4000-8000-bbbbbbbbbbbb",
    };

    [Fact]
    public async Task Link_is_readable_from_both_sides()
    {
        if (CreateStoreOrSkip() is not { } store)
        {
            return;
        }

        var link = NewSampleLink();
        await store.SaveLinkAsync(link, CancellationToken.None);

        var fromA = await store.GetLinkAsync(link.PairKey, "finops", link.RecordIdA, CancellationToken.None);
        var fromB = await store.GetLinkAsync(link.PairKey, "DATAVERSE", link.RecordIdB, CancellationToken.None);

        Assert.NotNull(fromA);
        Assert.NotNull(fromB);
        Assert.Equal(fromA.RecordIdB, fromB.RecordIdB);
        Assert.NotNull(fromA.ETag);
    }

    [Fact]
    public async Task Save_overwrites_state_of_existing_link()
    {
        if (CreateStoreOrSkip() is not { } store)
        {
            return;
        }

        var link = NewSampleLink();
        await store.SaveLinkAsync(link, CancellationToken.None);

        var state = new SyncState
        {
            WrittenToSystem = "dataverse",
            WrittenFields = ["name"],
            WrittenPayloadHash = "ABC123",
            LastWriterSystem = "finops",
            LastWriterOccurredAt = DateTimeOffset.UnixEpoch,
        };
        var loadedFirst = await store.GetLinkAsync(link.PairKey, "finops", link.RecordIdA, CancellationToken.None);
        Assert.NotNull(loadedFirst);
        await store.SaveLinkAsync(loadedFirst with { State = state }, CancellationToken.None);

        var loaded = await store.GetLinkAsync(link.PairKey, "dataverse", link.RecordIdB, CancellationToken.None);

        Assert.NotNull(loaded);
        Assert.NotNull(loaded.State);
        Assert.Equal("ABC123", loaded.State.WrittenPayloadHash);
        Assert.Equal(DateTimeOffset.UnixEpoch, loaded.State.LastWriterOccurredAt);
    }

    [Fact]
    public async Task Unknown_record_returns_null()
    {
        if (CreateStoreOrSkip() is not { } store)
        {
            return;
        }

        Assert.Null(await store.GetLinkAsync($"par|inexistente-{Guid.NewGuid():N}", "finops", "nada", CancellationToken.None));
    }

    [Fact]
    public async Task Stale_etag_is_rejected_with_precondition_failed()
    {
        if (CreateStoreOrSkip() is not { } store)
        {
            return;
        }

        var link = NewSampleLink();
        await store.SaveLinkAsync(link, CancellationToken.None);

        // Dos lectores del mismo espejo; el primero escribe y deja viejo el ETag del segundo.
        var winner = await store.GetLinkAsync(link.PairKey, "finops", link.RecordIdA, CancellationToken.None);
        var loser = await store.GetLinkAsync(link.PairKey, "finops", link.RecordIdA, CancellationToken.None);
        Assert.NotNull(winner);
        Assert.NotNull(loser);

        await store.SaveLinkAsync(winner, CancellationToken.None);

        var ex = await Assert.ThrowsAsync<CosmosException>(
            () => store.SaveLinkAsync(loser, CancellationToken.None));
        Assert.Equal(HttpStatusCode.PreconditionFailed, ex.StatusCode);
    }
}
