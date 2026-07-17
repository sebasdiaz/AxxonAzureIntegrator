using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Stores;
using Xunit;

namespace Axxon.Integrator.Tests;

/// <summary>Roundtrip del xref de archivos: guardar espejos, leer desde ambos lados, actualizar estado.</summary>
public sealed class JsonFileXrefStoreTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"axxon-xref-{Guid.NewGuid():N}");

    private static readonly XrefLink SampleLink = new()
    {
        PairKey = "dataverse:account|finops:customersv3",
        SystemA = "finops",
        RecordIdA = "9b2c1de0-1234-4c9a-8f00-aaaaaaaaaaaa",
        SystemB = "dataverse",
        RecordIdB = "1b2c3d4e-0000-4000-8000-bbbbbbbbbbbb",
    };

    [Fact]
    public async Task Link_is_readable_from_both_sides()
    {
        var store = new JsonFileXrefStore(_directory);
        await store.SaveLinkAsync(SampleLink, CancellationToken.None);

        var fromA = await store.GetLinkAsync(SampleLink.PairKey, "finops", SampleLink.RecordIdA, CancellationToken.None);
        var fromB = await store.GetLinkAsync(SampleLink.PairKey, "DATAVERSE", SampleLink.RecordIdB, CancellationToken.None);

        Assert.NotNull(fromA);
        Assert.NotNull(fromB);
        Assert.Equal(fromA.RecordIdB, fromB.RecordIdB);
    }

    [Fact]
    public async Task Save_overwrites_state_of_existing_link()
    {
        var store = new JsonFileXrefStore(_directory);
        await store.SaveLinkAsync(SampleLink, CancellationToken.None);

        var state = new SyncState
        {
            WrittenToSystem = "dataverse",
            WrittenFields = ["name"],
            WrittenPayloadHash = "ABC123",
            LastWriterSystem = "finops",
            LastWriterOccurredAt = DateTimeOffset.UnixEpoch,
        };
        await store.SaveLinkAsync(SampleLink with { State = state }, CancellationToken.None);

        var loaded = await store.GetLinkAsync(SampleLink.PairKey, "finops", SampleLink.RecordIdA, CancellationToken.None);

        Assert.NotNull(loaded);
        Assert.NotNull(loaded.State);
        Assert.Equal("ABC123", loaded.State.WrittenPayloadHash);
        Assert.Equal(DateTimeOffset.UnixEpoch, loaded.State.LastWriterOccurredAt);
    }

    [Fact]
    public async Task Links_for_pair_dedupe_mirrors_and_filter_other_pairs()
    {
        var store = new JsonFileXrefStore(_directory);
        await store.SaveLinkAsync(SampleLink, CancellationToken.None);
        await store.SaveLinkAsync(SampleLink with { RecordIdA = "otro-registro", RecordIdB = "otro-destino" }, CancellationToken.None);
        await store.SaveLinkAsync(SampleLink with { PairKey = "dataverse:contact|finops:vendorsv2" }, CancellationToken.None);

        var links = new List<XrefLink>();
        await foreach (var link in store.GetLinksForPairAsync(SampleLink.PairKey, CancellationToken.None))
        {
            links.Add(link);
        }

        // dos vínculos del par (cada uno persistido como dos espejos), el par ajeno afuera
        Assert.Equal(2, links.Count);
        Assert.All(links, l => Assert.Equal(SampleLink.PairKey, l.PairKey));
    }

    [Fact]
    public async Task Unknown_record_returns_null()
    {
        var store = new JsonFileXrefStore(_directory);

        Assert.Null(await store.GetLinkAsync("par|inexistente", "finops", "nada", CancellationToken.None));
    }

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }
}
