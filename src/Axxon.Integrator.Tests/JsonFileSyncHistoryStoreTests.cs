using Axxon.Integrator.Core.Model;
using Axxon.Integrator.Core.Stores;
using Xunit;

namespace Axxon.Integrator.Tests;

/// <summary>
/// Histórico de archivos JSONL: append + lectura paginada del más nuevo al más
/// viejo, con aislamiento por mapa. Es el contrato que el store de Table Storage
/// replica en producción.
/// </summary>
public sealed class JsonFileSyncHistoryStoreTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"axxon-history-{Guid.NewGuid():N}");

    private static SyncHistoryEntry Entry(string map, DateTimeOffset processedAt, SyncOutcome outcome = SyncOutcome.Created, string? error = null) => new()
    {
        MapName = map,
        ProcessedAt = processedAt,
        Outcome = outcome,
        Operation = ChangeOperation.Update,
        SourceSystem = "finops",
        SourceRecordId = "REC-1",
        TargetSystem = "dataverse",
        TargetRecordId = outcome == SyncOutcome.Failed ? null : "1b2c3d4e-0000-4000-8000-bbbbbbbbbbbb",
        Company = "ALAS",
        OccurredAt = processedAt.AddSeconds(-5),
        Error = error,
        DeliveryCount = 1,
        CorrelationId = Guid.NewGuid().ToString("N"),
    };

    [Fact]
    public async Task Entries_come_back_newest_first_with_cursor_pagination()
    {
        var store = new JsonFileSyncHistoryStore(_directory);
        var t0 = DateTimeOffset.UtcNow;
        for (var i = 0; i < 5; i++)
        {
            await store.AppendAsync(Entry("mapa", t0.AddMinutes(i)), CancellationToken.None);
        }

        var firstPage = await store.GetForMapAsync("mapa", take: 2, before: null, CancellationToken.None);
        Assert.Equal(2, firstPage.Count);
        Assert.Equal(t0.AddMinutes(4), firstPage[0].ProcessedAt);
        Assert.Equal(t0.AddMinutes(3), firstPage[1].ProcessedAt);

        var secondPage = await store.GetForMapAsync("mapa", take: 2, before: firstPage[^1].ProcessedAt, CancellationToken.None);
        Assert.Equal(2, secondPage.Count);
        Assert.Equal(t0.AddMinutes(2), secondPage[0].ProcessedAt);
        Assert.Equal(t0.AddMinutes(1), secondPage[1].ProcessedAt);
    }

    [Fact]
    public async Task History_is_isolated_per_map()
    {
        var store = new JsonFileSyncHistoryStore(_directory);
        await store.AppendAsync(Entry("mapa-a", DateTimeOffset.UtcNow), CancellationToken.None);
        await store.AppendAsync(Entry("mapa-b", DateTimeOffset.UtcNow), CancellationToken.None);

        var forA = await store.GetForMapAsync("mapa-a", take: 10, before: null, CancellationToken.None);

        Assert.Single(forA);
        Assert.Equal("mapa-a", forA[0].MapName);
    }

    [Fact]
    public async Task Failed_entry_roundtrips_error_and_outcome()
    {
        var store = new JsonFileSyncHistoryStore(_directory);
        await store.AppendAsync(
            Entry("mapa", DateTimeOffset.UtcNow, SyncOutcome.Failed, error: "400 Bad Request: \"detalle\", con comas"),
            CancellationToken.None);

        var loaded = await store.GetForMapAsync("mapa", take: 1, before: null, CancellationToken.None);

        Assert.Single(loaded);
        Assert.Equal(SyncOutcome.Failed, loaded[0].Outcome);
        Assert.Equal("400 Bad Request: \"detalle\", con comas", loaded[0].Error);
        Assert.Null(loaded[0].TargetRecordId);
    }

    [Fact]
    public async Task Unknown_map_returns_empty()
    {
        var store = new JsonFileSyncHistoryStore(_directory);
        Assert.Empty(await store.GetForMapAsync("inexistente", take: 10, before: null, CancellationToken.None));
    }

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }
}
