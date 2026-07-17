using Axxon.Integrator.Core.Model;
using Axxon.Integrator.Core.Stores;
using Xunit;

namespace Axxon.Integrator.Tests;

/// <summary>Roundtrip del store de watermarks de archivos (desarrollo local).</summary>
public sealed class JsonFileWatermarkStoreTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"axxon-watermarks-{Guid.NewGuid():N}");

    [Fact]
    public async Task Roundtrips_watermark_by_map_scope()
    {
        var store = new JsonFileWatermarkStore(_directory);
        var watermark = new Watermark("finops", "map:clientes", "2026-07-15T00:00:00.0000000+00:00", DateTimeOffset.UnixEpoch);

        await store.SaveAsync(watermark, CancellationToken.None);
        var loaded = await store.GetAsync("finops", "map:clientes", CancellationToken.None);

        Assert.NotNull(loaded);
        Assert.Equal(watermark.Token, loaded.Token);
        Assert.Equal(watermark.UpdatedAt, loaded.UpdatedAt);
    }

    [Fact]
    public async Task Save_overwrites_previous_token()
    {
        var store = new JsonFileWatermarkStore(_directory);
        await store.SaveAsync(new Watermark("finops", "map:clientes", "viejo", DateTimeOffset.UnixEpoch), CancellationToken.None);
        await store.SaveAsync(new Watermark("finops", "map:clientes", "nuevo", DateTimeOffset.UnixEpoch), CancellationToken.None);

        Assert.Equal("nuevo", (await store.GetAsync("finops", "map:clientes", CancellationToken.None))!.Token);
    }

    [Fact]
    public async Task Missing_watermark_returns_null()
    {
        var store = new JsonFileWatermarkStore(_directory);

        Assert.Null(await store.GetAsync("finops", "map:inexistente", CancellationToken.None));
    }

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }
}
