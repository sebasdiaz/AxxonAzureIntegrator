using Axxon.Integrator.Core.Model;
using Axxon.Integrator.Core.Stores;
using Xunit;

namespace Axxon.Integrator.Tests;

/// <summary>Roundtrip del store de archivos: guardar, leer, listar, eliminar.</summary>
public sealed class EntityMapStoreTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"axxon-maps-{Guid.NewGuid():N}");

    [Fact]
    public async Task Save_get_delete_roundtrip()
    {
        var store = new JsonFileEntityMapStore(_directory);
        var map = SampleMap("customers-test");

        await store.SaveAsync(map, CancellationToken.None);

        var loaded = await store.GetAsync("customers-test", CancellationToken.None);
        Assert.NotNull(loaded);
        Assert.Equal(map.Name, loaded.Name);
        Assert.Equal(map.IntegrationKey, loaded.IntegrationKey);
        Assert.Equal("Retail", loaded.Fields[1].ValueMap!["1"]);

        Assert.Single(await store.GetAllAsync(CancellationToken.None));

        await store.DeleteAsync("customers-test", CancellationToken.None);

        Assert.Null(await store.GetAsync("customers-test", CancellationToken.None));
        Assert.Empty(await store.GetAllAsync(CancellationToken.None));
    }

    [Fact]
    public async Task Delete_of_missing_map_is_idempotent()
    {
        var store = new JsonFileEntityMapStore(_directory);

        await store.DeleteAsync("nunca-existio", CancellationToken.None); // no lanza
    }

    [Fact]
    public async Task Paused_maps_are_excluded_from_source_lookup_but_listed()
    {
        var store = new JsonFileEntityMapStore(_directory);
        await store.SaveAsync(SampleMap("pausado") with { Status = MapStatus.Paused }, CancellationToken.None);

        Assert.Empty(await store.GetMapsForSourceAsync("dataverse", "account", CancellationToken.None));
        Assert.Single(await store.GetAllAsync(CancellationToken.None));
    }

    private static EntityMap SampleMap(string name) => new()
    {
        Name = name,
        SourceSystem = "dataverse",
        SourceEntity = "account",
        TargetSystem = "finops",
        TargetEntity = "CustomersV3",
        Fields =
        [
            new FieldMap { Source = "accountnumber", Target = "CustomerAccount" },
            new FieldMap
            {
                Source = "industrycode",
                Target = "SegmentId",
                ValueMap = new Dictionary<string, string> { ["1"] = "Retail", ["2"] = "Wholesale" },
                DefaultValue = "Retail",
            },
        ],
        IntegrationKey = ["CustomerAccount"],
    };

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }
}
