using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Model;
using Axxon.Integrator.Core.Sync;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Axxon.Integrator.Tests;

/// <summary>
/// El pipeline deja un renglón de histórico por desenlace: Created/Updated al
/// sincronizar, Failed (con el error y sin tumbar el reintento) cuando el destino
/// falla, y los descartes (last-writer-wins) también quedan registrados.
/// </summary>
public sealed class SyncPipelineHistoryTests
{
    private static readonly EntityMap Map = new()
    {
        Name = "clientes",
        SourceSystem = "finops",
        SourceEntity = "CustomersV3",
        TargetSystem = "dataverse",
        TargetEntity = "account",
        Fields = [new FieldMap { Source = "Name", Target = "name" }],
        IntegrationKey = ["name"],
    };

    private static ChangeEvent Event(DateTimeOffset? occurredAt = null) => new()
    {
        SourceSystem = "finops",
        EntityName = "CustomersV3",
        SourceRecordId = "REC-1",
        Operation = ChangeOperation.Update,
        OccurredAt = occurredAt ?? DateTimeOffset.UtcNow,
        Data = new Dictionary<string, object?> { ["Name"] = "Contoso" },
        CorrelationId = "test-corr-1",
    };

    private static SyncPipeline Pipeline(FakeConnector connector, InMemoryHistory history, IXrefStore? xref = null) => new(
        new SingleMapStore(Map),
        xref ?? new NullXref(),
        new EchoGuard(new HashSet<string>()),
        new MappingEngine(),
        new Dictionary<string, IConnector> { ["dataverse"] = connector },
        history,
        NullLogger<SyncPipeline>.Instance);

    [Fact]
    public async Task Successful_upsert_records_created_with_target_id()
    {
        var history = new InMemoryHistory();
        await Pipeline(new FakeConnector(), history).ProcessAsync(Event(), deliveryCount: 1, CancellationToken.None);

        var entry = Assert.Single(history.Entries);
        Assert.Equal(SyncOutcome.Created, entry.Outcome);
        Assert.Equal("clientes", entry.MapName);
        Assert.Equal("TARGET-1", entry.TargetRecordId);
        Assert.Null(entry.Error);
        Assert.Equal("test-corr-1", entry.CorrelationId);
    }

    [Fact]
    public async Task Failed_upsert_records_error_and_rethrows()
    {
        var history = new InMemoryHistory();
        var pipeline = Pipeline(new FakeConnector { Fail = new InvalidOperationException("400 del destino") }, history);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            pipeline.ProcessAsync(Event(), deliveryCount: 3, CancellationToken.None));

        var entry = Assert.Single(history.Entries);
        Assert.Equal(SyncOutcome.Failed, entry.Outcome);
        Assert.Equal("400 del destino", entry.Error);
        Assert.Equal(3, entry.DeliveryCount);
        Assert.Null(entry.TargetRecordId);
    }

    [Fact]
    public async Task Stale_event_records_discarded_by_last_writer_wins()
    {
        var history = new InMemoryHistory();
        var connector = new FakeConnector();
        var xref = new NullXref
        {
            Link = new XrefLink
            {
                PairKey = Map.PairKey,
                SystemA = "finops",
                RecordIdA = "REC-1",
                SystemB = "dataverse",
                RecordIdB = "TARGET-1",
                State = new SyncState
                {
                    WrittenToSystem = "dataverse",
                    WrittenFields = ["name"],
                    WrittenPayloadHash = "HASH",
                    LastWriterSystem = "dataverse",
                    LastWriterOccurredAt = DateTimeOffset.UtcNow,
                },
            },
        };

        await Pipeline(connector, history, xref)
            .ProcessAsync(Event(occurredAt: DateTimeOffset.UtcNow.AddMinutes(-10)), deliveryCount: 1, CancellationToken.None);

        var entry = Assert.Single(history.Entries);
        Assert.Equal(SyncOutcome.DiscardedByLastWriterWins, entry.Outcome);
        Assert.Equal("TARGET-1", entry.TargetRecordId);
        Assert.Equal(0, connector.UpsertCalls); // descartado: no se escribió nada
    }

    private sealed class InMemoryHistory : ISyncHistoryStore
    {
        public List<SyncHistoryEntry> Entries { get; } = [];

        public Task AppendAsync(SyncHistoryEntry entry, CancellationToken ct)
        {
            Entries.Add(entry);
            return Task.CompletedTask;
        }

        public Task<IReadOnlyList<SyncHistoryEntry>> GetForMapAsync(string mapName, int take, DateTimeOffset? before, CancellationToken ct) =>
            Task.FromResult<IReadOnlyList<SyncHistoryEntry>>([.. Entries.Where(e => e.MapName == mapName)]);
    }

    private sealed class SingleMapStore(EntityMap map) : IEntityMapStore
    {
        public Task<IReadOnlyList<EntityMap>> GetMapsForSourceAsync(string sourceSystem, string sourceEntity, CancellationToken ct) =>
            Task.FromResult<IReadOnlyList<EntityMap>>([map]);
        public Task<EntityMap?> GetAsync(string name, CancellationToken ct) => Task.FromResult<EntityMap?>(map);
        public Task<IReadOnlyList<EntityMap>> GetAllAsync(CancellationToken ct) => Task.FromResult<IReadOnlyList<EntityMap>>([map]);
        public Task SaveAsync(EntityMap m, CancellationToken ct) => Task.CompletedTask;
        public Task DeleteAsync(string name, CancellationToken ct) => Task.CompletedTask;
    }

    private sealed class NullXref : IXrefStore
    {
        public XrefLink? Link { get; init; }
        public Task<XrefLink?> GetLinkAsync(string pairKey, string system, string recordId, CancellationToken ct) =>
            Task.FromResult(Link);
        public Task SaveLinkAsync(XrefLink link, CancellationToken ct) => Task.CompletedTask;
    }

    private sealed class FakeConnector : IConnector
    {
        public Exception? Fail { get; init; }
        public int UpsertCalls { get; private set; }

        public string SystemName => "dataverse";

        public Task<UpsertResult> UpsertAsync(EntityPayload payload, CancellationToken ct)
        {
            UpsertCalls++;
            return Fail is null
                ? Task.FromResult(new UpsertResult { TargetRecordId = "TARGET-1", Created = true })
                : Task.FromException<UpsertResult>(Fail);
        }

        public Task DeleteAsync(EntityPayload payload, CancellationToken ct) => Task.CompletedTask;
        public IAsyncEnumerable<ChangeEvent> PullChangesAsync(Watermark since, CancellationToken ct) => throw new NotImplementedException();
        public IAsyncEnumerable<EntityPayload> ExportAsync(EntityQuery query, CancellationToken ct) => throw new NotImplementedException();
        public Task<EntityMetadata> GetMetadataAsync(string entityName, CancellationToken ct) => throw new NotImplementedException();
        public Task<IReadOnlyList<string>> ListEntitiesAsync(CancellationToken ct) => throw new NotImplementedException();
        public Task<IReadOnlyList<string>> ListCompaniesAsync(CancellationToken ct) => throw new NotImplementedException();
        public Task<IReadOnlyDictionary<string, string>> GetOptionSetAsync(string entityName, string fieldName, CancellationToken ct) => throw new NotImplementedException();
    }
}
