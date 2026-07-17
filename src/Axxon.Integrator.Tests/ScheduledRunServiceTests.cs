using System.Runtime.CompilerServices;
using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Model;
using Axxon.Integrator.Core.Sync;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Axxon.Integrator.Tests;

/// <summary>
/// El run agendado publica lo pulleado al flujo normalizado, avanza la watermark por
/// mapa recién al final, y detecta deletes por ausencia contra el xref (salteando
/// tombstones para no re-emitir borrados ya propagados).
/// </summary>
public sealed class ScheduledRunServiceTests
{
    private static readonly DateTimeOffset ScheduledFor = new(2026, 7, 16, 12, 0, 0, TimeSpan.Zero);

    private static EntityMap Map(ScheduledRunMode mode, bool detectDeletes = false, IReadOnlyList<string>? companies = null) => new()
    {
        Name = "clientes",
        SourceSystem = "finops",
        SourceEntity = "CustomersV3",
        TargetSystem = "dataverse",
        TargetEntity = "account",
        Fields = [new FieldMap { Source = "Name", Target = "name" }],
        IntegrationKey = ["name"],
        Companies = companies ?? [],
        Schedule = new MapSchedule { Cron = "0 * * * * *", Mode = mode, DetectDeletes = detectDeletes },
    };

    private static ChangeEvent Change(string recordId, DateTimeOffset occurredAt) => new()
    {
        SourceSystem = "finops",
        EntityName = "CustomersV3",
        SourceRecordId = recordId,
        Operation = ChangeOperation.Update,
        OccurredAt = occurredAt,
        Data = new Dictionary<string, object?> { ["Name"] = "Contoso" },
    };

    private static XrefLink Link(EntityMap map, string finopsId, SyncState? state = null) => new()
    {
        PairKey = map.PairKey,
        SystemA = "finops",
        RecordIdA = finopsId,
        SystemB = "dataverse",
        RecordIdB = $"dv-{finopsId}",
        State = state,
    };

    private static SyncState Tombstone => new()
    {
        WrittenToSystem = "dataverse",
        WrittenFields = [],
        WrittenPayloadHash = "",
        LastWriterSystem = "finops",
        LastWriterOccurredAt = ScheduledFor.AddDays(-1),
    };

    private static SyncState SyncedState => new()
    {
        WrittenToSystem = "dataverse",
        WrittenFields = ["name"],
        WrittenPayloadHash = "HASH",
        LastWriterSystem = "finops",
        LastWriterOccurredAt = ScheduledFor.AddDays(-1),
    };

    private static ScheduledRunService Service(FakePullConnector connector, InMemoryWatermarks watermarks,
        ListXref xref, InMemoryPublisher publisher) => new(
        new Dictionary<string, IConnector> { ["finops"] = connector },
        watermarks,
        xref,
        publisher,
        NullLogger<ScheduledRunService>.Instance);

    [Fact]
    public async Task Incremental_publishes_changes_and_advances_watermark_to_max_occurred()
    {
        var older = ScheduledFor.AddHours(-2);
        var newer = ScheduledFor.AddHours(-1);
        var connector = new FakePullConnector { Changes = [Change("A", newer), Change("B", older)] };
        var watermarks = new InMemoryWatermarks();
        var publisher = new InMemoryPublisher();

        await Service(connector, watermarks, new ListXref(), publisher)
            .RunAsync(Map(ScheduledRunMode.Incremental), ScheduledFor, CancellationToken.None);

        Assert.Equal(2, publisher.Published.Count);
        Assert.Equal("", Assert.Single(connector.PullCalls).Since.Token); // primer run: desde el principio
        var watermark = await watermarks.GetAsync("finops", "map:clientes", CancellationToken.None);
        Assert.NotNull(watermark);
        Assert.Equal(newer.ToString("O"), watermark.Token);
    }

    [Fact]
    public async Task Incremental_resumes_from_stored_watermark_and_keeps_it_when_no_changes()
    {
        var connector = new FakePullConnector();
        var watermarks = new InMemoryWatermarks();
        var stored = new Watermark("finops", "map:clientes", "2026-07-15T00:00:00.0000000+00:00", ScheduledFor.AddDays(-1));
        await watermarks.SaveAsync(stored, CancellationToken.None);

        await Service(connector, watermarks, new ListXref(), new InMemoryPublisher())
            .RunAsync(Map(ScheduledRunMode.Incremental), ScheduledFor, CancellationToken.None);

        Assert.Equal(stored.Token, Assert.Single(connector.PullCalls).Since.Token);
        // sin cambios la watermark no se toca: avanzarla al reloj del run perdería eventos rezagados
        Assert.Equal(stored.Token, (await watermarks.GetAsync("finops", "map:clientes", CancellationToken.None))!.Token);
    }

    [Fact]
    public async Task Full_export_always_pulls_from_start_and_never_saves_watermark()
    {
        var connector = new FakePullConnector { Changes = [Change("A", ScheduledFor.AddHours(-1))] };
        var watermarks = new InMemoryWatermarks();
        await watermarks.SaveAsync(new Watermark("finops", "map:clientes", "2026-07-15T00:00:00.0000000+00:00", ScheduledFor), CancellationToken.None);
        var publisher = new InMemoryPublisher();

        await Service(connector, watermarks, new ListXref(), publisher)
            .RunAsync(Map(ScheduledRunMode.FullExport), ScheduledFor, CancellationToken.None);

        Assert.Equal("", Assert.Single(connector.PullCalls).Since.Token);
        Assert.Single(publisher.Published);
    }

    [Fact]
    public async Task Detect_deletes_emits_delete_for_absent_links_skipping_live_and_tombstoned()
    {
        var map = Map(ScheduledRunMode.FullExport, detectDeletes: true);
        var connector = new FakePullConnector { Changes = [Change("live-1", ScheduledFor.AddHours(-1))] };
        var xref = new ListXref
        {
            Links =
            {
                Link(map, "live-1", SyncedState),   // sigue en el origen: no se borra
                Link(map, "gone-1", SyncedState),   // ausente: delete
                Link(map, "tomb-1", Tombstone),     // delete ya propagado: no re-emitir
            },
        };
        var publisher = new InMemoryPublisher();

        await Service(connector, new InMemoryWatermarks(), xref, publisher)
            .RunAsync(map, ScheduledFor, CancellationToken.None);

        // full export sin filtro de empresas: las claves vivas salen del propio pull, sin barrido extra
        Assert.Single(connector.PullCalls);

        var delete = Assert.Single(publisher.Published, e => e.Operation == ChangeOperation.Delete);
        Assert.Equal("gone-1", delete.SourceRecordId);
        Assert.Equal("CustomersV3", delete.EntityName);
        Assert.Equal(ScheduledFor, delete.OccurredAt);
        Assert.Null(delete.Company);
    }

    [Fact]
    public async Task Incremental_detect_deletes_scans_keys_without_company_filter()
    {
        var map = Map(ScheduledRunMode.Incremental, detectDeletes: true, companies: ["usmf"]);
        var connector = new FakePullConnector
        {
            Changes = [Change("A", ScheduledFor.AddHours(-1))],
            KeyScan = [Change("A", ScheduledFor)],
        };
        var xref = new ListXref { Links = { Link(map, "A", SyncedState), Link(map, "B", SyncedState) } };
        var publisher = new InMemoryPublisher();

        await Service(connector, new InMemoryWatermarks(), xref, publisher)
            .RunAsync(map, ScheduledFor, CancellationToken.None);

        Assert.Equal(2, connector.PullCalls.Count);
        Assert.Equal(["usmf"], connector.PullCalls[0].Query.Companies);
        Assert.False(connector.PullCalls[0].Query.KeysOnly);
        // el barrido de claves va sin filtro de empresa: la ausencia debe ser absoluta
        Assert.True(connector.PullCalls[1].Query.KeysOnly);
        Assert.Empty(connector.PullCalls[1].Query.Companies);

        var delete = Assert.Single(publisher.Published, e => e.Operation == ChangeOperation.Delete);
        Assert.Equal("B", delete.SourceRecordId);
    }

    [Fact]
    public async Task Full_export_with_company_filter_needs_separate_unscoped_key_scan()
    {
        var map = Map(ScheduledRunMode.FullExport, detectDeletes: true, companies: ["usmf"]);
        var connector = new FakePullConnector
        {
            Changes = [Change("A", ScheduledFor.AddHours(-1))], // export filtrado por empresa
            KeyScan = [Change("A", ScheduledFor), Change("other-company", ScheduledFor)],
        };
        var xref = new ListXref { Links = { Link(map, "other-company", SyncedState) } };
        var publisher = new InMemoryPublisher();

        await Service(connector, new InMemoryWatermarks(), xref, publisher)
            .RunAsync(map, ScheduledFor, CancellationToken.None);

        // el registro de otra empresa está vivo en el origen: el export filtrado no
        // alcanza para declararlo ausente, el barrido sin filtro lo salva
        Assert.Equal(2, connector.PullCalls.Count);
        Assert.DoesNotContain(publisher.Published, e => e.Operation == ChangeOperation.Delete);
    }

    private sealed class FakePullConnector : IConnector
    {
        public List<ChangeEvent> Changes { get; init; } = [];

        /// <summary>Respuesta a los pulls con KeysOnly (el barrido de la detección de deletes).</summary>
        public List<ChangeEvent> KeyScan { get; init; } = [];

        public List<(EntityQuery Query, Watermark Since)> PullCalls { get; } = [];

        public string SystemName => "finops";

        public async IAsyncEnumerable<ChangeEvent> PullChangesAsync(EntityQuery query, Watermark since,
            [EnumeratorCancellation] CancellationToken ct)
        {
            PullCalls.Add((query, since));
            foreach (var evt in query.KeysOnly ? KeyScan : Changes)
            {
                yield return evt;
            }
            await Task.CompletedTask;
        }

        public Task<UpsertResult> UpsertAsync(EntityPayload payload, CancellationToken ct) => throw new NotImplementedException();
        public Task DeleteAsync(EntityPayload payload, CancellationToken ct) => throw new NotImplementedException();
        public IAsyncEnumerable<EntityPayload> ExportAsync(EntityQuery query, CancellationToken ct) => throw new NotImplementedException();
        public Task<EntityMetadata> GetMetadataAsync(string entityName, CancellationToken ct) => throw new NotImplementedException();
        public Task<IReadOnlyList<string>> ListEntitiesAsync(CancellationToken ct) => throw new NotImplementedException();
        public Task<IReadOnlyList<string>> ListCompaniesAsync(CancellationToken ct) => throw new NotImplementedException();
        public Task<IReadOnlyDictionary<string, string>> GetOptionSetAsync(string entityName, string fieldName, CancellationToken ct) => throw new NotImplementedException();
    }

    private sealed class InMemoryPublisher : IChangeEventPublisher
    {
        public List<ChangeEvent> Published { get; } = [];

        public Task PublishAsync(ChangeEvent evt, CancellationToken ct)
        {
            Published.Add(evt);
            return Task.CompletedTask;
        }
    }

    private sealed class InMemoryWatermarks : IWatermarkStore
    {
        private readonly Dictionary<string, Watermark> _byKey = new(StringComparer.OrdinalIgnoreCase);

        public Task<Watermark?> GetAsync(string system, string entityName, CancellationToken ct) =>
            Task.FromResult(_byKey.TryGetValue($"{system}|{entityName}", out var watermark) ? watermark : null);

        public Task SaveAsync(Watermark watermark, CancellationToken ct)
        {
            _byKey[$"{watermark.System}|{watermark.EntityName}"] = watermark;
            return Task.CompletedTask;
        }
    }

    private sealed class ListXref : IXrefStore
    {
        public List<XrefLink> Links { get; } = [];

        public async IAsyncEnumerable<XrefLink> GetLinksForPairAsync(string pairKey,
            [EnumeratorCancellation] CancellationToken ct)
        {
            foreach (var link in Links.Where(l => string.Equals(l.PairKey, pairKey, StringComparison.OrdinalIgnoreCase)))
            {
                yield return link;
            }
            await Task.CompletedTask;
        }

        public Task<XrefLink?> GetLinkAsync(string pairKey, string system, string recordId, CancellationToken ct) => throw new NotImplementedException();
        public Task SaveLinkAsync(XrefLink link, CancellationToken ct) => throw new NotImplementedException();
    }
}
