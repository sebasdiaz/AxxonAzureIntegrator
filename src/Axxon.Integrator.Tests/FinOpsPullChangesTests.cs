using System.Net;
using Axxon.Integrator.Azure;
using Axxon.Integrator.Connectors.FinOps;
using Axxon.Integrator.Core.Model;
using Xunit;

namespace Axxon.Integrator.Tests;

/// <summary>
/// PullChangesAsync de F&O: filtro incremental por ModifiedDateTime, cross-company,
/// paginación por nextLink, id canónico (clave compuesta ordenada por nombre de campo)
/// y $select de claves para el barrido de la detección de deletes.
/// </summary>
public sealed class FinOpsPullChangesTests
{
    private const string Metadata = """
        {"value":[{"EntitySetName":"CustomersV3","Properties":[
            {"Name":"dataAreaId","TypeName":"Edm.String","IsKey":true},
            {"Name":"CustomerAccount","TypeName":"Edm.String","IsKey":true},
            {"Name":"OrganizationName","TypeName":"Edm.String"},
            {"Name":"ModifiedDateTime","TypeName":"Edm.DateTimeOffset"}
        ]}]}
        """;

    private const string MetadataWithoutModified = """
        {"value":[{"EntitySetName":"CustomersV3","Properties":[
            {"Name":"dataAreaId","TypeName":"Edm.String","IsKey":true},
            {"Name":"CustomerAccount","TypeName":"Edm.String","IsKey":true},
            {"Name":"OrganizationName","TypeName":"Edm.String"}
        ]}]}
        """;

    private const string SinglePage = """
        {"value":[{"dataAreaId":"usmf","CustomerAccount":"C001","OrganizationName":"Contoso","ModifiedDateTime":"2026-07-15T10:00:00Z"}]}
        """;

    [Fact]
    public async Task Incremental_filters_by_watermark_and_builds_canonical_record_id()
    {
        var stub = new SequencedStub((HttpStatusCode.OK, Metadata), (HttpStatusCode.OK, SinglePage));
        var connector = ConnectorWith(stub);
        var query = new EntityQuery { EntityName = "CustomersV3", Companies = ["usmf"] };
        var since = new Watermark("finops", "map:clientes", "2026-07-15T00:00:00.0000000+00:00", DateTimeOffset.UtcNow);

        var events = await ToListAsync(connector.PullChangesAsync(query, since, CancellationToken.None));

        var url = Uri.UnescapeDataString(stub.Requests[1].RequestUri!.ToString());
        Assert.Contains("data/CustomersV3", url);
        Assert.Contains("cross-company=true", url);
        Assert.Contains("ModifiedDateTime ge 2026-07-15T00:00:00.0000000Z", url);
        Assert.Contains("dataAreaId eq 'usmf'", url);

        var evt = Assert.Single(events);
        Assert.Equal("CustomerAccount='C001',dataAreaId='usmf'", evt.SourceRecordId); // misma identidad que el upsert
        Assert.Equal(ChangeOperation.Update, evt.Operation);
        Assert.Equal(new DateTimeOffset(2026, 7, 15, 10, 0, 0, TimeSpan.Zero), evt.OccurredAt);
        Assert.Equal("usmf", evt.Company);
        Assert.Equal("Contoso", evt.Data["OrganizationName"]);
        Assert.Equal("finops", evt.SourceSystem);
    }

    [Fact]
    public async Task Empty_watermark_is_a_full_scan_without_filter()
    {
        var stub = new SequencedStub((HttpStatusCode.OK, Metadata), (HttpStatusCode.OK, SinglePage));
        var connector = ConnectorWith(stub);
        var query = new EntityQuery { EntityName = "CustomersV3" };

        await ToListAsync(connector.PullChangesAsync(query, Watermark.Start("finops", "map:clientes"), CancellationToken.None));

        var url = Uri.UnescapeDataString(stub.Requests[1].RequestUri!.ToString());
        Assert.DoesNotContain("$filter", url);
        Assert.Contains("cross-company=true", url);
    }

    [Fact]
    public async Task Follows_server_driven_pagination()
    {
        const string pageOne = """
            {"value":[{"dataAreaId":"usmf","CustomerAccount":"C001","ModifiedDateTime":"2026-07-15T10:00:00Z"}],
             "@odata.nextLink":"https://unit.test/data/CustomersV3?cross-company=true&$skiptoken=abc"}
            """;
        const string pageTwo = """
            {"value":[{"dataAreaId":"usmf","CustomerAccount":"C002","ModifiedDateTime":"2026-07-15T11:00:00Z"}]}
            """;
        var stub = new SequencedStub((HttpStatusCode.OK, Metadata), (HttpStatusCode.OK, pageOne), (HttpStatusCode.OK, pageTwo));
        var connector = ConnectorWith(stub);

        var events = await ToListAsync(connector.PullChangesAsync(
            new EntityQuery { EntityName = "CustomersV3" }, Watermark.Start("finops", "map:clientes"), CancellationToken.None));

        Assert.Equal(2, events.Count);
        Assert.Contains("$skiptoken=abc", stub.Requests[2].RequestUri!.ToString());
    }

    [Fact]
    public async Task Keys_only_selects_just_the_key_fields()
    {
        const string keysPage = """{"value":[{"dataAreaId":"usmf","CustomerAccount":"C001"}]}""";
        var stub = new SequencedStub((HttpStatusCode.OK, Metadata), (HttpStatusCode.OK, keysPage));
        var connector = ConnectorWith(stub);

        var events = await ToListAsync(connector.PullChangesAsync(
            new EntityQuery { EntityName = "CustomersV3", KeysOnly = true }, Watermark.Start("finops", "map:clientes"), CancellationToken.None));

        var url = Uri.UnescapeDataString(stub.Requests[1].RequestUri!.ToString());
        Assert.Contains("$select=dataAreaId,CustomerAccount", url);
        Assert.Equal("CustomerAccount='C001',dataAreaId='usmf'", Assert.Single(events).SourceRecordId);
    }

    [Fact]
    public async Task Incremental_without_modified_field_is_a_permanent_error()
    {
        var stub = new SequencedStub((HttpStatusCode.OK, MetadataWithoutModified));
        var connector = ConnectorWith(stub);
        var since = new Watermark("finops", "map:clientes", "2026-07-15T00:00:00.0000000+00:00", DateTimeOffset.UtcNow);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            ToListAsync(connector.PullChangesAsync(new EntityQuery { EntityName = "CustomersV3" }, since, CancellationToken.None)));

        Assert.Contains("FullExport", ex.Message);
    }

    private static async Task<List<ChangeEvent>> ToListAsync(IAsyncEnumerable<ChangeEvent> source)
    {
        var list = new List<ChangeEvent>();
        await foreach (var item in source)
        {
            list.Add(item);
        }
        return list;
    }

    private static FinOpsConnector ConnectorWith(SequencedStub stub) => new(
        new HttpClient(stub) { BaseAddress = new Uri("https://unit.test/") },
        new EntraAppOptions { EnvironmentUrl = "https://unit.test" });

    private sealed class SequencedStub(params (HttpStatusCode Status, string Body)[] responses) : HttpMessageHandler
    {
        private readonly Queue<(HttpStatusCode Status, string Body)> _responses = new(responses);

        public List<HttpRequestMessage> Requests { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Requests.Add(request);
            var (status, body) = _responses.Count > 1 ? _responses.Dequeue() : _responses.Peek();
            return Task.FromResult(new HttpResponseMessage(status)
            {
                RequestMessage = request,
                Content = new StringContent(body, System.Text.Encoding.UTF8, "application/json"),
            });
        }
    }
}
