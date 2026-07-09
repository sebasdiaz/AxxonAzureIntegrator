using System.Net;
using Axxon.Integrator.Azure;
using Axxon.Integrator.Connectors.Dataverse;
using Axxon.Integrator.Core.Model;
using Xunit;

namespace Axxon.Integrator.Tests;

/// <summary>
/// UpsertAsync/DeleteAsync de Dataverse contra un stub con respuestas en secuencia:
/// resolución de entity set, resolución por integration key, PATCH-upsert y POST.
/// </summary>
public sealed class DataverseUpsertTests
{
    private const string EntityRefJson = """
    { "EntitySetName": "accounts", "PrimaryIdAttribute": "accountid", "MetadataId": "70816501-edb9-4740-a16c-6a5efbc05d84" }
    """;

    private static readonly string KnownId = "11111111-2222-3333-4444-555555555555";

    [Fact]
    public async Task Upsert_with_known_id_patches_by_primary_key()
    {
        var stub = new SequencedStub(
            (HttpStatusCode.OK, EntityRefJson, null),
            (HttpStatusCode.NoContent, "", null));
        var connector = ConnectorWith(stub);

        var result = await connector.UpsertAsync(Payload(targetRecordId: KnownId), CancellationToken.None);

        Assert.False(result.Created);
        Assert.Equal(KnownId, result.TargetRecordId);
        var patch = stub.Requests[1];
        Assert.Equal(HttpMethod.Patch, patch.Method);
        Assert.Equal($"https://unit.test/api/data/v9.2/accounts({KnownId})", patch.RequestUri!.ToString());
        // Sin If-Match: PATCH-upsert nativo, tolera xref apuntando a un registro borrado
        Assert.Empty(patch.Headers.IfMatch);
    }

    [Fact]
    public async Task Upsert_without_link_resolves_by_integration_key()
    {
        var stub = new SequencedStub(
            (HttpStatusCode.OK, EntityRefJson, null),
            (HttpStatusCode.OK, $$"""{ "value": [ { "accountid": "{{KnownId}}" } ] }""", null),
            (HttpStatusCode.NoContent, "", null));
        var connector = ConnectorWith(stub);

        var result = await connector.UpsertAsync(Payload(targetRecordId: null), CancellationToken.None);

        Assert.False(result.Created);
        Assert.Equal(KnownId, result.TargetRecordId);
        var query = Uri.UnescapeDataString(stub.Requests[1].RequestUri!.ToString());
        Assert.Contains("$filter=accountnumber eq 'C001'", query);
        Assert.Contains("$select=accountid", query);
        Assert.Equal(HttpMethod.Patch, stub.Requests[2].Method);
    }

    [Fact]
    public async Task Upsert_without_match_creates_and_returns_new_id()
    {
        var newId = "99999999-8888-7777-6666-555555555555";
        var stub = new SequencedStub(
            (HttpStatusCode.OK, EntityRefJson, null),
            (HttpStatusCode.OK, """{ "value": [] }""", null),
            (HttpStatusCode.NoContent, "", $"https://unit.test/api/data/v9.2/accounts({newId})"));
        var connector = ConnectorWith(stub);

        var result = await connector.UpsertAsync(Payload(targetRecordId: null), CancellationToken.None);

        Assert.True(result.Created);
        Assert.Equal(newId, result.TargetRecordId);
        Assert.Equal(HttpMethod.Post, stub.Requests[2].Method);
    }

    [Fact]
    public async Task Missing_integration_key_is_a_permanent_error()
    {
        var stub = new SequencedStub((HttpStatusCode.OK, EntityRefJson, null));
        var connector = ConnectorWith(stub);
        var payload = Payload(targetRecordId: null) with { IntegrationKey = [] };

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => connector.UpsertAsync(payload, CancellationToken.None));

        Assert.Contains("integration key", ex.Message);
    }

    [Fact]
    public async Task Entity_ref_is_cached_across_calls()
    {
        var stub = new SequencedStub(
            (HttpStatusCode.OK, EntityRefJson, null),
            (HttpStatusCode.NoContent, "", null),
            (HttpStatusCode.NoContent, "", null));
        var connector = ConnectorWith(stub);

        await connector.UpsertAsync(Payload(targetRecordId: KnownId), CancellationToken.None);
        await connector.UpsertAsync(Payload(targetRecordId: KnownId), CancellationToken.None);

        // metadata 1 vez + 2 PATCH
        Assert.Equal(3, stub.Requests.Count);
    }

    [Fact]
    public async Task Delete_of_missing_record_is_idempotent()
    {
        var stub = new SequencedStub(
            (HttpStatusCode.OK, EntityRefJson, null),
            (HttpStatusCode.NotFound, """{"error":{"message":"Not Found"}}""", null));
        var connector = ConnectorWith(stub);

        await connector.DeleteAsync(Payload(targetRecordId: KnownId) with { Operation = ChangeOperation.Delete }, CancellationToken.None);
    }

    private static EntityPayload Payload(string? targetRecordId) => new()
    {
        TargetSystem = "dataverse",
        EntityName = "account",
        TargetRecordId = targetRecordId,
        Operation = ChangeOperation.Update,
        Fields = new Dictionary<string, object?>
        {
            ["accountnumber"] = "C001",
            ["name"] = "Contoso",
        },
        IntegrationKey = ["accountnumber"],
        IdempotencyKey = "test:1",
        CorrelationId = "corr-1",
    };

    private static DataverseConnector ConnectorWith(SequencedStub stub) => new(
        new HttpClient(stub) { BaseAddress = new Uri("https://unit.test/") },
        new EntraAppOptions { EnvironmentUrl = "https://unit.test" });

    private sealed class SequencedStub(params (HttpStatusCode Status, string Body, string? EntityIdHeader)[] responses) : HttpMessageHandler
    {
        private readonly Queue<(HttpStatusCode Status, string Body, string? EntityIdHeader)> _responses = new(responses);

        public List<HttpRequestMessage> Requests { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Requests.Add(request);
            var (status, body, entityId) = _responses.Count > 1 ? _responses.Dequeue() : _responses.Peek();
            var response = new HttpResponseMessage(status)
            {
                RequestMessage = request,
                Content = new StringContent(body, System.Text.Encoding.UTF8, "application/json"),
            };
            if (entityId is not null)
            {
                response.Headers.Add("OData-EntityId", entityId);
            }
            return Task.FromResult(response);
        }
    }
}
