using System.Net;
using Axxon.Integrator.Azure;
using Axxon.Integrator.Connectors.FinOps;
using Axxon.Integrator.Core.Model;
using Xunit;

namespace Axxon.Integrator.Tests;

/// <summary>
/// UpsertAsync/DeleteAsync de F&O: clave OData compuesta desde la integration key,
/// PATCH update-only con fallback a POST, cuerpo sin claves en PATCH.
/// </summary>
public sealed class FinOpsUpsertTests
{
    private const string ExpectedKey = "dataAreaId='usmf',CustomerAccount='C001'";

    [Fact]
    public async Task Patches_existing_record_by_composite_key()
    {
        var stub = new SequencedStub((HttpStatusCode.NoContent, ""));
        var connector = ConnectorWith(stub);

        var result = await connector.UpsertAsync(Payload(), CancellationToken.None);

        Assert.False(result.Created);
        Assert.Equal(ExpectedKey, result.TargetRecordId);
        var patch = Assert.Single(stub.Requests);
        Assert.Equal(HttpMethod.Patch, patch.Method);
        var url = Uri.UnescapeDataString(patch.RequestUri!.ToString());
        Assert.Contains($"data/CustomersV3({ExpectedKey})", url);
        Assert.Contains("cross-company=true", url);
        Assert.Single(patch.Headers.IfMatch); // update-only: el 404 decide el create
        Assert.DoesNotContain("CustomerAccount", stub.Bodies[0]); // las claves van en la URL, no en el cuerpo
        Assert.Contains("OrganizationName", stub.Bodies[0]);
    }

    [Fact]
    public async Task Patch_404_falls_back_to_create_with_company()
    {
        var stub = new SequencedStub(
            (HttpStatusCode.NotFound, """{"error":{"message":"Not Found"}}"""),
            (HttpStatusCode.Created, "{}"));
        var connector = ConnectorWith(stub);

        var result = await connector.UpsertAsync(Payload(), CancellationToken.None);

        Assert.True(result.Created);
        Assert.Equal(ExpectedKey, result.TargetRecordId);
        var post = stub.Requests[1];
        Assert.Equal(HttpMethod.Post, post.Method);
        Assert.EndsWith("data/CustomersV3", post.RequestUri!.ToString());
        // el create lleva la clave completa, incluida la empresa del evento
        Assert.Contains("dataAreaId", stub.Bodies[1]);
        Assert.Contains("usmf", stub.Bodies[1]);
        Assert.Contains("CustomerAccount", stub.Bodies[1]);
    }

    [Fact]
    public async Task Missing_key_value_is_a_permanent_error()
    {
        var connector = ConnectorWith(new SequencedStub((HttpStatusCode.NoContent, "")));
        var payload = Payload() with
        {
            Fields = new Dictionary<string, object?> { ["OrganizationName"] = "Contoso" },
            Company = null, // sin dataAreaId ni empresa: la clave no se puede armar
        };

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => connector.UpsertAsync(payload, CancellationToken.None));

        Assert.Contains("dataAreaId", ex.Message);
    }

    [Fact]
    public async Task Quotes_in_key_values_are_escaped()
    {
        var stub = new SequencedStub((HttpStatusCode.NoContent, ""));
        var connector = ConnectorWith(stub);
        var payload = Payload() with
        {
            Fields = new Dictionary<string, object?>
            {
                ["CustomerAccount"] = "C'001",
                ["OrganizationName"] = "Contoso",
            },
        };

        await connector.UpsertAsync(payload, CancellationToken.None);

        Assert.Contains("CustomerAccount='C''001'", Uri.UnescapeDataString(stub.Requests[0].RequestUri!.ToString()));
    }

    [Fact]
    public async Task Delete_of_missing_record_is_idempotent()
    {
        var stub = new SequencedStub((HttpStatusCode.NotFound, """{"error":{"message":"Not Found"}}"""));
        var connector = ConnectorWith(stub);

        await connector.DeleteAsync(Payload() with { Operation = ChangeOperation.Delete }, CancellationToken.None);

        Assert.Equal(HttpMethod.Delete, Assert.Single(stub.Requests).Method);
    }

    private static EntityPayload Payload() => new()
    {
        TargetSystem = "finops",
        EntityName = "CustomersV3",
        TargetRecordId = null,
        Operation = ChangeOperation.Update,
        Fields = new Dictionary<string, object?>
        {
            ["CustomerAccount"] = "C001",
            ["OrganizationName"] = "Contoso",
        },
        IntegrationKey = ["dataAreaId", "CustomerAccount"],
        Company = "usmf",
        IdempotencyKey = "test:1",
        CorrelationId = "corr-1",
    };

    private static FinOpsConnector ConnectorWith(SequencedStub stub) => new(
        new HttpClient(stub) { BaseAddress = new Uri("https://unit.test/") },
        new EntraAppOptions { EnvironmentUrl = "https://unit.test" });

    private sealed class SequencedStub(params (HttpStatusCode Status, string Body)[] responses) : HttpMessageHandler
    {
        private readonly Queue<(HttpStatusCode Status, string Body)> _responses = new(responses);

        public List<HttpRequestMessage> Requests { get; } = [];

        /// <summary>Cuerpos capturados al enviar: el conector dispone el request después, así que leerlos más tarde falla.</summary>
        public List<string> Bodies { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Requests.Add(request);
            Bodies.Add(request.Content is null ? "" : await request.Content.ReadAsStringAsync(ct));
            var (status, body) = _responses.Count > 1 ? _responses.Dequeue() : _responses.Peek();
            return new HttpResponseMessage(status)
            {
                RequestMessage = request,
                Content = new StringContent(body, System.Text.Encoding.UTF8, "application/json"),
            };
        }
    }
}
