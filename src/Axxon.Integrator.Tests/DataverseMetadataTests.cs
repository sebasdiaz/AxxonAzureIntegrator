using System.Net;
using Axxon.Integrator.Azure;
using Axxon.Integrator.Connectors.Dataverse;
using Xunit;

namespace Axxon.Integrator.Tests;

/// <summary>
/// GetMetadataAsync contra un handler stub con la forma real de la respuesta de
/// EntityDefinitions (Web API v9.2). El conector acepta cualquier HttpClient, así que
/// el mismo código que corre autenticado en producción se testea acá sin red.
/// </summary>
public sealed class DataverseMetadataTests
{
    // Forma real (recortada) de EntityDefinitions(LogicalName='account')
    // ?$select=LogicalName,PrimaryIdAttribute&$expand=Attributes($select=LogicalName,AttributeType,AttributeOf)
    private const string AccountMetadataJson = """
    {
      "@odata.context": "https://unit.test/api/data/v9.2/$metadata#EntityDefinitions(LogicalName,PrimaryIdAttribute,Attributes(LogicalName,AttributeType,AttributeOf))/$entity",
      "LogicalName": "account",
      "PrimaryIdAttribute": "accountid",
      "MetadataId": "70816501-edb9-4740-a16c-6a5efbc05d84",
      "Attributes": [
        { "LogicalName": "accountid", "AttributeType": "Uniqueidentifier", "AttributeOf": null, "MetadataId": "00000000-0000-0000-0000-000000000001" },
        { "LogicalName": "accountnumber", "AttributeType": "String", "AttributeOf": null, "MetadataId": "00000000-0000-0000-0000-000000000002" },
        { "LogicalName": "name", "AttributeType": "String", "AttributeOf": null, "MetadataId": "00000000-0000-0000-0000-000000000003" },
        { "LogicalName": "industrycode", "AttributeType": "Picklist", "AttributeOf": null, "MetadataId": "00000000-0000-0000-0000-000000000004" },
        { "LogicalName": "primarycontactid", "AttributeType": "Lookup", "AttributeOf": null, "MetadataId": "00000000-0000-0000-0000-000000000005" },
        { "LogicalName": "primarycontactidname", "AttributeType": "String", "AttributeOf": "primarycontactid", "MetadataId": "00000000-0000-0000-0000-000000000006" },
        { "LogicalName": "industrycodename", "AttributeType": "Virtual", "AttributeOf": "industrycode", "MetadataId": "00000000-0000-0000-0000-000000000007" }
      ]
    }
    """;

    [Fact]
    public async Task Parses_entity_and_filters_secondary_attributes()
    {
        var stub = new StubHandler(HttpStatusCode.OK, AccountMetadataJson);
        var connector = ConnectorWith(stub);

        var metadata = await connector.GetMetadataAsync("account", CancellationToken.None);

        Assert.Equal("account", metadata.EntityName);
        Assert.Equal("accountid", metadata.PrimaryKeyField);
        Assert.Equal(["accountid", "accountnumber", "industrycode", "name", "primarycontactid"],
            metadata.Fields.Keys.OrderBy(k => k, StringComparer.Ordinal));
        Assert.Equal("Picklist", metadata.Fields["industrycode"]);
        // Los secundarios (AttributeOf != null) no llegan al diseñador
        Assert.False(metadata.Fields.ContainsKey("primarycontactidname"));
        Assert.False(metadata.Fields.ContainsKey("industrycodename"));
    }

    [Fact]
    public async Task Requests_entity_definitions_with_odata_headers()
    {
        var stub = new StubHandler(HttpStatusCode.OK, AccountMetadataJson);
        var connector = ConnectorWith(stub);

        await connector.GetMetadataAsync("account", CancellationToken.None);

        var sent = Assert.Single(stub.Requests);
        Assert.StartsWith("https://unit.test/api/data/v9.2/EntityDefinitions(LogicalName='account')", sent.RequestUri!.ToString());
        Assert.Contains("$expand=Attributes", sent.RequestUri.ToString());
        Assert.Equal("4.0", Assert.Single(sent.Headers.GetValues("OData-Version")));
    }

    [Fact]
    public async Task Unknown_entity_throws_clear_error()
    {
        var connector = ConnectorWith(new StubHandler(HttpStatusCode.NotFound, """{"error":{"message":"Not Found"}}"""));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => connector.GetMetadataAsync("noexiste", CancellationToken.None));

        Assert.Contains("noexiste", ex.Message);
    }

    [Theory]
    [InlineData("")]
    [InlineData("Account")]           // mayúsculas: no es un logical name
    [InlineData("account'/../hack")]  // nada interpolable en la URL
    public async Task Invalid_logical_names_are_rejected_before_any_request(string entityName)
    {
        var stub = new StubHandler(HttpStatusCode.OK, AccountMetadataJson);
        var connector = ConnectorWith(stub);

        await Assert.ThrowsAsync<ArgumentException>(() => connector.GetMetadataAsync(entityName, CancellationToken.None));
        Assert.Empty(stub.Requests);
    }

    private static DataverseConnector ConnectorWith(StubHandler stub) => new(
        new HttpClient(stub) { BaseAddress = new Uri("https://unit.test/") },
        new EntraAppOptions { EnvironmentUrl = "https://unit.test" });

    private sealed class StubHandler(HttpStatusCode status, string body) : HttpMessageHandler
    {
        public List<HttpRequestMessage> Requests { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Requests.Add(request);
            return Task.FromResult(new HttpResponseMessage(status)
            {
                Content = new StringContent(body, System.Text.Encoding.UTF8, "application/json"),
            });
        }
    }
}
