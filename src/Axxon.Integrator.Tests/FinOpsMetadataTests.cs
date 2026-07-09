using System.Net;
using Axxon.Integrator.Azure;
using Axxon.Integrator.Connectors.FinOps;
using Xunit;

namespace Axxon.Integrator.Tests;

/// <summary>
/// GetMetadataAsync / ListEntitiesAsync contra un handler stub con la forma del
/// Metadata service REST de F&O ({env}/metadata, JSON — no el $metadata CSDL).
/// </summary>
public sealed class FinOpsMetadataTests
{
    private const string PublicEntitiesJson = """
    {
      "@odata.context": "https://unit.test/Metadata/$metadata#PublicEntities",
      "value": [
        {
          "Name": "CustomerV3",
          "EntitySetName": "CustomersV3",
          "LabelId": "@SYS342805",
          "IsReadOnly": false,
          "ConfigurationEnabled": true,
          "Properties": [
            { "Name": "dataAreaId", "TypeName": "Edm.String", "DataType": "String", "IsKey": true, "IsMandatory": true, "ConfigurationEnabled": true, "AllowEdit": true, "AllowEditOnCreate": true, "IsDimension": false },
            { "Name": "CustomerAccount", "TypeName": "Edm.String", "DataType": "String", "IsKey": true, "IsMandatory": true, "ConfigurationEnabled": true, "AllowEdit": false, "AllowEditOnCreate": true, "IsDimension": false },
            { "Name": "OrganizationName", "TypeName": "Edm.String", "DataType": "String", "IsKey": false, "IsMandatory": false, "ConfigurationEnabled": true, "AllowEdit": true, "AllowEditOnCreate": true, "IsDimension": false },
            { "Name": "CreditLimit", "TypeName": "Edm.Decimal", "DataType": "Decimal", "IsKey": false, "IsMandatory": false, "ConfigurationEnabled": true, "AllowEdit": true, "AllowEditOnCreate": true, "IsDimension": false },
            { "Name": "IsOneTimeCustomer", "TypeName": "Microsoft.Dynamics.DataEntities.NoYes", "DataType": "Enum", "IsKey": false, "IsMandatory": false, "ConfigurationEnabled": true, "AllowEdit": true, "AllowEditOnCreate": true, "IsDimension": false }
          ]
        }
      ]
    }
    """;

    private const string DataEntitiesJson = """
    {
      "@odata.context": "https://unit.test/Metadata/$metadata#DataEntities(PublicCollectionName)",
      "value": [
        { "PublicCollectionName": "Vendors" },
        { "PublicCollectionName": "CustomersV3" },
        { "PublicCollectionName": "ReleasedProductsV2" }
      ]
    }
    """;

    [Fact]
    public async Task Parses_entity_with_composite_key()
    {
        var connector = ConnectorWith(new StubHandler(HttpStatusCode.OK, PublicEntitiesJson));

        var metadata = await connector.GetMetadataAsync("CustomersV3", CancellationToken.None);

        Assert.Equal("CustomersV3", metadata.EntityName);
        // Clave compuesta, en el orden del payload: empresa + clave natural
        Assert.Equal(["dataAreaId", "CustomerAccount"], metadata.KeyFields);
        Assert.Equal("String", metadata.Fields["OrganizationName"]);
        Assert.Equal("Decimal", metadata.Fields["CreditLimit"]);
        // El namespace del enum se recorta a su nombre
        Assert.Equal("NoYes", metadata.Fields["IsOneTimeCustomer"]);
    }

    [Fact]
    public async Task Queries_public_entities_by_entity_set_name()
    {
        var stub = new StubHandler(HttpStatusCode.OK, PublicEntitiesJson);
        var connector = ConnectorWith(stub);

        await connector.GetMetadataAsync("CustomersV3", CancellationToken.None);

        var sent = Assert.Single(stub.Requests);
        Assert.StartsWith("https://unit.test/metadata/PublicEntities", sent.RequestUri!.ToString());
        Assert.Contains("EntitySetName", sent.RequestUri.ToString());
        Assert.Equal("4.0", Assert.Single(sent.Headers.GetValues("OData-Version")));
    }

    [Fact]
    public async Task Unknown_entity_set_throws_clear_error()
    {
        var connector = ConnectorWith(new StubHandler(HttpStatusCode.OK, """{"value":[]}"""));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => connector.GetMetadataAsync("NoExiste", CancellationToken.None));

        Assert.Contains("NoExiste", ex.Message);
    }

    [Fact]
    public async Task Lists_data_service_enabled_collections_sorted()
    {
        var stub = new StubHandler(HttpStatusCode.OK, DataEntitiesJson);
        var connector = ConnectorWith(stub);

        var entities = await connector.ListEntitiesAsync(CancellationToken.None);

        Assert.Equal(["CustomersV3", "ReleasedProductsV2", "Vendors"], entities);
        var sent = Assert.Single(stub.Requests);
        Assert.Contains("metadata/DataEntities", sent.RequestUri!.ToString());
        Assert.Contains("DataServiceEnabled", sent.RequestUri.ToString());
    }

    [Fact]
    public async Task Lists_legal_entities_sorted()
    {
        var stub = new StubHandler(HttpStatusCode.OK, """
        {
          "@odata.context": "https://unit.test/data/$metadata#LegalEntities(LegalEntityId)",
          "value": [
            { "LegalEntityId": "USMF" },
            { "LegalEntityId": "DAT" },
            { "LegalEntityId": "USRT" }
          ]
        }
        """);
        var connector = ConnectorWith(stub);

        var companies = await connector.ListCompaniesAsync(CancellationToken.None);

        Assert.Equal(["DAT", "USMF", "USRT"], companies);
        var sent = Assert.Single(stub.Requests);
        Assert.StartsWith("https://unit.test/data/LegalEntities", sent.RequestUri!.ToString());
    }

    [Fact]
    public async Task Missing_environment_url_reports_the_setting_name()
    {
        var options = new EntraAppOptions();
        var connector = new FinOpsConnector(EntraHttp.ClientFor(options), options);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => connector.ListEntitiesAsync(CancellationToken.None));

        Assert.Contains("FinOps:EnvironmentUrl", ex.Message);
    }

    private static FinOpsConnector ConnectorWith(StubHandler stub) => new(
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
