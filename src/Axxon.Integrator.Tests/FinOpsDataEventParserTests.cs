using Axxon.Integrator.Connectors.FinOps;
using Axxon.Integrator.Core.Model;
using Xunit;

namespace Axxon.Integrator.Tests;

/// <summary>
/// Parseo del payload RemoteExecutionContext de los data events de F&O. El sample
/// reproduce la forma documentada (Target en InputParameters, envelope de business
/// events, fechas /Date(ms)/ de DataContract); el spike con payloads reales de la
/// cola valida los detalles que la documentación no publica.
/// </summary>
public sealed class FinOpsDataEventParserTests
{
    private readonly FinOpsDataEventParser _parser = new();

    private const string CreatePayload = """
    {
      "BusinessEventId": "CustCustomerV3Entity_DataEvent_Create",
      "ControlNumber": 5637144576,
      "EventId": "0f8fad5b-d9cb-469f-a165-70867728950e",
      "EventTime": "/Date(1720000000000)/",
      "MajorVersion": 0,
      "MinorVersion": 0,
      "CorrelationId": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
      "InitiatingUserAzureActiveDirectoryObjectId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
      "MessageName": "Create",
      "PrimaryEntityId": "9b2c1de0-1234-4c9a-8f00-aaaaaaaaaaaa",
      "PrimaryEntityName": "CustCustomerV3Entity",
      "InputParameters": [
        {
          "key": "Target",
          "value": {
            "__type": "Entity:http://schemas.microsoft.com/xrm/2011/Contracts",
            "Attributes": [
              { "key": "CustomerAccount", "value": "CUST-001" },
              { "key": "dataAreaId", "value": "usmf" },
              { "key": "NameAlias", "value": "Contoso" },
              { "key": "CreditLimit", "value": { "__type": "Money:http://schemas.microsoft.com/xrm/2011/Contracts", "Value": 1500.5 } },
              { "key": "Blocked", "value": { "__type": "OptionSetValue:http://schemas.microsoft.com/xrm/2011/Contracts", "Value": 0 } },
              { "key": "InvoiceAccount", "value": { "__type": "EntityReference:http://schemas.microsoft.com/xrm/2011/Contracts", "Id": "1b2c3d4e-0000-4000-8000-bbbbbbbbbbbb", "LogicalName": "CustCustomerV3Entity" } },
              { "key": "ModifiedDateTime", "value": "/Date(1719999999000)/" },
              { "key": "MemoField", "value": null }
            ],
            "Id": "9b2c1de0-1234-4c9a-8f00-aaaaaaaaaaaa",
            "LogicalName": "CustCustomerV3Entity"
          }
        }
      ]
    }
    """;

    [Fact]
    public void Parse_create_extracts_normalized_event()
    {
        var evt = _parser.Parse(BinaryData.FromString(CreatePayload));

        Assert.Equal("finops", evt.SourceSystem);
        Assert.Equal(ChangeOperation.Create, evt.Operation);
        Assert.Equal("CustCustomerV3Entity", evt.EntityName);
        Assert.Equal("9b2c1de0-1234-4c9a-8f00-aaaaaaaaaaaa", evt.SourceRecordId);
        Assert.Equal("usmf", evt.Company);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1720000000000), evt.OccurredAt);
        Assert.Equal("3fa85f64-5717-4562-b3fc-2c963f66afa6", evt.OriginatingUserId);
        Assert.Equal("7c9e6679-7425-40de-944b-e07fc1f90ae7", evt.CorrelationId);

        Assert.Equal("CUST-001", evt.Data["CustomerAccount"]);
        Assert.Equal(1500.5m, evt.Data["CreditLimit"]); // Money aplanado a su Value
        Assert.Equal(0L, evt.Data["Blocked"]); // OptionSetValue aplanado a su Value
        Assert.Equal("1b2c3d4e-0000-4000-8000-bbbbbbbbbbbb", evt.Data["InvoiceAccount"]); // EntityReference aplanado a su Id
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1719999999000), evt.Data["ModifiedDateTime"]);
        Assert.Null(evt.Data["MemoField"]);
    }

    [Fact]
    public void Parse_without_envelope_falls_back_to_OperationCreatedOn_and_generates_correlation()
    {
        const string payload = """
        {
          "MessageName": "Update",
          "OperationCreatedOn": "/Date(1720000123456)/",
          "PrimaryEntityId": "9b2c1de0-1234-4c9a-8f00-aaaaaaaaaaaa",
          "PrimaryEntityName": "CustCustomerV3Entity",
          "CorrelationId": "00000000-0000-0000-0000-000000000000",
          "InputParameters": [
            { "key": "Target", "value": { "Attributes": [ { "key": "NameAlias", "value": "Contoso 2" } ] } }
          ]
        }
        """;

        var evt = _parser.Parse(BinaryData.FromString(payload));

        Assert.Equal(ChangeOperation.Update, evt.Operation);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1720000123456), evt.OccurredAt);
        Assert.Null(evt.Company);
        Assert.Null(evt.OriginatingUserId);
        Assert.False(string.IsNullOrWhiteSpace(evt.CorrelationId)); // el GUID vacío no cuenta como correlación
        Assert.NotEqual("00000000-0000-0000-0000-000000000000", evt.CorrelationId);
    }

    [Theory]
    [InlineData("Assign")]
    [InlineData("")]
    public void Parse_rejects_unsupported_message_name(string messageName)
    {
        var payload = $$"""
        {
          "MessageName": "{{messageName}}",
          "InputParameters": []
        }
        """;

        Assert.Throws<FormatException>(() => _parser.Parse(BinaryData.FromString(payload)));
    }

    [Fact]
    public void Parse_without_target_is_permanent_error()
    {
        const string payload = """
        {
          "MessageName": "Create",
          "EventTime": "/Date(1720000000000)/",
          "InputParameters": []
        }
        """;

        Assert.Throws<FormatException>(() => _parser.Parse(BinaryData.FromString(payload)));
    }

    [Theory]
    [InlineData(CreatePayload, true)] // señal fuerte: BusinessEventId
    [InlineData("""{ "MessageName": "Create", "InputParameters": [] }""", true)] // señal débil: forma RemoteExecutionContext
    [InlineData("""{ "sujeto": "otro sistema" }""", false)]
    [InlineData("""[1, 2, 3]""", false)]
    [InlineData("esto no es json", false)]
    public void CanParse_sniffs_payload_shape(string payload, bool expected) =>
        Assert.Equal(expected, _parser.CanParse(BinaryData.FromString(payload)));
}
