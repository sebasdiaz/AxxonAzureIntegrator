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

    /// <summary>
    /// Forma real capturada de la cola (data event de F&O 10.0.48): MessageName
    /// "OnExternal*", entidad virtual mserp_*, empresa en "mserp_dataareaid",
    /// PrimaryEntityId en GUID vacío (el id útil viene en Target.Id) y fecha solo
    /// en OperationCreatedOn.
    /// </summary>
    [Fact]
    public void Parse_real_data_event_with_OnExternal_message_and_mserp_fields()
    {
        const string payload = """
        {
          "CorrelationId": "844bbf91-d451-4cad-a83d-1fea5f32ebaf",
          "InitiatingUserAzureActiveDirectoryObjectId": "e3a96d5d-8187-498c-88e7-4448cc8e57fa",
          "MessageName": "OnExternalCreated",
          "OperationCreatedOn": "/Date(1784051293000+0000)/",
          "PrimaryEntityId": "00000000-0000-0000-0000-000000000000",
          "PrimaryEntityName": "mserp_custcustomergroupentity",
          "InputParameters": [
            {
              "key": "Target",
              "value": {
                "__type": "Entity:http://schemas.microsoft.com/xrm/2011/Contracts",
                "Attributes": [
                  { "key": "mserp_customergroupid", "value": "Default" },
                  { "key": "mserp_issalestaxincludedinprice", "value": { "__type": "OptionSetValue:http://schemas.microsoft.com/xrm/2011/Contracts", "Value": 200000000 } },
                  { "key": "mserp_dataareaid", "value": "alas" }
                ],
                "Id": "00004951-0000-0000-5e1a-005001000000",
                "LogicalName": "mserp_custcustomergroupentity"
              }
            }
          ]
        }
        """;

        var evt = _parser.Parse(BinaryData.FromString(payload));

        Assert.Equal(ChangeOperation.Create, evt.Operation);
        Assert.Equal("mserp_custcustomergroupentity", evt.EntityName);
        Assert.Equal("00004951-0000-0000-5e1a-005001000000", evt.SourceRecordId); // Target.Id: el PrimaryEntityId vacío no cuenta
        Assert.Equal("alas", evt.Company); // mserp_dataareaid
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1784051293000), evt.OccurredAt);
        Assert.Equal(200000000L, evt.Data["mserp_issalestaxincludedinprice"]);
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
