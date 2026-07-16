using Axxon.Integrator.Core.Model;
using Axxon.Integrator.Core.Sync;
using Xunit;

namespace Axxon.Integrator.Tests;

/// <summary>
/// Resolución de campos origen del MappingEngine: el nombre del mapa (diseñador, OData)
/// y el del evento (alias en minúsculas del parser de F&O) difieren en casing y el
/// diccionario llega del topic sin comparer — la resolución debe tolerarlo.
/// </summary>
public sealed class MappingEngineTests
{
    private static readonly EntityMap DesignerStyleMap = new()
    {
        Name = "Customer groups (msdyn_customergroups)",
        SourceSystem = "finops",
        SourceEntity = "CustomerGroups",
        SourceEventEntity = "mserp_custcustomergroupentity",
        TargetSystem = "dataverse",
        TargetEntity = "msdyn_customergroup",
        Fields =
        [
            new FieldMap { Source = "CustomerGroupId", Target = "msdyn_groupid" },
            new FieldMap { Source = "Description", Target = "msdyn_description" },
            new FieldMap
            {
                Source = "IsSalesTaxIncludedInPrice",
                Target = "msdyn_issalestaxincludedinprice",
                ValueMap = new Dictionary<string, string> { ["200000000"] = "false", ["200000001"] = "true" },
            },
        ],
        IntegrationKey = ["msdyn_groupid"],
    };

    [Fact]
    public void Source_fields_resolve_case_insensitively_against_event_aliases()
    {
        // Data como lo publica el parser de F&O: campos mserp_* y sus alias sin
        // prefijo, todo en minúsculas (acá sin comparer, como al deserializar del topic).
        var evt = new ChangeEvent
        {
            SourceSystem = "finops",
            EntityName = "mserp_custcustomergroupentity",
            SourceRecordId = "00004951-0000-0000-5e1a-005001000000",
            Operation = ChangeOperation.Create,
            OccurredAt = DateTimeOffset.UnixEpoch,
            Company = "alas",
            Data = new Dictionary<string, object?>
            {
                ["mserp_customergroupid"] = "Default",
                ["customergroupid"] = "Default",
                ["mserp_description"] = "Default group ALAS",
                ["description"] = "Default group ALAS",
                ["mserp_issalestaxincludedinprice"] = 200000000L,
                ["issalestaxincludedinprice"] = 200000000L,
            },
        };

        var payload = new MappingEngine().Apply(DesignerStyleMap, evt, targetRecordId: null);

        Assert.Equal("Default", payload.Fields["msdyn_groupid"]);
        Assert.Equal("Default group ALAS", payload.Fields["msdyn_description"]);
        Assert.Equal("false", payload.Fields["msdyn_issalestaxincludedinprice"]); // el ValueMap corre tras resolver el alias
    }

    [Fact]
    public void Missing_source_field_still_skips_write()
    {
        var evt = new ChangeEvent
        {
            SourceSystem = "finops",
            EntityName = "mserp_custcustomergroupentity",
            SourceRecordId = "x",
            Operation = ChangeOperation.Update,
            OccurredAt = DateTimeOffset.UnixEpoch,
            Data = new Dictionary<string, object?> { ["customergroupid"] = "Default" },
        };

        var payload = new MappingEngine().Apply(DesignerStyleMap, evt, targetRecordId: null);

        Assert.True(payload.Fields.ContainsKey("msdyn_groupid"));
        Assert.False(payload.Fields.ContainsKey("msdyn_description")); // ausente sigue sin escribirse
    }
}
