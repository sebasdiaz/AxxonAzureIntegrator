using Axxon.Integrator.Core.Model;
using Axxon.Integrator.Core.Sync;
using Xunit;

namespace Axxon.Integrator.Tests;

/// <summary>
/// Aplicación del mapa sobre un evento. El caso central es el de los data events de
/// F&O: los campos llegan en minúsculas y sin prefijo mserp_ (los normaliza el
/// parser), el mapa usa los nombres públicos OData, y el diccionario del evento
/// perdió cualquier comparer al viajar serializado por el topic.
/// </summary>
public sealed class MappingEngineTests
{
    private readonly MappingEngine _engine = new();

    [Fact]
    public void Apply_matches_source_fields_case_insensitively()
    {
        var map = new EntityMap
        {
            Name = "grupos",
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

        // Claves como las deja el parser de data events (minúsculas, sin mserp_) en un
        // diccionario case-sensitive, como queda tras deserializar del topic.
        var evt = new ChangeEvent
        {
            SourceSystem = "finops",
            EntityName = "mserp_custcustomergroupentity",
            SourceRecordId = "00004951-0000-0000-4d1d-005001000000",
            Operation = ChangeOperation.Create,
            OccurredAt = DateTimeOffset.UtcNow,
            Company = "alas",
            Data = new Dictionary<string, object?>
            {
                ["customergroupid"] = "Test 2",
                ["description"] = "Test 2",
                ["issalestaxincludedinprice"] = 200000000L, // OptionSetValue aplanado
            },
        };

        var payload = _engine.Apply(map, evt, targetRecordId: null);

        Assert.Equal("Test 2", payload.Fields["msdyn_groupid"]);
        Assert.Equal("Test 2", payload.Fields["msdyn_description"]);
        Assert.Equal("false", payload.Fields["msdyn_issalestaxincludedinprice"]); // ValueMap sobre el valor aplanado
    }
}
