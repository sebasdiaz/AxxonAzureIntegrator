using System.Net;
using System.Text.Json;
using Axxon.Integrator.Azure;
using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Model;

namespace Axxon.Integrator.Connectors.Dataverse;

/// <summary>
/// Conector de Dataverse.
/// - Captura viva: service endpoints nativos de Dataverse (steps registrados sobre
///   Create/Update/Delete que publican a la cola 'ingest' con una SAS de solo Send).
///   Este conector solo aporta el parser; el registro del step es configuración.
/// - Escritura: Web API (upsert por GUID o clave alternativa), autenticada con la app
///   registration de Entra ID (decisión 14) detrás de un application user dedicado —
///   su systemuserid (Options.IntegrationUserId) alimenta la supresión de eco.
/// - Sync inicial y catch-up: Web API con change tracking (delta tokens).
/// </summary>
public sealed class DataverseConnector(HttpClient http, EntraAppOptions options) : IConnector
{
    /// <summary>Cliente con bearer de la app registration; BaseAddress = ambiente Dataverse (la Web API cuelga de /api/data/v9.2/).</summary>
    private HttpClient Http { get; } = http;

    private EntraAppOptions Options { get; } = options;

    public string SystemName => "dataverse";

    public IAsyncEnumerable<ChangeEvent> PullChangesAsync(Watermark since, CancellationToken ct) =>
        throw new NotImplementedException("Delta query con change tracking de la Web API. Fase 3.");

    public async Task<UpsertResult> UpsertAsync(EntityPayload payload, CancellationToken ct)
    {
        EnsureEnvironmentConfigured();
        ValidateLogicalName(payload.EntityName, nameof(payload));

        var entity = await GetEntityRefAsync(payload.EntityName, ct);
        var targetId = payload.TargetRecordId ?? await ResolveByIntegrationKeyAsync(entity, payload, ct);

        if (targetId is not null)
        {
            // PATCH sin If-Match = upsert nativo por primary key: idempotente ante
            // redeliveries at-least-once, y tolera un xref que apunte a un registro
            // borrado (lo recrea con el mismo GUID en vez de fallar).
            if (!Guid.TryParse(targetId, out var recordGuid))
            {
                throw new InvalidOperationException($"El id destino '{targetId}' no es un GUID de Dataverse ({payload.CorrelationId}).");
            }

            using var patch = ODataRequest.Patch($"api/data/v9.2/{entity.SetName}({recordGuid})", ODataRequest.JsonContent(payload.Fields));
            using var patchResponse = await Http.SendAsync(patch, ct);
            await ODataRequest.EnsureSuccessAsync(patchResponse, ct);
            return new UpsertResult { TargetRecordId = recordGuid.ToString(), Created = false };
        }

        using var post = ODataRequest.Post($"api/data/v9.2/{entity.SetName}", ODataRequest.JsonContent(payload.Fields));
        using var postResponse = await Http.SendAsync(post, ct);
        await ODataRequest.EnsureSuccessAsync(postResponse, ct);
        return new UpsertResult { TargetRecordId = ExtractEntityId(postResponse), Created = true };
    }

    public async Task DeleteAsync(EntityPayload payload, CancellationToken ct)
    {
        EnsureEnvironmentConfigured();
        ValidateLogicalName(payload.EntityName, nameof(payload));

        var entity = await GetEntityRefAsync(payload.EntityName, ct);
        var targetId = payload.TargetRecordId ?? await ResolveByIntegrationKeyAsync(entity, payload, ct);
        if (targetId is null)
        {
            return; // nunca se sincronizó y no hay match por clave: no hay nada que borrar
        }

        using var request = ODataRequest.Delete($"api/data/v9.2/{entity.SetName}({Guid.Parse(targetId)})");
        using var response = await Http.SendAsync(request, ct);
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return; // ya no existe: el delete es idempotente
        }
        await ODataRequest.EnsureSuccessAsync(response, ct);
    }

    /// <summary>
    /// Primer match contra el destino cuando el xref no tiene vínculo: consulta por la
    /// integration key del mapa. Nunca create ciego — Service Bus es at-least-once y
    /// sin esta resolución cada redelivery duplicaría el registro.
    /// </summary>
    private async Task<string?> ResolveByIntegrationKeyAsync(EntityRef entity, EntityPayload payload, CancellationToken ct)
    {
        if (payload.IntegrationKey.Count == 0)
        {
            throw new InvalidOperationException(
                $"Sin vínculo en el xref y sin integration key en el mapa para {payload.EntityName} ({payload.CorrelationId}): imposible resolver el destino sin riesgo de duplicar.");
        }

        var criteria = new List<string>(payload.IntegrationKey.Count);
        foreach (var key in payload.IntegrationKey)
        {
            ValidateLogicalName(key, nameof(payload));
            if (!payload.Fields.TryGetValue(key, out var value) || value is null)
            {
                throw new InvalidOperationException(
                    $"La integration key '{key}' no tiene valor en el payload de {payload.EntityName} ({payload.CorrelationId}).");
            }
            criteria.Add($"{key} eq {ODataLiteral.Format(value)}");
        }

        var url = $"api/data/v9.2/{entity.SetName}?$select={entity.PrimaryKey}" +
                  $"&$filter={Uri.EscapeDataString(string.Join(" and ", criteria))}&$top=2";
        using var request = ODataRequest.Get(url);
        using var response = await Http.SendAsync(request, ct);
        await ODataRequest.EnsureSuccessAsync(response, ct);

        using var doc = await JsonDocument.ParseAsync(await response.Content.ReadAsStreamAsync(ct), cancellationToken: ct);
        var matches = doc.RootElement.GetProperty("value");
        return matches.GetArrayLength() switch
        {
            0 => null,
            1 => matches[0].GetProperty(entity.PrimaryKey).GetString(),
            _ => throw new InvalidOperationException(
                $"La integration key de {payload.EntityName} matchea más de un registro ({payload.CorrelationId}): clave ambigua, corregir el mapa o los datos."),
        };
    }

    /// <summary>LogicalName → (entity set para la URL, primary key). Estable: se cachea de por vida del conector.</summary>
    private async Task<EntityRef> GetEntityRefAsync(string entityName, CancellationToken ct)
    {
        if (_entityRefs.TryGetValue(entityName, out var cached))
        {
            return cached;
        }

        using var request = ODataRequest.Get($"api/data/v9.2/EntityDefinitions(LogicalName='{entityName}')?$select=EntitySetName,PrimaryIdAttribute");
        using var response = await Http.SendAsync(request, ct);
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            throw new InvalidOperationException($"La entidad '{entityName}' no existe en {Options.EnvironmentUrl}.");
        }
        await ODataRequest.EnsureSuccessAsync(response, ct);

        using var doc = await JsonDocument.ParseAsync(await response.Content.ReadAsStreamAsync(ct), cancellationToken: ct);
        var entity = new EntityRef(
            doc.RootElement.GetProperty("EntitySetName").GetString()!,
            doc.RootElement.GetProperty("PrimaryIdAttribute").GetString()!);
        _entityRefs[entityName] = entity;
        return entity;
    }

    private static string ExtractEntityId(HttpResponseMessage response)
    {
        // OData-EntityId: https://org.crm.dynamics.com/api/data/v9.2/accounts(guid)
        var entityId = response.Headers.TryGetValues("OData-EntityId", out var values) ? values.FirstOrDefault() : null;
        var start = entityId?.LastIndexOf('(') ?? -1;
        if (entityId is null || start < 0 || !entityId.EndsWith(')'))
        {
            throw new FormatException("La respuesta de creación no trae el header OData-EntityId esperado.");
        }
        return entityId[(start + 1)..^1];
    }

    private sealed record EntityRef(string SetName, string PrimaryKey);

    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, EntityRef> _entityRefs = new(StringComparer.OrdinalIgnoreCase);

    public IAsyncEnumerable<EntityPayload> ExportAsync(EntityQuery query, CancellationToken ct) =>
        throw new NotImplementedException("Export paginado vía Web API. Fase 3 (sync inicial).");

    public async Task<EntityMetadata> GetMetadataAsync(string entityName, CancellationToken ct)
    {
        EnsureEnvironmentConfigured();
        ValidateLogicalName(entityName, nameof(entityName));

        // Una sola llamada: definición + atributos expandidos. Las consultas de
        // metadata no paginan — la primera respuesta trae todo.
        var url = $"api/data/v9.2/EntityDefinitions(LogicalName='{entityName}')" +
                  "?$select=LogicalName,PrimaryIdAttribute" +
                  "&$expand=Attributes($select=LogicalName,AttributeType,AttributeOf)";

        using var request = ODataRequest.Get(url);
        using var response = await Http.SendAsync(request, ct);
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            throw new InvalidOperationException($"La entidad '{entityName}' no existe en {Options.EnvironmentUrl}.");
        }
        response.EnsureSuccessStatusCode();

        using var doc = await JsonDocument.ParseAsync(await response.Content.ReadAsStreamAsync(ct), cancellationToken: ct);
        var root = doc.RootElement;

        var fields = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var attribute in root.GetProperty("Attributes").EnumerateArray())
        {
            // AttributeOf != null marca atributos secundarios (el "...name" de un
            // lookup, los "yomi..."): no son mapeables, solo ruido para el diseñador.
            if (attribute.TryGetProperty("AttributeOf", out var attributeOf) &&
                attributeOf.ValueKind is not JsonValueKind.Null)
            {
                continue;
            }

            var name = attribute.GetProperty("LogicalName").GetString()!;
            fields[name] = attribute.GetProperty("AttributeType").GetString() ?? "Unknown";
        }

        return new EntityMetadata
        {
            EntityName = root.GetProperty("LogicalName").GetString()!,
            KeyFields = [root.GetProperty("PrimaryIdAttribute").GetString()!],
            Fields = fields,
        };
    }

    public async Task<IReadOnlyList<string>> ListEntitiesAsync(CancellationToken ct)
    {
        EnsureEnvironmentConfigured();

        // IsIntersect eq false: fuera las tablas de relación N:N, que no se mapean.
        // Como toda consulta de metadata, no pagina: una respuesta trae todo.
        using var request = ODataRequest.Get("api/data/v9.2/EntityDefinitions?$select=LogicalName&$filter=IsIntersect%20eq%20false");
        using var response = await Http.SendAsync(request, ct);
        response.EnsureSuccessStatusCode();

        using var doc = await JsonDocument.ParseAsync(await response.Content.ReadAsStreamAsync(ct), cancellationToken: ct);

        return [.. doc.RootElement.GetProperty("value").EnumerateArray()
            .Select(e => e.GetProperty("LogicalName").GetString()!)
            .OrderBy(n => n, StringComparer.Ordinal)];
    }

    public async Task<IReadOnlyDictionary<string, string>> GetOptionSetAsync(string entityName, string fieldName, CancellationToken ct)
    {
        EnsureEnvironmentConfigured();
        ValidateLogicalName(entityName, nameof(entityName));
        ValidateLogicalName(fieldName, nameof(fieldName));

        // Cast del atributo a PicklistAttributeMetadata: expone Options tanto del
        // option set local como del global. Si el campo no es picklist (o no existe),
        // el cast devuelve 404 → no hay valores que ofrecer, no es un error.
        var url = $"api/data/v9.2/EntityDefinitions(LogicalName='{entityName}')" +
                  $"/Attributes(LogicalName='{fieldName}')/Microsoft.Dynamics.CRM.PicklistAttributeMetadata" +
                  "?$select=LogicalName&$expand=OptionSet($select=Options),GlobalOptionSet($select=Options)";

        using var request = ODataRequest.Get(url);
        using var response = await Http.SendAsync(request, ct);
        if (response.StatusCode is HttpStatusCode.NotFound or HttpStatusCode.BadRequest)
        {
            return new Dictionary<string, string>();
        }
        response.EnsureSuccessStatusCode();

        using var doc = await JsonDocument.ParseAsync(await response.Content.ReadAsStreamAsync(ct), cancellationToken: ct);
        var root = doc.RootElement;

        var values = new Dictionary<string, string>();
        foreach (var setName in new[] { "OptionSet", "GlobalOptionSet" })
        {
            if (!root.TryGetProperty(setName, out var set) || set.ValueKind != JsonValueKind.Object ||
                !set.TryGetProperty("Options", out var options) || options.ValueKind != JsonValueKind.Array)
            {
                continue;
            }

            foreach (var option in options.EnumerateArray())
            {
                var value = option.GetProperty("Value").GetInt32().ToString(System.Globalization.CultureInfo.InvariantCulture);
                var label = value;
                if (option.TryGetProperty("Label", out var lbl) && lbl.ValueKind == JsonValueKind.Object &&
                    lbl.TryGetProperty("UserLocalizedLabel", out var localized) && localized.ValueKind == JsonValueKind.Object &&
                    localized.TryGetProperty("Label", out var text) && text.ValueKind == JsonValueKind.String)
                {
                    label = text.GetString()!;
                }
                values[value] = label;
            }

            if (values.Count > 0)
            {
                break; // local y global son excluyentes: usa el que tenga opciones
            }
        }
        return values;
    }

    private static void ValidateLogicalName(string value, string paramName)
    {
        // Logical names de Dataverse: minúsculas ASCII, dígitos y guión bajo. Se
        // valida antes de interpolar en la URL.
        if (value.Length == 0 || !value.All(c => char.IsAsciiLetterLower(c) || char.IsAsciiDigit(c) || c == '_'))
        {
            throw new ArgumentException($"Logical name de Dataverse inválido: '{value}'.", paramName);
        }
    }

    public Task<IReadOnlyList<string>> ListCompaniesAsync(CancellationToken ct) =>
        // La noción de empresa (dataAreaId) es de F&O; un Dataverse vanilla no tiene
        // tabla de empresas (cdm_company existe solo con Dual Write instalado). El
        // checklist del diseñador se alimenta del lado F&O.
        Task.FromResult<IReadOnlyList<string>>([]);

    /// <summary>
    /// Sin EnvironmentUrl el HttpClient no tiene BaseAddress y HttpClient tiraría un
    /// críptico "BaseAddress must be set"; este guard lo convierte en el error real.
    /// </summary>
    private void EnsureEnvironmentConfigured()
    {
        if (Http.BaseAddress is null)
        {
            throw new InvalidOperationException(
                "Falta Dataverse:EnvironmentUrl en la configuración (appsettings/user-secrets en el portal, local.settings.json en el SyncEngine).");
        }
    }

}
