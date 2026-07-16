using System.Net;
using System.Text.Json;
using Axxon.Integrator.Azure;
using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Model;

namespace Axxon.Integrator.Connectors.FinOps;

/// <summary>
/// Conector de Dynamics 365 Finance &amp; Operations. Sin desarrollo X++:
/// - Captura viva: data events nativos de F&O publicando a la cola 'ingest' de Service
///   Bus (configurado en Sys admin > Business events; requiere change tracking en la
///   data entity y el secret del bus en Key Vault). Este conector solo aporta el parser.
/// - Escritura: OData sobre data entities (upsert por clave de entidad), autenticada
///   con la app registration de Entra ID (decisión 14). La app debe registrarse en
///   F&O (Sys admin > Microsoft Entra applications) asociada a un usuario de servicio,
///   cuyo ID (Options.IntegrationUserId) alimenta la supresión de eco.
/// - Sync inicial: DMF (paquetes de exportación), nunca data events — el ambiente
///   soporta ~5.000 eventos/5 min y ~50.000/hora.
/// </summary>
public sealed class FinOpsConnector(HttpClient http, EntraAppOptions options) : IConnector
{
    /// <summary>Cliente con bearer de la app registration; BaseAddress = ambiente F&O.</summary>
    private HttpClient Http { get; } = http;

    private EntraAppOptions Options { get; } = options;

    public string SystemName => "finops";

    public IAsyncEnumerable<ChangeEvent> PullChangesAsync(Watermark since, CancellationToken ct) =>
        throw new NotImplementedException("Catch-up por OData con change tracking. Fase 3.");

    public async Task<UpsertResult> UpsertAsync(EntityPayload payload, CancellationToken ct)
    {
        EnsureEnvironmentConfigured();
        ValidateODataName(payload.EntityName, nameof(payload));

        // En F&O la integration key ES la clave OData de la entidad: el "record id"
        // que persiste el xref es este segmento canónico de clave.
        var keySegment = payload.TargetRecordId ?? BuildKeySegment(payload);

        // PATCH primero (update-only con If-Match:*): el caso dominante del sync
        // continuo es update. cross-company: la clave incluye dataAreaId y el registro
        // puede no estar en la empresa default del usuario de integración.
        using var patch = ODataRequest.Patch(
            $"data/{payload.EntityName}({keySegment})?cross-company=true",
            ODataRequest.JsonContent(BodyFor(payload, includeKeys: false)),
            ifMatchAny: true);
        using var patchResponse = await Http.SendAsync(patch, ct);
        if (patchResponse.StatusCode != HttpStatusCode.NotFound)
        {
            await ODataRequest.EnsureSuccessAsync(patchResponse, ct);
            return new UpsertResult { TargetRecordId = keySegment, Created = false };
        }

        using var post = ODataRequest.Post($"data/{payload.EntityName}", ODataRequest.JsonContent(BodyFor(payload, includeKeys: true)));
        using var postResponse = await Http.SendAsync(post, ct);
        await ODataRequest.EnsureSuccessAsync(postResponse, ct);
        return new UpsertResult { TargetRecordId = keySegment, Created = true };
    }

    public async Task DeleteAsync(EntityPayload payload, CancellationToken ct)
    {
        EnsureEnvironmentConfigured();
        ValidateODataName(payload.EntityName, nameof(payload));

        var keySegment = payload.TargetRecordId ?? BuildKeySegment(payload);
        using var request = ODataRequest.Delete($"data/{payload.EntityName}({keySegment})?cross-company=true");
        using var response = await Http.SendAsync(request, ct);
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return; // ya no existe: el delete es idempotente
        }
        await ODataRequest.EnsureSuccessAsync(response, ct);
    }

    /// <summary>Segmento de clave OData: dataAreaId='usmf',CustomerAccount='C001'.</summary>
    private static string BuildKeySegment(EntityPayload payload)
    {
        if (payload.IntegrationKey.Count == 0)
        {
            throw new InvalidOperationException(
                $"Sin vínculo en el xref y sin integration key en el mapa para {payload.EntityName} ({payload.CorrelationId}): imposible dirigir el upsert OData.");
        }

        var parts = new List<string>(payload.IntegrationKey.Count);
        foreach (var key in payload.IntegrationKey)
        {
            ValidateODataName(key, nameof(payload));
            parts.Add($"{key}={ODataLiteral.Format(KeyValueFor(payload, key))}");
        }
        return string.Join(",", parts);
    }

    private static object KeyValueFor(EntityPayload payload, string key)
    {
        if (payload.Fields.TryGetValue(key, out var value) && value is not null)
        {
            return value;
        }

        // dataAreaId suele venir del filtro por empresa del evento, no de un campo mapeado
        if (string.Equals(key, "dataAreaId", StringComparison.OrdinalIgnoreCase) && !string.IsNullOrEmpty(payload.Company))
        {
            return payload.Company;
        }

        throw new InvalidOperationException(
            $"La integration key '{key}' no tiene valor en el payload de {payload.EntityName} ({payload.CorrelationId}).");
    }

    /// <summary>
    /// Cuerpo del write: en PATCH las claves van en la URL y F&O rechaza modificarlas;
    /// en POST van todas (más dataAreaId desde la empresa del evento si no está mapeada,
    /// para que el registro se cree en la legal entity correcta).
    /// </summary>
    private static IReadOnlyDictionary<string, object?> BodyFor(EntityPayload payload, bool includeKeys)
    {
        if (includeKeys)
        {
            if (string.IsNullOrEmpty(payload.Company) ||
                payload.Fields.Keys.Contains("dataAreaId", StringComparer.OrdinalIgnoreCase))
            {
                return payload.Fields;
            }
            var withCompany = new Dictionary<string, object?>(payload.Fields, StringComparer.OrdinalIgnoreCase)
            {
                ["dataAreaId"] = payload.Company,
            };
            return withCompany;
        }

        return payload.Fields
            .Where(f => !payload.IntegrationKey.Contains(f.Key, StringComparer.OrdinalIgnoreCase))
            .ToDictionary(f => f.Key, f => f.Value, StringComparer.OrdinalIgnoreCase);
    }

    public IAsyncEnumerable<EntityPayload> ExportAsync(EntityQuery query, CancellationToken ct) =>
        throw new NotImplementedException("Export masivo vía DMF package API. Fase 3 (sync inicial).");

    public async Task<EntityMetadata> GetMetadataAsync(string entityName, CancellationToken ct)
    {
        EnsureEnvironmentConfigured();
        ValidateODataName(entityName, nameof(entityName));

        // Metadata service REST (JSON), no el $metadata CSDL (XML de decenas de MB).
        // Se filtra por EntitySetName porque los mapas usan el nombre de colección
        // OData — el mismo con el que se escribe en /data/{coleccion}.
        using var request = ODataRequest.Get(
            $"metadata/PublicEntities?$filter=EntitySetName%20eq%20'{entityName}'");
        using var response = await Http.SendAsync(request, ct);
        response.EnsureSuccessStatusCode();

        using var doc = await JsonDocument.ParseAsync(await response.Content.ReadAsStreamAsync(ct), cancellationToken: ct);
        var matches = doc.RootElement.GetProperty("value");
        if (matches.GetArrayLength() == 0)
        {
            throw new InvalidOperationException(
                $"No hay ninguna public entity con EntitySetName '{entityName}' en {Options.EnvironmentUrl}.");
        }

        var entity = matches[0];
        var keyFields = new List<string>();
        var fields = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var property in entity.GetProperty("Properties").EnumerateArray())
        {
            var name = property.GetProperty("Name").GetString()!;
            // "Edm.String" -> "String"; enums "Microsoft.Dynamics.DataEntities.NoYes" -> "NoYes"
            var typeName = property.GetProperty("TypeName").GetString() ?? "Unknown";
            fields[name] = typeName[(typeName.LastIndexOf('.') + 1)..];

            if (property.TryGetProperty("IsKey", out var isKey) && isKey.ValueKind == JsonValueKind.True)
            {
                keyFields.Add(name);
            }
        }

        return new EntityMetadata
        {
            EntityName = entity.GetProperty("EntitySetName").GetString()!,
            KeyFields = keyFields, // compuesta: típicamente dataAreaId + clave natural
            Fields = fields,
            EventEntityName = await EventEntityNameFor(entityName, ct),
        };
    }

    /// <summary>
    /// Nombre de la entidad en los data events: la entidad virtual mserp_ se llama
    /// 'mserp_' + nombre AOT de la data entity en minúsculas (CustCustomerGroupEntity →
    /// mserp_custcustomergroupentity). El nombre AOT sale de metadata/DataEntities;
    /// best-effort: sin él el diseñador degrada a entrada manual del alias.
    /// </summary>
    private async Task<string?> EventEntityNameFor(string entitySetName, CancellationToken ct)
    {
        try
        {
            using var request = ODataRequest.Get(
                $"metadata/DataEntities?$filter=PublicCollectionName%20eq%20'{entitySetName}'&$select=Name");
            using var response = await Http.SendAsync(request, ct);
            response.EnsureSuccessStatusCode();

            using var doc = await JsonDocument.ParseAsync(await response.Content.ReadAsStreamAsync(ct), cancellationToken: ct);
            var matches = doc.RootElement.GetProperty("value");
            if (matches.GetArrayLength() == 0)
            {
                return null;
            }

            var aotName = matches[0].GetProperty("Name").GetString();
            return string.IsNullOrEmpty(aotName) ? null : $"mserp_{aotName.ToLowerInvariant()}";
        }
        catch (Exception ex) when (ex is HttpRequestException or JsonException or KeyNotFoundException)
        {
            return null;
        }
    }

    public async Task<IReadOnlyList<string>> ListEntitiesAsync(CancellationToken ct)
    {
        EnsureEnvironmentConfigured();

        // Solo las entidades expuestas por OData (DataServiceEnabled): son las únicas
        // sobre las que el conector puede escribir.
        using var request = ODataRequest.Get(
            "metadata/DataEntities?$filter=DataServiceEnabled%20eq%20true&$select=PublicCollectionName");
        using var response = await Http.SendAsync(request, ct);
        response.EnsureSuccessStatusCode();

        using var doc = await JsonDocument.ParseAsync(await response.Content.ReadAsStreamAsync(ct), cancellationToken: ct);

        return [.. doc.RootElement.GetProperty("value").EnumerateArray()
            .Select(e => e.GetProperty("PublicCollectionName").GetString())
            .Where(n => !string.IsNullOrEmpty(n))
            .Select(n => n!)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(n => n, StringComparer.Ordinal)];
    }

    public async Task<IReadOnlyDictionary<string, string>> GetOptionSetAsync(string entityName, string fieldName, CancellationToken ct)
    {
        EnsureEnvironmentConfigured();
        ValidateODataName(entityName, nameof(entityName));
        ValidateODataName(fieldName, nameof(fieldName));

        var empty = new Dictionary<string, string>();

        // 1. Tipo del campo desde PublicEntities. Entidad o campo inexistente no es
        //    error acá: simplemente no hay valores que ofrecer.
        using var entityRequest = ODataRequest.Get(
            $"metadata/PublicEntities?$filter=EntitySetName%20eq%20'{entityName}'");
        using var entityResponse = await Http.SendAsync(entityRequest, ct);
        entityResponse.EnsureSuccessStatusCode();

        string? typeName = null;
        using (var doc = await JsonDocument.ParseAsync(await entityResponse.Content.ReadAsStreamAsync(ct), cancellationToken: ct))
        {
            var matches = doc.RootElement.GetProperty("value");
            if (matches.GetArrayLength() == 0)
            {
                return empty;
            }

            foreach (var property in matches[0].GetProperty("Properties").EnumerateArray())
            {
                if (string.Equals(property.GetProperty("Name").GetString(), fieldName, StringComparison.OrdinalIgnoreCase))
                {
                    typeName = property.GetProperty("TypeName").GetString();
                    break;
                }
            }
        }

        // Los primitivos son "Edm.*"; los enums llevan el namespace de DataEntities.
        if (typeName is null || typeName.StartsWith("Edm.", StringComparison.Ordinal))
        {
            return empty;
        }
        var enumName = typeName[(typeName.LastIndexOf('.') + 1)..];
        ValidateODataName(enumName, nameof(fieldName));

        // 2. Miembros del enum. OData escribe enums por nombre de miembro, así que el
        //    diccionario usa el Name tanto de clave como de etiqueta.
        using var enumRequest = ODataRequest.Get($"metadata/PublicEnumerations('{enumName}')");
        using var enumResponse = await Http.SendAsync(enumRequest, ct);
        if (enumResponse.StatusCode == HttpStatusCode.NotFound)
        {
            return empty;
        }
        enumResponse.EnsureSuccessStatusCode();

        using var enumDoc = await JsonDocument.ParseAsync(await enumResponse.Content.ReadAsStreamAsync(ct), cancellationToken: ct);
        var values = new Dictionary<string, string>();
        foreach (var member in enumDoc.RootElement.GetProperty("Members").EnumerateArray())
        {
            var name = member.GetProperty("Name").GetString();
            if (!string.IsNullOrEmpty(name))
            {
                values[name] = name;
            }
        }
        return values;
    }

    private static void ValidateODataName(string value, string paramName)
    {
        // Nombres públicos OData de F&O: PascalCase alfanumérico. Se valida antes de
        // interpolar en URLs y $filter.
        if (value.Length == 0 || !value.All(c => char.IsAsciiLetter(c) || char.IsAsciiDigit(c) || c == '_'))
        {
            throw new ArgumentException($"Nombre OData de F&O inválido: '{value}'.", paramName);
        }
    }

    public async Task<IReadOnlyList<string>> ListCompaniesAsync(CancellationToken ct)
    {
        EnsureEnvironmentConfigured();

        // LegalEntities es el maestro de empresas (dataAreaId). Son pocas filas:
        // una página de OData alcanza, sin seguir @odata.nextLink.
        using var request = ODataRequest.Get("data/LegalEntities?$select=LegalEntityId");
        using var response = await Http.SendAsync(request, ct);
        response.EnsureSuccessStatusCode();

        using var doc = await JsonDocument.ParseAsync(await response.Content.ReadAsStreamAsync(ct), cancellationToken: ct);

        return [.. doc.RootElement.GetProperty("value").EnumerateArray()
            .Select(e => e.GetProperty("LegalEntityId").GetString())
            .Where(id => !string.IsNullOrEmpty(id))
            .Select(id => id!)
            .OrderBy(id => id, StringComparer.Ordinal)];
    }

    /// <summary>
    /// Sin EnvironmentUrl el HttpClient no tiene BaseAddress y HttpClient tiraría un
    /// críptico "BaseAddress must be set"; este guard lo convierte en el error real.
    /// </summary>
    private void EnsureEnvironmentConfigured()
    {
        if (Http.BaseAddress is null)
        {
            throw new InvalidOperationException(
                "Falta FinOps:EnvironmentUrl en la configuración (appsettings/user-secrets en el portal, local.settings.json en el SyncEngine).");
        }
    }
}
