using System.Globalization;
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

    /// <summary>Campo estándar de las data entities por el que filtra el pull incremental.</summary>
    private const string ModifiedField = "ModifiedDateTime";

    /// <summary>
    /// Pull por OData para los mapas agendados: F&O no expone change tracking
    /// incremental por OData, así que el incremental filtra por
    /// <see cref="ModifiedField"/> &gt;= watermark (token = ISO 8601 del máximo visto).
    /// Con token vacío es un barrido completo. Los deletes no se ven por acá — los
    /// cubre la detección por ausencia (<see cref="Core.Model.MapSchedule.DetectDeletes"/>).
    /// </summary>
    public async IAsyncEnumerable<ChangeEvent> PullChangesAsync(EntityQuery query, Watermark since,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        EnsureEnvironmentConfigured();
        ValidateODataName(query.EntityName, nameof(query));

        // La clave OData (compuesta) identifica cada registro pulleado: es el mismo id
        // canónico que persiste el xref cuando F&O es destino.
        var metadata = await GetMetadataAsync(query.EntityName, ct);
        if (metadata.KeyFields.Count == 0)
        {
            throw new InvalidOperationException(
                $"La entidad {query.EntityName} no declara campos clave en su metadata: imposible identificar los registros del pull.");
        }

        var filters = new List<string>();
        if (!string.IsNullOrEmpty(since.Token))
        {
            if (!metadata.Fields.ContainsKey(ModifiedField))
            {
                throw new InvalidOperationException(
                    $"La entidad {query.EntityName} no expone {ModifiedField}: el pull incremental no tiene por dónde filtrar. Usar modo FullExport.");
            }
            if (!DateTimeOffset.TryParse(since.Token, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var sinceTime))
            {
                throw new InvalidOperationException(
                    $"Watermark ilegible para {query.EntityName}: '{since.Token}' no es una fecha ISO 8601.");
            }
            // 'ge' inclusivo: el borde se re-trae en el run siguiente y lo absorben la
            // dedupe del topic y el upsert idempotente — mejor repetir que perder.
            filters.Add($"{ModifiedField} ge {sinceTime.UtcDateTime:O}");
        }
        if (query.Companies.Count > 0)
        {
            filters.Add("(" + string.Join(" or ",
                query.Companies.Select(c => $"dataAreaId eq {ODataLiteral.Format(c)}")) + ")");
        }

        // cross-company siempre: sin él OData devuelve solo la empresa default del
        // usuario de integración y el pull "no vería" el resto de las legal entities.
        var options = new List<string> { "cross-company=true" };
        if (filters.Count > 0)
        {
            options.Add("$filter=" + Uri.EscapeDataString(string.Join(" and ", filters)));
        }
        if (query.KeysOnly)
        {
            foreach (var key in metadata.KeyFields)
            {
                ValidateODataName(key, nameof(query));
            }
            options.Add("$select=" + string.Join(",", metadata.KeyFields));
        }

        var url = $"data/{query.EntityName}?{string.Join("&", options)}";
        while (url is not null)
        {
            using var request = ODataRequest.Get(url);
            using var response = await Http.SendAsync(request, ct);
            await ODataRequest.EnsureSuccessAsync(response, ct);

            using var doc = await JsonDocument.ParseAsync(await response.Content.ReadAsStreamAsync(ct), cancellationToken: ct);
            foreach (var record in doc.RootElement.GetProperty("value").EnumerateArray())
            {
                yield return ToChangeEvent(query.EntityName, metadata, record);
            }

            // Paginación server-driven: el nextLink viene absoluto y se sigue tal cual.
            url = doc.RootElement.TryGetProperty("@odata.nextLink", out var next) && next.ValueKind == JsonValueKind.String
                ? next.GetString()
                : null;
        }
    }

    private static ChangeEvent ToChangeEvent(string entityName, EntityMetadata metadata, JsonElement record)
    {
        var data = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var property in record.EnumerateObject())
        {
            if (property.Name.StartsWith('@'))
            {
                continue; // anotaciones @odata.*
            }
            data[property.Name] = ConvertODataValue(property.Value);
        }

        var occurredAt = data.TryGetValue(ModifiedField, out var modified) &&
            modified is string text &&
            DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var parsed)
                ? parsed
                // sin ModifiedDateTime (keys-only o entidad sin el campo) el momento del
                // cambio es desconocido: el "ahora" del pull ordena el last-writer-wins
                : DateTimeOffset.UtcNow;

        return new ChangeEvent
        {
            SourceSystem = "finops",
            EntityName = entityName,
            SourceRecordId = KeySegment(metadata.KeyFields.Select(k => new KeyValuePair<string, object?>(
                k,
                data.TryGetValue(k, out var value) && value is not null
                    ? value
                    : throw new InvalidOperationException($"El registro pulleado de {entityName} no trae valor para la clave '{k}'.")))),
            Operation = ChangeOperation.Update, // create vs update lo resuelve el upsert del destino
            OccurredAt = occurredAt,
            Company = data.TryGetValue("dataAreaId", out var company) ? company as string : null,
            Data = data,
        };
    }

    /// <summary>Primitivos .NET desde el JSON de OData; las fechas viajan como string ISO, que es lo que los destinos aceptan.</summary>
    private static object? ConvertODataValue(JsonElement value) => value.ValueKind switch
    {
        JsonValueKind.Null or JsonValueKind.Undefined => null,
        JsonValueKind.String => value.GetString(),
        JsonValueKind.Number => value.TryGetInt64(out var integer) ? integer
            : value.TryGetDecimal(out var dec) ? dec
            : value.GetDouble(),
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        _ => value.Clone(),
    };

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

    /// <summary>Segmento de clave OData: CustomerAccount='C001',dataAreaId='usmf'.</summary>
    private static string BuildKeySegment(EntityPayload payload)
    {
        if (payload.IntegrationKey.Count == 0)
        {
            throw new InvalidOperationException(
                $"Sin vínculo en el xref y sin integration key en el mapa para {payload.EntityName} ({payload.CorrelationId}): imposible dirigir el upsert OData.");
        }

        foreach (var key in payload.IntegrationKey)
        {
            ValidateODataName(key, nameof(payload));
        }
        return KeySegment(payload.IntegrationKey.Select(k =>
            new KeyValuePair<string, object?>(k, KeyValueFor(payload, k))));
    }

    /// <summary>
    /// Segmento de clave canónico: partes ordenadas por nombre de campo, para que el
    /// mismo registro produzca el mismo id venga del upsert (orden de la integration
    /// key del mapa) o del poll agendado (orden de la metadata) — el xref y la
    /// detección de deletes por ausencia comparan estos ids como strings. OData acepta
    /// las partes de una clave compuesta en cualquier orden.
    /// </summary>
    private static string KeySegment(IEnumerable<KeyValuePair<string, object?>> keys) =>
        string.Join(",", keys
            .OrderBy(k => k.Key, StringComparer.OrdinalIgnoreCase)
            .Select(k => $"{k.Key}={ODataLiteral.Format(k.Value)}"));

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
        };
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
