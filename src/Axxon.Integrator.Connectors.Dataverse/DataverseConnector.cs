using System.Net;
using System.Net.Http.Headers;
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

    public Task<UpsertResult> UpsertAsync(EntityPayload payload, CancellationToken ct) =>
        throw new NotImplementedException("Upsert vía Web API (PATCH con clave alternativa). Fase 1 (MVP).");

    public Task DeleteAsync(EntityPayload payload, CancellationToken ct) =>
        throw new NotImplementedException("DELETE vía Web API. Fase 2.");

    public IAsyncEnumerable<EntityPayload> ExportAsync(EntityQuery query, CancellationToken ct) =>
        throw new NotImplementedException("Export paginado vía Web API. Fase 3 (sync inicial).");

    public async Task<EntityMetadata> GetMetadataAsync(string entityName, CancellationToken ct)
    {
        // Logical names de Dataverse: minúsculas ASCII, dígitos y guión bajo. Se valida
        // antes de interpolar en la URL.
        if (entityName.Length == 0 || !entityName.All(c => char.IsAsciiLetterLower(c) || char.IsAsciiDigit(c) || c == '_'))
        {
            throw new ArgumentException($"Logical name de Dataverse inválido: '{entityName}'.", nameof(entityName));
        }

        // Una sola llamada: definición + atributos expandidos. Las consultas de
        // metadata no paginan — la primera respuesta trae todo.
        var url = $"api/data/v9.2/EntityDefinitions(LogicalName='{entityName}')" +
                  "?$select=LogicalName,PrimaryIdAttribute" +
                  "&$expand=Attributes($select=LogicalName,AttributeType,AttributeOf)";

        using var request = new HttpRequestMessage(HttpMethod.Get, url);
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        request.Headers.Add("OData-MaxVersion", "4.0");
        request.Headers.Add("OData-Version", "4.0");

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
            PrimaryKeyField = root.GetProperty("PrimaryIdAttribute").GetString()!,
            Fields = fields,
        };
    }
}
