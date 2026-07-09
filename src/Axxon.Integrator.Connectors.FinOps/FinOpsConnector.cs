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

    public Task<UpsertResult> UpsertAsync(EntityPayload payload, CancellationToken ct) =>
        throw new NotImplementedException("Upsert vía OData: PATCH con clave de entidad y If-Match:*. Fase 1 (MVP).");

    public Task DeleteAsync(EntityPayload payload, CancellationToken ct) =>
        throw new NotImplementedException("DELETE vía OData. Fase 2.");

    public IAsyncEnumerable<EntityPayload> ExportAsync(EntityQuery query, CancellationToken ct) =>
        throw new NotImplementedException("Export masivo vía DMF package API. Fase 3 (sync inicial).");

    public async Task<EntityMetadata> GetMetadataAsync(string entityName, CancellationToken ct)
    {
        EnsureEnvironmentConfigured();

        // Nombres públicos OData de F&O: PascalCase alfanumérico. Se valida antes de
        // interpolar en el $filter.
        if (entityName.Length == 0 || !entityName.All(c => char.IsAsciiLetter(c) || char.IsAsciiDigit(c) || c == '_'))
        {
            throw new ArgumentException($"Nombre de entidad OData de F&O inválido: '{entityName}'.", nameof(entityName));
        }

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
