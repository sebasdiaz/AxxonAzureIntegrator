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

    public Task<EntityMetadata> GetMetadataAsync(string entityName, CancellationToken ct) =>
        throw new NotImplementedException("EntityDefinitions de la Web API. Fase 4 (diseñador de mapas).");
}
