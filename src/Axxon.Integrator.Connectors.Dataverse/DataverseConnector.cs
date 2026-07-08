using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Model;

namespace Axxon.Integrator.Connectors.Dataverse;

/// <summary>
/// Conector de Dataverse.
/// - Captura viva: service endpoints nativos de Dataverse (steps registrados sobre
///   Create/Update/Delete que publican al topic de Service Bus). Este conector solo
///   aporta el parser; el registro del step es configuración, no código.
/// - Escritura: Web API (upsert por GUID o clave alternativa) con un application user
///   dedicado — su ID alimenta la supresión de eco.
/// - Sync inicial y catch-up: Web API con change tracking (delta tokens).
/// </summary>
public sealed class DataverseConnector : IConnector
{
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
