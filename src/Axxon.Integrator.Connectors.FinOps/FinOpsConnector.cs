using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Model;

namespace Axxon.Integrator.Connectors.FinOps;

/// <summary>
/// Conector de Dynamics 365 Finance &amp; Operations. Sin desarrollo X++:
/// - Captura viva: data events nativos de F&O publicando directo al topic de Service Bus
///   (configurado en Sys admin > Business events; requiere change tracking en la data
///   entity y el secret del bus en Key Vault). Este conector solo aporta el parser.
/// - Escritura: OData sobre data entities (upsert por clave de entidad).
/// - Sync inicial: DMF (paquetes de exportación), nunca data events — el ambiente
///   soporta ~5.000 eventos/5 min y ~50.000/hora.
/// </summary>
public sealed class FinOpsConnector : IConnector
{
    public string SystemName => "finops";

    public IAsyncEnumerable<ChangeEvent> PullChangesAsync(Watermark since, CancellationToken ct) =>
        throw new NotImplementedException("Catch-up por OData con change tracking. Fase 3.");

    public Task<UpsertResult> UpsertAsync(EntityPayload payload, CancellationToken ct) =>
        throw new NotImplementedException("Upsert vía OData: PATCH con clave de entidad y If-Match:*. Fase 1 (MVP).");

    public Task DeleteAsync(EntityPayload payload, CancellationToken ct) =>
        throw new NotImplementedException("DELETE vía OData. Fase 2.");

    public IAsyncEnumerable<EntityPayload> ExportAsync(EntityQuery query, CancellationToken ct) =>
        throw new NotImplementedException("Export masivo vía DMF package API. Fase 3 (sync inicial).");

    public Task<EntityMetadata> GetMetadataAsync(string entityName, CancellationToken ct) =>
        throw new NotImplementedException("Metadata vía $metadata de OData. Fase 4 (diseñador de mapas).");
}
