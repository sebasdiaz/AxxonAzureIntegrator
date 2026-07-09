using Axxon.Integrator.Core.Model;

namespace Axxon.Integrator.Core.Abstractions;

/// <summary>
/// Contrato que implementa cada sistema conectado. El motor de sincronización solo
/// conoce esta interfaz; F&O y Dataverse son los dos primeros conectores, cualquier
/// sistema futuro (SQL, REST, SaaS) se suma implementándola.
///
/// Nota sobre la captura push: los sistemas que publican eventos por sí mismos
/// (data events de F&O, service endpoints de Dataverse) no usan <see cref="PullChangesAsync"/>
/// para el flujo vivo — sus eventos llegan directo a Service Bus y el conector solo
/// aporta el parser (ver IChangeEventParser). PullChangesAsync queda para catch-up
/// y para sistemas sin push.
/// </summary>
public interface IConnector
{
    /// <summary>Identificador lógico del sistema (ej. "finops", "dataverse").</summary>
    string SystemName { get; }

    /// <summary>Captura incremental por polling desde una watermark (catch-up o sistemas sin eventos push).</summary>
    IAsyncEnumerable<ChangeEvent> PullChangesAsync(Watermark since, CancellationToken ct);

    /// <summary>
    /// Escritura idempotente en el destino. Debe comportarse como upsert: si
    /// <see cref="EntityPayload.TargetRecordId"/> viene null, resolver el registro
    /// existente por <see cref="EntityPayload.IntegrationKey"/> antes de crear.
    /// </summary>
    Task<UpsertResult> UpsertAsync(EntityPayload payload, CancellationToken ct);

    Task DeleteAsync(EntityPayload payload, CancellationToken ct);

    /// <summary>Exportación masiva para el sync inicial (en F&O: DMF, nunca data events).</summary>
    IAsyncEnumerable<EntityPayload> ExportAsync(EntityQuery query, CancellationToken ct);

    Task<EntityMetadata> GetMetadataAsync(string entityName, CancellationToken ct);

    /// <summary>
    /// Entidades disponibles para mapear (logical names / nombres públicos), para los
    /// combos del diseñador de mapas.
    /// </summary>
    Task<IReadOnlyList<string>> ListEntitiesAsync(CancellationToken ct);

    /// <summary>
    /// Empresas / legal entities del sistema, para el checklist del filtro por empresa
    /// de los mapas. Vacío si el sistema no tiene noción de empresa.
    /// </summary>
    Task<IReadOnlyList<string>> ListCompaniesAsync(CancellationToken ct);

    /// <summary>
    /// Valores del option set / enum de un campo (valor → etiqueta), para pre-poblar
    /// los diccionarios de traducción del diseñador. Vacío si el campo no es
    /// enumerado o no existe.
    /// </summary>
    Task<IReadOnlyDictionary<string, string>> GetOptionSetAsync(string entityName, string fieldName, CancellationToken ct);
}

/// <summary>
/// Traduce el payload nativo que el sistema dejó en Service Bus a un <see cref="ChangeEvent"/>.
/// F&O y Dataverse comparten la forma RemoteExecutionContext (Target/PreImage en
/// InputParameters), así que sus parsers comparten la mayor parte del código.
/// </summary>
public interface IChangeEventParser
{
    string SystemName { get; }
    ChangeEvent Parse(BinaryData messageBody);
}
