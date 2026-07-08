namespace Axxon.Integrator.Core.Model;

public enum ChangeOperation
{
    Create,
    Update,
    Delete
}

/// <summary>
/// Evento de cambio normalizado. Todos los conectores traducen su formato nativo
/// (RemoteExecutionContext de F&O/Dataverse, CDC de SQL, webhook, etc.) a este contrato
/// antes de publicarlo en Service Bus.
/// </summary>
public sealed record ChangeEvent
{
    /// <summary>Identificador lógico del sistema origen (ej. "finops", "dataverse").</summary>
    public required string SourceSystem { get; init; }

    /// <summary>Nombre de la entidad en el sistema origen (ej. "CustomersV3", "account").</summary>
    public required string EntityName { get; init; }

    /// <summary>Identificador nativo del registro en el sistema origen.</summary>
    public required string SourceRecordId { get; init; }

    public required ChangeOperation Operation { get; init; }

    /// <summary>Momento del cambio en el origen. Se usa para resolución de conflictos (last-writer-wins).</summary>
    public required DateTimeOffset OccurredAt { get; init; }

    /// <summary>Empresa / legal entity, si aplica. Habilita el filtrado por compañía de los mapas.</summary>
    public string? Company { get; init; }

    /// <summary>
    /// Campos del registro tras el cambio. Ojo: los data events de F&O omiten los
    /// datetime en NULL, así que la ausencia de clave no implica ausencia de campo.
    /// </summary>
    public required IReadOnlyDictionary<string, object?> Data { get; init; }

    /// <summary>Usuario que originó el cambio en el sistema origen. Clave para la supresión de eco.</summary>
    public string? OriginatingUserId { get; init; }

    /// <summary>Correlación de punta a punta para trazabilidad en App Insights.</summary>
    public string CorrelationId { get; init; } = Guid.NewGuid().ToString("N");
}
