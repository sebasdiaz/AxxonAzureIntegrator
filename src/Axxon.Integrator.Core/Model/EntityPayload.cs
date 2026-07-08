namespace Axxon.Integrator.Core.Model;

/// <summary>
/// Registro ya transformado, listo para escribirse en el sistema destino.
/// </summary>
public sealed record EntityPayload
{
    public required string TargetSystem { get; init; }
    public required string EntityName { get; init; }

    /// <summary>ID nativo en el destino si el xref ya lo conoce; null fuerza resolución por clave natural o create.</summary>
    public string? TargetRecordId { get; init; }

    public required ChangeOperation Operation { get; init; }
    public required IReadOnlyDictionary<string, object?> Fields { get; init; }

    public string? Company { get; init; }

    /// <summary>Clave de idempotencia: el conector destino debe garantizar que aplicar dos veces el mismo payload no duplica.</summary>
    public required string IdempotencyKey { get; init; }

    public required string CorrelationId { get; init; }
}

public sealed record UpsertResult
{
    /// <summary>ID nativo del registro en el destino tras el upsert. Se persiste en el xref.</summary>
    public required string TargetRecordId { get; init; }

    public required bool Created { get; init; }
}
