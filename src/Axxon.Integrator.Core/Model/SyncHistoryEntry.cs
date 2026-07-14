namespace Axxon.Integrator.Core.Model;

/// <summary>
/// Desenlace de un evento procesado por el pipeline contra un mapa. Uno por intento:
/// un evento que falla y se reintenta deja varias filas Failed (con su DeliveryCount)
/// y, si al final entra, una fila Created/Updated — el histórico es la línea de
/// tiempo real, no el resumen.
/// </summary>
public enum SyncOutcome
{
    Created,
    Updated,
    Deleted,
    EchoSuppressed,
    DiscardedByLastWriterWins,
    Failed
}

/// <summary>
/// Renglón del histórico de sincronización de un mapa. Lo escribe el pipeline
/// (best-effort: un fallo al registrar nunca tumba la sync) y lo consume la pestaña
/// Histórico del portal. El CorrelationId enlaza con la telemetría de App Insights
/// para el detalle completo.
/// </summary>
public sealed record SyncHistoryEntry
{
    /// <summary>Mapa contra el que se procesó el evento; clave de partición del histórico.</summary>
    public required string MapName { get; init; }

    /// <summary>Momento en que el pipeline terminó de procesar el intento (UTC).</summary>
    public required DateTimeOffset ProcessedAt { get; init; }

    public required SyncOutcome Outcome { get; init; }

    public required ChangeOperation Operation { get; init; }

    public required string SourceSystem { get; init; }
    public required string SourceRecordId { get; init; }
    public required string TargetSystem { get; init; }

    /// <summary>Registro escrito/borrado en el destino; null si el intento no llegó a resolverlo.</summary>
    public string? TargetRecordId { get; init; }

    public string? Company { get; init; }

    /// <summary>OccurredAt del evento origen (el momento del cambio de negocio).</summary>
    public DateTimeOffset OccurredAt { get; init; }

    /// <summary>Mensaje de error del intento fallido; null en los desenlaces exitosos.</summary>
    public string? Error { get; init; }

    /// <summary>Número de entrega de Service Bus del intento (1 = primer intento).</summary>
    public int DeliveryCount { get; init; } = 1;

    public required string CorrelationId { get; init; }
}
