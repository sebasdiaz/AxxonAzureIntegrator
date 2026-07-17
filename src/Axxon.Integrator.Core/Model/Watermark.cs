namespace Axxon.Integrator.Core.Model;

/// <summary>
/// Marca de progreso para captura por polling o para el sync inicial.
/// El significado del token es propio de cada conector (timestamp, LSN de CDC,
/// delta token de Dataverse, etc.); el motor lo trata como opaco.
/// </summary>
public sealed record Watermark(string System, string EntityName, string Token, DateTimeOffset UpdatedAt)
{
    public static Watermark Start(string system, string entityName) =>
        new(system, entityName, string.Empty, DateTimeOffset.MinValue);
}

public sealed record EntityQuery
{
    public required string EntityName { get; init; }
    public IReadOnlyList<string> Companies { get; init; } = [];

    /// <summary>Campos a exportar; vacío = todos los del mapa.</summary>
    public IReadOnlyList<string> Fields { get; init; } = [];

    /// <summary>
    /// Traer solo los campos de la clave del registro (el conector sabe cuáles son).
    /// Lo usa la detección de deletes por ausencia: alcanza con enumerar identidades,
    /// sin pagar el payload completo.
    /// </summary>
    public bool KeysOnly { get; init; }
}

public sealed record EntityMetadata
{
    public required string EntityName { get; init; }

    /// <summary>
    /// Campos de la clave primaria. Lista porque en F&O es compuesta
    /// (ej. dataAreaId + CustomerAccount); en Dataverse es un único GUID.
    /// </summary>
    public required IReadOnlyList<string> KeyFields { get; init; }

    public required IReadOnlyDictionary<string, string> Fields { get; init; } // nombre -> tipo
}
