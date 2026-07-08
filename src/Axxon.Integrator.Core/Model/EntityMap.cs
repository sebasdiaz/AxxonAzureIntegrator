namespace Axxon.Integrator.Core.Model;

public enum SyncDirection
{
    OneWay,
    Bidirectional
}

public enum MapStatus
{
    Active,
    Paused
}

/// <summary>
/// Definición declarativa de una sincronización entre dos entidades. Es el equivalente
/// a las "table maps" de Dual Write: vive como documento versionado en el store de
/// configuración, no como código, para que se pueda crear/editar sin deploy.
/// </summary>
public sealed record EntityMap
{
    /// <summary>Identificador único del mapa (ej. "customers-dataverse-to-finops").</summary>
    public required string Name { get; init; }

    public required string SourceSystem { get; init; }
    public required string SourceEntity { get; init; }
    public required string TargetSystem { get; init; }
    public required string TargetEntity { get; init; }

    /// <summary>
    /// Identidad canónica del par de entidades, independiente de la dirección: los dos
    /// mapas de un bidireccional (A→B y B→A) comparten PairKey y, con él, el vínculo
    /// del xref y su estado de sync — requisito para que eco y last-writer-wins crucen
    /// sistemas.
    /// </summary>
    public string PairKey
    {
        get
        {
            var sides = new[] { $"{SourceSystem}:{SourceEntity}", $"{TargetSystem}:{TargetEntity}" };
            Array.Sort(sides, StringComparer.OrdinalIgnoreCase);
            return string.Join("|", sides).ToLowerInvariant();
        }
    }

    public SyncDirection Direction { get; init; } = SyncDirection.OneWay;
    public MapStatus Status { get; init; } = MapStatus.Active;

    /// <summary>
    /// Empresas / legal entities incluidas. Vacío = todas. Equivalente al filtro
    /// por legal entity de Dual Write.
    /// </summary>
    public IReadOnlyList<string> Companies { get; init; } = [];

    public required IReadOnlyList<FieldMap> Fields { get; init; }

    /// <summary>
    /// Campo del origen que actúa como clave natural para el primer match contra el
    /// destino cuando todavía no existe entrada en el xref (ej. "accountnumber").
    /// </summary>
    public string? NaturalKeyField { get; init; }

    /// <summary>Los mapas son versionados: los eventos en vuelo se procesan con la versión vigente al consumirse.</summary>
    public int Version { get; init; } = 1;
}

public sealed record FieldMap
{
    public required string Source { get; init; }
    public required string Target { get; init; }

    /// <summary>Nombre de una transformación registrada en el motor (ej. "trim", "toUpper", "lookupCurrency").</summary>
    public string? Transform { get; init; }

    /// <summary>Traducción de valores enumerados entre sistemas (option sets / enums de F&O).</summary>
    public IReadOnlyDictionary<string, string>? ValueMap { get; init; }

    /// <summary>Valor a usar cuando el origen no trae el campo.</summary>
    public object? DefaultValue { get; init; }
}
