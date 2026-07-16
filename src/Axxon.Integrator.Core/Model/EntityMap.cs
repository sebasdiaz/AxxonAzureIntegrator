using System.Text.Json.Serialization;

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
    /// Nombre con el que la entidad origen llega en los eventos, cuando difiere del
    /// nombre canónico de la API. Caso F&O: la API OData usa el public collection name
    /// (<c>CustomerGroups</c>) pero los data events emiten la entidad virtual mserp_
    /// (<c>mserp_custcustomergroupentity</c>). El ruteo de eventos matchea contra ambos;
    /// el <see cref="PairKey"/> usa solo <see cref="SourceEntity"/> para que el vínculo
    /// del xref no dependa del alias. Lo completa el diseñador desde la metadata; null
    /// cuando el nombre de la API y el de los eventos coinciden (Dataverse).
    /// </summary>
    public string? SourceEventEntity { get; init; }

    /// <summary>
    /// Identidad canónica del par de entidades, independiente de la dirección: los dos
    /// mapas de un bidireccional (A→B y B→A) comparten PairKey y, con él, el vínculo
    /// del xref y su estado de sync — requisito para que eco y last-writer-wins crucen
    /// sistemas.
    /// </summary>
    [JsonIgnore] // derivada; no se persiste en el documento JSON del mapa
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
    /// Integration key (como en Dual Write): campos del destino, posiblemente
    /// compuesta (ej. ["accountnumber"] o ["dataAreaId", "CustomerAccount"]), que
    /// identifican el registro para el primer match cuando todavía no existe vínculo
    /// en el xref. Deben estar entre los campos mapeados.
    /// </summary>
    public IReadOnlyList<string> IntegrationKey { get; init; } = [];

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
