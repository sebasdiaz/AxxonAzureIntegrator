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
    /// Nombre con el que el origen publica sus data events cuando difiere del nombre
    /// OData de <see cref="SourceEntity"/>. F&O publica con el nombre de la entidad
    /// virtual: "mserp_" + nombre AOT en minúsculas (ej. "mserp_custcustomergroupentity"
    /// para CustomerGroups) — no es derivable del nombre OData, por eso es configuración
    /// del mapa. Null = los eventos llegan con el mismo nombre que SourceEntity.
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

    /// <summary>
    /// ¿Este mapa procesa los eventos de <paramref name="sourceSystem"/>/<paramref name="sourceEntity"/>?
    /// Matchea contra el nombre OData (<see cref="SourceEntity"/>, el que traen los
    /// eventos de un run agendado) o contra <see cref="SourceEventEntity"/> (el que
    /// traen los data events del bus). Predicado único para todos los stores.
    /// </summary>
    public bool MatchesSource(string sourceSystem, string sourceEntity) =>
        Status == MapStatus.Active &&
        string.Equals(SourceSystem, sourceSystem, StringComparison.OrdinalIgnoreCase) &&
        (string.Equals(SourceEntity, sourceEntity, StringComparison.OrdinalIgnoreCase) ||
         string.Equals(SourceEventEntity, sourceEntity, StringComparison.OrdinalIgnoreCase));

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

    /// <summary>
    /// Ejecución agendada del mapa (null = solo data events). Un mapa puede tener las
    /// dos cosas: los eventos lo mantienen near-real-time y el run periódico actúa de
    /// red de contención (eventos perdidos, deletes que el origen no publica).
    /// </summary>
    public MapSchedule? Schedule { get; init; }
}

public enum ScheduledRunMode
{
    /// <summary>Trae solo lo modificado desde la última watermark del mapa.</summary>
    Incremental,

    /// <summary>Re-publica todos los registros del origen en cada run (tablas de referencia chicas).</summary>
    FullExport
}

/// <summary>
/// Programación de un mapa: el run periódico hace pull del origen y publica los
/// cambios al mismo topic que los data events, así todo el pipeline (eco, conflicto,
/// xref, histórico, DLQ) aplica igual.
/// </summary>
public sealed record MapSchedule
{
    /// <summary>
    /// Expresión cron en UTC, formato NCRONTAB de 5 o 6 campos (con segundos), el
    /// mismo de los timer triggers de Functions. Ej: "0 */15 * * * *" = cada 15 min.
    /// </summary>
    public required string Cron { get; init; }

    public ScheduledRunMode Mode { get; init; } = ScheduledRunMode.Incremental;

    /// <summary>
    /// Detección de deletes por ausencia: además del pull, el run recorre las claves
    /// vivas del origen y compara contra los vínculos del xref del par — lo que el
    /// xref conoce y ya no está en el origen se publica como Delete. Necesario para
    /// orígenes que no publican tombstones por polling (OData de F&O). El barrido de
    /// claves es sobre todas las empresas (la ausencia debe ser absoluta), así que
    /// conviene reservarlo para entidades de volumen acotado.
    /// </summary>
    public bool DetectDeletes { get; init; }
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
