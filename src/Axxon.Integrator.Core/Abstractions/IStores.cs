using Axxon.Integrator.Core.Model;

namespace Axxon.Integrator.Core.Abstractions;

/// <summary>
/// Cross-reference de identidades y estado de sincronización, por vínculo lógico.
/// Dual Write se apoya en GUIDs compartidos entre F&O y Dataverse; con un tercer
/// sistema eso no existe, esta tabla es la fuente de verdad del vínculo.
///
/// El estado vive en el vínculo (par de registros), no en cada lado: así la supresión
/// de eco por contenido y el last-writer-wins funcionan entre sistemas — un evento de
/// Dataverse se compara contra lo que el motor escribió viniendo de F&O, y viceversa.
/// </summary>
public interface IXrefStore
{
    /// <summary>
    /// Devuelve el vínculo al que pertenece el registro, buscando desde cualquiera de
    /// los dos lados, o null si nunca se sincronizó.
    /// </summary>
    Task<XrefLink?> GetLinkAsync(string pairKey, string system, string recordId, CancellationToken ct);

    /// <summary>Crea o actualiza el vínculo (concurrencia optimista vía <see cref="XrefLink.ETag"/>).</summary>
    Task SaveLinkAsync(XrefLink link, CancellationToken ct);
}

/// <summary>
/// Vínculo entre los dos registros que un par de entidades mantiene sincronizados,
/// más el estado del último sync. En Cosmos se persiste como dos documentos espejo
/// (uno por lado, point read por <c>pairKey|system|recordId</c>).
/// </summary>
public sealed record XrefLink
{
    /// <summary>Identidad canónica del par de entidades (ver <see cref="EntityMap.PairKey"/>).</summary>
    public required string PairKey { get; init; }

    public required string SystemA { get; init; }
    public required string RecordIdA { get; init; }
    public required string SystemB { get; init; }
    public required string RecordIdB { get; init; }

    public SyncState? State { get; init; }

    /// <summary>ETag de Cosmos para concurrencia optimista entre las dos direcciones del sync.</summary>
    public string? ETag { get; init; }

    public string? RecordIdIn(string system) =>
        string.Equals(system, SystemA, StringComparison.OrdinalIgnoreCase) ? RecordIdA
        : string.Equals(system, SystemB, StringComparison.OrdinalIgnoreCase) ? RecordIdB
        : null;
}

/// <summary>
/// Estado del último sync del vínculo. Base de la supresión de eco por contenido y
/// del last-writer-wins entre sistemas.
/// </summary>
public sealed record SyncState
{
    /// <summary>Sistema en el que el motor escribió por última vez. Un evento que llega desde este sistema es candidato a eco.</summary>
    public required string WrittenToSystem { get; init; }

    /// <summary>Campos escritos (esquema del destino). Permiten re-proyectar el evento-eco para compararlo por hash.</summary>
    public required IReadOnlyList<string> WrittenFields { get; init; }

    /// <summary>Hash canónico del payload escrito (ver <see cref="Sync.EchoGuard.ComputeHash"/>).</summary>
    public required string WrittenPayloadHash { get; init; }

    /// <summary>Sistema donde ocurrió el cambio de negocio que ganó el último sync.</summary>
    public required string LastWriterSystem { get; init; }

    /// <summary>OccurredAt del evento ganador. Cualquier evento del vínculo más viejo que esto pierde por last-writer-wins.</summary>
    public required DateTimeOffset LastWriterOccurredAt { get; init; }
}

/// <summary>
/// Acceso a la configuración de mapas: documentos JSON planos (Blob Storage en
/// producción, archivos locales en desarrollo — el mismo documento). Un bidireccional
/// se modela como dos mapas direccionales que comparten <see cref="EntityMap.PairKey"/>.
/// El motor solo lee (detrás de un caché en memoria); el portal escribe.
/// </summary>
public interface IEntityMapStore
{
    /// <summary>Mapas activos que tienen a <paramref name="sourceSystem"/>/<paramref name="sourceEntity"/> como origen.</summary>
    Task<IReadOnlyList<EntityMap>> GetMapsForSourceAsync(string sourceSystem, string sourceEntity, CancellationToken ct);

    Task<EntityMap?> GetAsync(string name, CancellationToken ct);

    /// <summary>Todos los mapas, en cualquier estado. Para el portal de administración.</summary>
    Task<IReadOnlyList<EntityMap>> GetAllAsync(CancellationToken ct);

    /// <summary>Crea o reemplaza el documento del mapa (el diseñador incrementa <see cref="EntityMap.Version"/>).</summary>
    Task SaveAsync(EntityMap map, CancellationToken ct);
}

/// <summary>Persistencia de watermarks para polling y sync inicial.</summary>
public interface IWatermarkStore
{
    Task<Watermark?> GetAsync(string system, string entityName, CancellationToken ct);
    Task SaveAsync(Watermark watermark, CancellationToken ct);
}
