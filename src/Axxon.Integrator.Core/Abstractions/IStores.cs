using Axxon.Integrator.Core.Model;

namespace Axxon.Integrator.Core.Abstractions;

/// <summary>
/// Cross-reference de identidades: relaciona el ID nativo de un registro en cada
/// sistema. Dual Write se apoya en GUIDs compartidos entre F&O y Dataverse; con un
/// tercer sistema eso no existe, esta tabla es la fuente de verdad del vínculo.
/// </summary>
public interface IXrefStore
{
    /// <summary>Devuelve el ID del registro en <paramref name="targetSystem"/> vinculado al registro origen, o null si nunca se sincronizó.</summary>
    Task<string?> ResolveAsync(string sourceSystem, string entityMapName, string sourceRecordId, string targetSystem, CancellationToken ct);

    Task LinkAsync(string entityMapName, string systemA, string recordIdA, string systemB, string recordIdB, CancellationToken ct);

    /// <summary>Último estado sincronizado (hash del payload + timestamp). Base de la supresión de eco y del last-writer-wins.</summary>
    Task<SyncState?> GetSyncStateAsync(string entityMapName, string sourceSystem, string sourceRecordId, CancellationToken ct);

    Task SetSyncStateAsync(string entityMapName, string sourceSystem, string sourceRecordId, SyncState state, CancellationToken ct);
}

public sealed record SyncState(string PayloadHash, DateTimeOffset LastSyncedAt, string LastWriterSystem);

/// <summary>Acceso a la configuración de mapas (Cosmos DB en producción, JSON local en desarrollo).</summary>
public interface IEntityMapStore
{
    /// <summary>Mapas activos que tienen a <paramref name="sourceSystem"/>/<paramref name="sourceEntity"/> como origen (o como lado de un bidireccional).</summary>
    Task<IReadOnlyList<EntityMap>> GetMapsForSourceAsync(string sourceSystem, string sourceEntity, CancellationToken ct);

    Task<EntityMap?> GetAsync(string name, CancellationToken ct);
}

/// <summary>Persistencia de watermarks para polling y sync inicial.</summary>
public interface IWatermarkStore
{
    Task<Watermark?> GetAsync(string system, string entityName, CancellationToken ct);
    Task SaveAsync(Watermark watermark, CancellationToken ct);
}
