using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Model;

namespace Axxon.Integrator.Core.Sync;

/// <summary>
/// Supresión de eco para sync bidireccional. Sin esto, cada escritura del integrador
/// dispara un evento de cambio en el destino que vuelve a propagarse al origen, en loop.
/// Dos defensas, en orden:
/// 1. Identidad: si el evento lo originó el usuario de integración del propio motor, es eco.
/// 2. Contenido: si el hash del payload coincide con el último estado sincronizado,
///    no hay cambio real que propagar.
/// </summary>
public sealed class EchoGuard(IXrefStore xrefStore, IReadOnlySet<string> integrationUserIds)
{
    public async Task<bool> IsEchoAsync(EntityMap map, ChangeEvent evt, CancellationToken ct)
    {
        if (evt.OriginatingUserId is not null && integrationUserIds.Contains(evt.OriginatingUserId))
        {
            return true;
        }

        var state = await xrefStore.GetSyncStateAsync(map.Name, evt.SourceSystem, evt.SourceRecordId, ct);
        return state is not null && state.PayloadHash == ComputeHash(evt.Data);
    }

    /// <summary>
    /// Hash canónico del payload: claves ordenadas para que el mismo contenido dé el
    /// mismo hash sin importar el orden de serialización del origen.
    /// </summary>
    public static string ComputeHash(IReadOnlyDictionary<string, object?> data)
    {
        var canonical = JsonSerializer.Serialize(
            data.OrderBy(kv => kv.Key, StringComparer.Ordinal)
                .ToDictionary(kv => kv.Key, kv => kv.Value));
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(canonical)));
    }
}
