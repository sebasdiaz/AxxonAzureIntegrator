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
/// 1. Identidad (primaria): si el evento lo originó el usuario de integración del
///    propio motor, es eco.
/// 2. Contenido (defensa en profundidad, para orígenes que no reportan el usuario
///    iniciador): si el evento viene del sistema donde el motor escribió por última
///    vez, y la proyección del payload sobre los campos escritos coincide en hash con
///    lo escrito, no hay cambio real que propagar. Como evento y escritura comparten
///    esquema (el del sistema que reporta el cambio), la comparación es directa.
/// </summary>
public sealed class EchoGuard(IReadOnlySet<string> integrationUserIds)
{
    public bool IsEcho(ChangeEvent evt, XrefLink? link)
    {
        if (evt.OriginatingUserId is not null && integrationUserIds.Contains(evt.OriginatingUserId))
        {
            return true;
        }

        if (link?.State is not { } state ||
            !string.Equals(state.WrittenToSystem, evt.SourceSystem, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        // Clave ausente se proyecta como null: recupera el gotcha de F&O que omite
        // los datetime en NULL del payload de sus data events.
        var projection = state.WrittenFields.ToDictionary(
            f => f,
            f => evt.Data.GetValueOrDefault(f),
            StringComparer.Ordinal);

        return ComputeHash(projection) == state.WrittenPayloadHash;
    }

    /// <summary>
    /// Hash canónico de un payload: claves ordenadas para que el mismo contenido dé el
    /// mismo hash sin importar el orden de serialización. Pendiente (fase 2): normalizar
    /// la representación de valores (números, fechas) entre lo que escribe el motor y lo
    /// que el sistema reporta en sus eventos. Un falso negativo por representación no
    /// pierde datos: la defensa por identidad contiene el loop y el upsert es idempotente.
    /// </summary>
    public static string ComputeHash(IReadOnlyDictionary<string, object?> fields)
    {
        var canonical = JsonSerializer.Serialize(
            fields.OrderBy(kv => kv.Key, StringComparer.Ordinal)
                  .ToDictionary(kv => kv.Key, kv => kv.Value));
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(canonical)));
    }
}
