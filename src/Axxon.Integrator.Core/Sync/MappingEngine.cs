using Axxon.Integrator.Core.Model;

namespace Axxon.Integrator.Core.Sync;

/// <summary>
/// Aplica un <see cref="EntityMap"/> sobre un <see cref="ChangeEvent"/> y produce el
/// payload para el destino. Las transformaciones con nombre se registran acá; un mapa
/// referencia transformaciones por nombre, nunca código.
/// </summary>
public sealed class MappingEngine
{
    private readonly Dictionary<string, Func<object?, object?>> _transforms = new(StringComparer.OrdinalIgnoreCase)
    {
        ["trim"] = v => (v as string)?.Trim() ?? v,
        ["toUpper"] = v => (v as string)?.ToUpperInvariant() ?? v,
        ["toLower"] = v => (v as string)?.ToLowerInvariant() ?? v,
    };

    public void RegisterTransform(string name, Func<object?, object?> transform) =>
        _transforms[name] = transform;

    public EntityPayload Apply(EntityMap map, ChangeEvent evt, string? targetRecordId)
    {
        var fields = new Dictionary<string, object?>(map.Fields.Count);

        foreach (var fieldMap in map.Fields)
        {
            // Ausencia de clave != campo nulo: los data events de F&O omiten datetimes
            // en NULL, así que un campo ausente sin DefaultValue no se escribe (evita
            // pisar el valor del destino con un null fantasma).
            if (!evt.Data.TryGetValue(fieldMap.Source, out var value))
            {
                if (fieldMap.DefaultValue is not null)
                {
                    fields[fieldMap.Target] = fieldMap.DefaultValue;
                }
                continue;
            }

            if (fieldMap.ValueMap is not null && value is not null)
            {
                if (fieldMap.ValueMap.TryGetValue(value.ToString()!, out var mapped))
                {
                    value = mapped;
                }
                else if (fieldMap.DefaultValue is not null)
                {
                    value = fieldMap.DefaultValue;
                }
                else
                {
                    // Error permanente (config incompleta): dejar pasar el valor crudo
                    // escribiría un option set inválido en el destino. Debe ir a DLQ,
                    // no reintentarse.
                    throw new InvalidOperationException(
                        $"El mapa '{map.Name}' no tiene traducción en el ValueMap de '{fieldMap.Source}' para el valor '{value}' y no define DefaultValue.");
                }
            }

            if (fieldMap.Transform is not null)
            {
                if (!_transforms.TryGetValue(fieldMap.Transform, out var transform))
                {
                    throw new InvalidOperationException(
                        $"El mapa '{map.Name}' referencia la transformación '{fieldMap.Transform}', que no está registrada.");
                }
                value = transform(value);
            }

            fields[fieldMap.Target] = value;
        }

        return new EntityPayload
        {
            TargetSystem = map.TargetSystem,
            EntityName = map.TargetEntity,
            TargetRecordId = targetRecordId,
            Operation = evt.Operation,
            Fields = fields,
            Company = evt.Company,
            IdempotencyKey = $"{map.Name}:{evt.SourceRecordId}:{evt.OccurredAt.UtcTicks}",
            CorrelationId = evt.CorrelationId,
        };
    }
}
