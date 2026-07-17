using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Model;

namespace Axxon.Integrator.Connectors.FinOps;

/// <summary>
/// Traduce el payload de un data event de F&O al <see cref="ChangeEvent"/> normalizado.
/// El payload sigue la forma de RemoteExecutionContext del SDK de Dataverse: la operación
/// viene en MessageName (Create/Update/Delete) y los campos en InputParameters.Target
/// (pares key/value con Attributes), con PreImage disponible para updates. El pipeline de
/// business events le agrega el envelope (BusinessEventId, EventTime, ControlNumber).
/// Gotcha documentado: los datetime en NULL se omiten del payload.
/// </summary>
public sealed partial class FinOpsDataEventParser : IChangeEventParser
{
    public string SystemName => "finops";

    /// <summary>
    /// Señal fuerte: el envelope de business events (BusinessEventId) que F&O agrega al
    /// payload. Señal débil: la forma RemoteExecutionContext pelada — Dataverse comparte
    /// esa forma, así que cuando exista su parser (fase 2) la señal débil deja de
    /// alcanzar; el spike con payloads reales define si la fuerte basta sola.
    /// </summary>
    public bool CanParse(BinaryData messageBody)
    {
        try
        {
            using var doc = JsonDocument.Parse(messageBody);
            var root = doc.RootElement;
            return root.ValueKind == JsonValueKind.Object &&
                (TryGetPropertyIgnoreCase(root, "BusinessEventId", out _) ||
                 (TryGetPropertyIgnoreCase(root, "MessageName", out _) &&
                  TryGetPropertyIgnoreCase(root, "InputParameters", out _)));
        }
        catch (JsonException)
        {
            return false;
        }
    }

    public ChangeEvent Parse(BinaryData messageBody)
    {
        using var doc = JsonDocument.Parse(messageBody);
        var root = doc.RootElement;

        var messageName = GetString(root, "MessageName")
            ?? throw new FormatException("Data event sin MessageName.");

        // Los data events reales llegan con los MessageName "OnExternal*" del pipeline
        // de business events; la forma "Create/Update/Delete" pelada del contrato Xrm
        // se acepta también por si algún origen la usa.
        var operation = messageName switch
        {
            "Create" or "OnExternalCreated" => ChangeOperation.Create,
            "Update" or "OnExternalUpdated" => ChangeOperation.Update,
            "Delete" or "OnExternalDeleted" => ChangeOperation.Delete,
            _ => throw new FormatException($"MessageName no soportado: '{messageName}'."),
        };

        var target = FindInputParameter(root, "Target")
            ?? throw new FormatException("Data event sin InputParameters.Target.");

        var data = ReadAttributes(target);

        var entityName = GetString(root, "PrimaryEntityName") ?? GetString(target, "LogicalName")
            ?? throw new FormatException("Data event sin PrimaryEntityName ni Target.LogicalName.");

        var recordId = FirstUsefulId(GetString(root, "PrimaryEntityId"), GetString(target, "Id"))
            ?? throw new FormatException("Data event sin PrimaryEntityId ni Target.Id.");

        // Sin momento del cambio no hay last-writer-wins: mejor DLQ que inventar un
        // reloj de llegada que resuelva conflictos al revés.
        var occurredAt = GetDate(root, "EventTimeIso8601") ?? GetDate(root, "EventTime") ?? GetDate(root, "OperationCreatedOn")
            ?? throw new FormatException("Data event sin EventTime ni OperationCreatedOn.");

        return new ChangeEvent
        {
            SourceSystem = SystemName,
            EntityName = entityName,
            SourceRecordId = recordId,
            Operation = operation,
            OccurredAt = occurredAt,
            // La empresa viene como "dataAreaId" en el contrato documentado; en los data
            // events reales llega "mserp_dataareaid", que StripVirtualPrefix ya redujo
            // a "dataareaid".
            Company = data.FirstOrDefault(kv =>
                string.Equals(kv.Key, "dataAreaId", StringComparison.OrdinalIgnoreCase)).Value as string,
            Data = data,
            OriginatingUserId = FirstUsefulId(
                GetString(root, "InitiatingUserAzureActiveDirectoryObjectId"),
                GetString(root, "InitiatingUserAADObjectId"),
                GetString(root, "UserId")),
            CorrelationId = FirstUsefulId(GetString(root, "CorrelationId")) ?? Guid.NewGuid().ToString("N"),
        };
    }

    private static Dictionary<string, object?> ReadAttributes(JsonElement target)
    {
        var data = new Dictionary<string, object?>();
        if (target.ValueKind != JsonValueKind.Object ||
            !TryGetPropertyIgnoreCase(target, "Attributes", out var attributes) ||
            attributes.ValueKind != JsonValueKind.Array)
        {
            return data;
        }

        foreach (var pair in attributes.EnumerateArray())
        {
            if (pair.ValueKind == JsonValueKind.Object &&
                TryGetPropertyIgnoreCase(pair, "key", out var key) &&
                key.ValueKind == JsonValueKind.String &&
                TryGetPropertyIgnoreCase(pair, "value", out var value))
            {
                data[StripVirtualPrefix(key.GetString()!)] = ConvertValue(value);
            }
        }
        return data;
    }

    /// <summary>
    /// Los data events llegan con los nombres de la entidad virtual: "mserp_" + nombre
    /// público en minúsculas ("mserp_customergroupid" para CustomerGroupId). Los mapas
    /// usan los nombres públicos OData (los mismos del pull y del diseñador), así que
    /// el prefijo se quita acá y el MappingEngine resuelve la diferencia de mayúsculas.
    /// El nombre de entidad NO se traduce (no es derivable): eso lo cubre
    /// EntityMap.SourceEventEntity.
    /// </summary>
    private static string StripVirtualPrefix(string name) =>
        name.StartsWith("mserp_", StringComparison.OrdinalIgnoreCase) ? name["mserp_".Length..] : name;

    /// <summary>
    /// Aplana los valores del contrato Xrm a primitivos .NET: los wrappers
    /// (OptionSetValue / Money / BooleanManagedProperty) se reducen a su Value, los
    /// EntityReference a su Id, y los "/Date(ms)/" de DataContract a DateTimeOffset.
    /// Lo no reconocido viaja como JSON crudo — el mapa decide qué hacer con él.
    /// </summary>
    private static object? ConvertValue(JsonElement value) => value.ValueKind switch
    {
        JsonValueKind.Null or JsonValueKind.Undefined => null,
        JsonValueKind.String => ConvertString(value.GetString()!),
        JsonValueKind.Number => value.TryGetInt64(out var integer) ? integer
            : value.TryGetDecimal(out var dec) ? dec
            : value.GetDouble(),
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        JsonValueKind.Object when TryGetPropertyIgnoreCase(value, "Value", out var wrapped) => ConvertValue(wrapped),
        JsonValueKind.Object when TryGetPropertyIgnoreCase(value, "Id", out var reference) => ConvertValue(reference),
        _ => value.Clone(),
    };

    private static object? ConvertString(string text)
    {
        var match = DataContractDateRegex().Match(text);
        return match.Success
            ? DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture))
            : text;
    }

    private static JsonElement? FindInputParameter(JsonElement root, string parameterKey)
    {
        if (!TryGetPropertyIgnoreCase(root, "InputParameters", out var parameters) ||
            parameters.ValueKind != JsonValueKind.Array)
        {
            return null;
        }

        foreach (var pair in parameters.EnumerateArray())
        {
            if (pair.ValueKind == JsonValueKind.Object &&
                TryGetPropertyIgnoreCase(pair, "key", out var key) &&
                key.ValueKind == JsonValueKind.String &&
                string.Equals(key.GetString(), parameterKey, StringComparison.OrdinalIgnoreCase) &&
                TryGetPropertyIgnoreCase(pair, "value", out var value))
            {
                return value;
            }
        }
        return null;
    }

    private static DateTimeOffset? GetDate(JsonElement element, string name)
    {
        if (!TryGetPropertyIgnoreCase(element, name, out var value) || value.ValueKind != JsonValueKind.String)
        {
            return null;
        }

        return ConvertString(value.GetString()!) switch
        {
            DateTimeOffset date => date,
            string text when DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var parsed) => parsed,
            _ => null,
        };
    }

    private static string? GetString(JsonElement element, string name) =>
        TryGetPropertyIgnoreCase(element, name, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : null;

    /// <summary>Primer candidato con contenido real; los GUID vacíos que el contrato serializa por default no cuentan.</summary>
    private static string? FirstUsefulId(params string?[] candidates) =>
        candidates.FirstOrDefault(c =>
            !string.IsNullOrWhiteSpace(c) &&
            !(Guid.TryParse(c, out var guid) && guid == Guid.Empty));

    /// <summary>Los pares key/value y las propiedades del contexto llegan con casing variable según el serializador del origen.</summary>
    private static bool TryGetPropertyIgnoreCase(JsonElement element, string name, out JsonElement value)
    {
        if (element.ValueKind == JsonValueKind.Object)
        {
            foreach (var property in element.EnumerateObject())
            {
                if (string.Equals(property.Name, name, StringComparison.OrdinalIgnoreCase))
                {
                    value = property.Value;
                    return true;
                }
            }
        }
        value = default;
        return false;
    }

    [GeneratedRegex(@"^/Date\((-?\d+)(?:[+-]\d{4})?\)/$")]
    private static partial Regex DataContractDateRegex();
}
