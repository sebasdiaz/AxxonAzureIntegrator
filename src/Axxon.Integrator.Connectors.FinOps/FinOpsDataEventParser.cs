using System.Text.Json;
using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Model;

namespace Axxon.Integrator.Connectors.FinOps;

/// <summary>
/// Traduce el payload de un data event de F&O al <see cref="ChangeEvent"/> normalizado.
/// El payload sigue la forma de RemoteExecutionContext del SDK de Dataverse: la operación
/// viene en MessageName (Create/Update/Delete) y los campos en InputParameters.Target,
/// con PreImage disponible para updates.
/// Gotcha documentado: los datetime en NULL se omiten del payload.
/// </summary>
public sealed class FinOpsDataEventParser : IChangeEventParser
{
    public string SystemName => "finops";

    public ChangeEvent Parse(BinaryData messageBody)
    {
        using var doc = JsonDocument.Parse(messageBody);
        var root = doc.RootElement;

        var messageName = root.GetProperty("MessageName").GetString()
            ?? throw new FormatException("Data event sin MessageName.");

        var operation = messageName switch
        {
            "Create" => ChangeOperation.Create,
            "Update" => ChangeOperation.Update,
            "Delete" => ChangeOperation.Delete,
            _ => throw new FormatException($"MessageName no soportado: '{messageName}'."),
        };

        // TODO(MVP): extraer Target de InputParameters (par KeyValuePair "Target" con
        // Attributes), PrimaryEntityId, empresa (dataAreaId) y usuario iniciador.
        throw new NotImplementedException(
            $"Parseo de InputParameters pendiente (operación detectada: {operation}). Fase 1 (MVP).");
    }
}
