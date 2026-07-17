using Axxon.Integrator.Core.Model;

namespace Axxon.Integrator.Core.Abstractions;

/// <summary>
/// Publicación de un <see cref="ChangeEvent"/> al flujo normalizado (el topic 'changes'
/// en producción). Los mapas agendados publican por acá lo que el pull trae del origen,
/// de modo que el pipeline los procese exactamente igual que a un data event: mismas
/// sesiones por registro, misma dedupe, mismo eco/conflicto/histórico/DLQ.
/// </summary>
public interface IChangeEventPublisher
{
    Task PublishAsync(ChangeEvent evt, CancellationToken ct);
}
