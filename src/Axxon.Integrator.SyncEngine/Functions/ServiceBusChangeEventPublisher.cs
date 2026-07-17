using Azure.Messaging.ServiceBus;
using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Model;

namespace Axxon.Integrator.SyncEngine.Functions;

/// <summary>
/// Publicación al topic 'changes' para los runs agendados, con las mismas convenciones
/// de sesión y dedupe que la ingesta (<see cref="ChangeEventMessages"/>): lo que trae
/// el pull entra al motor por la misma puerta que un data event.
/// </summary>
public sealed class ServiceBusChangeEventPublisher(ServiceBusSender changesTopicSender) : IChangeEventPublisher
{
    public Task PublishAsync(ChangeEvent evt, CancellationToken ct) =>
        changesTopicSender.SendMessageAsync(ChangeEventMessages.Envelope(evt), ct);
}
