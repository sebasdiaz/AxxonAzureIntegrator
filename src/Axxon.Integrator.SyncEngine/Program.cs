using Axxon.Integrator.Azure;
using Axxon.Integrator.Connectors.Dataverse;
using Axxon.Integrator.Connectors.FinOps;
using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Sync;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices((context, services) =>
    {
        services.AddApplicationInsightsTelemetryWorkerService();

        // Decisión 14: escritura en cada sistema con la app registration de Entra ID
        // de su sección de configuración ("Dataverse", "FinOps"). El secret llega por
        // referencia de Key Vault en producción; sin configurar, el host arranca y el
        // error claro aparece al usar el conector.
        var dataverse = context.Configuration.GetSection("Dataverse").Get<EntraAppOptions>() ?? new EntraAppOptions();
        var finops = context.Configuration.GetSection("FinOps").Get<EntraAppOptions>() ?? new EntraAppOptions();

        services.AddSingleton<IConnector>(new DataverseConnector(EntraHttp.ClientFor(dataverse), dataverse));
        services.AddSingleton<IConnector>(new FinOpsConnector(EntraHttp.ClientFor(finops), finops));
        services.AddSingleton<IChangeEventParser, FinOpsDataEventParser>();

        services.AddSingleton<IReadOnlyDictionary<string, IConnector>>(sp =>
            sp.GetServices<IConnector>().ToDictionary(c => c.SystemName));

        services.AddSingleton<MappingEngine>();

        // Los usuarios de integración del EchoGuard son las mismas identidades con las
        // que el motor escribe: el application user de Dataverse y el usuario de
        // servicio de F&O asociados a las app registrations.
        services.AddSingleton(new EchoGuard(
            new[] { dataverse.IntegrationUserId, finops.IntegrationUserId }
                .Where(id => !string.IsNullOrWhiteSpace(id))
                .ToHashSet(StringComparer.OrdinalIgnoreCase)));

        // TODO(MVP): stores sobre Cosmos DB y sender del topic para el IngestProcessor.
        // services.AddSingleton<IEntityMapStore, CosmosEntityMapStore>();
        // services.AddSingleton<IXrefStore, CosmosXrefStore>();
        // services.AddSingleton<IWatermarkStore, CosmosWatermarkStore>();
        // services.AddSingleton(sp => serviceBusClient.CreateSender(changesTopicName));
        // services.AddSingleton<SyncPipeline>();
    });

builder.Build().Run();
