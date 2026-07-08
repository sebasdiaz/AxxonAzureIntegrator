using Axxon.Integrator.Connectors.Dataverse;
using Axxon.Integrator.Connectors.FinOps;
using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Sync;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices(services =>
    {
        services.AddApplicationInsightsTelemetryWorkerService();

        services.AddSingleton<IConnector, FinOpsConnector>();
        services.AddSingleton<IConnector, DataverseConnector>();
        services.AddSingleton<IChangeEventParser, FinOpsDataEventParser>();

        services.AddSingleton<IReadOnlyDictionary<string, IConnector>>(sp =>
            sp.GetServices<IConnector>().ToDictionary(c => c.SystemName));

        services.AddSingleton<MappingEngine>();

        // TODO(MVP): implementaciones reales sobre Cosmos DB. Los IDs de los usuarios
        // de integración (app users de Dataverse / F&O) vienen de configuración.
        // services.AddSingleton<IEntityMapStore, CosmosEntityMapStore>();
        // services.AddSingleton<IXrefStore, CosmosXrefStore>();
        // services.AddSingleton<IWatermarkStore, CosmosWatermarkStore>();
        // services.AddSingleton(sp => new EchoGuard(sp.GetRequiredService<IXrefStore>(), integrationUserIds));
        // services.AddSingleton<SyncPipeline>();
    });

builder.Build().Run();
