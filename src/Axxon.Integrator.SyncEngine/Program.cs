using Axxon.Integrator.Azure;
using Axxon.Integrator.Connectors.Dataverse;
using Axxon.Integrator.Connectors.FinOps;
using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Stores;
using Axxon.Integrator.Core.Sync;
using Axxon.Integrator.SyncEngine.Functions;
using global::Azure.Data.Tables;
using global::Azure.Identity;
using global::Azure.Messaging.ServiceBus;
using global::Azure.Storage.Blobs;
using Microsoft.Azure.Cosmos;
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

        // Mapas: blobs en 'entity-maps' cuando hay Maps:BlobContainerUri (producción,
        // managed identity), archivos locales si no (desarrollo). TODO(fase 1): caché
        // en memoria delante del store — el pipeline no debe listar blobs por evento.
        var mapsBlobUri = context.Configuration["Maps:BlobContainerUri"];
        if (!string.IsNullOrWhiteSpace(mapsBlobUri))
        {
            services.AddSingleton<IEntityMapStore>(_ => new BlobEntityMapStore(
                new BlobContainerClient(new Uri(mapsBlobUri), new DefaultAzureCredential())));
        }
        else
        {
            var mapsDirectory = context.Configuration["Maps:Directory"];
            services.AddSingleton<IEntityMapStore>(_ => new JsonFileEntityMapStore(
                string.IsNullOrWhiteSpace(mapsDirectory) ? "maps" : mapsDirectory));
        }

        // Publicación del IngestProcessor al topic 'changes'. Misma convención de
        // conexión que el trigger: 'ServiceBusConnection' como connection string, o
        // 'ServiceBusConnection__fullyQualifiedNamespace' para identidad. La validación
        // corre al construirse el primer function, no al arrancar el host.
        services.AddSingleton(_ =>
        {
            var connectionString = context.Configuration["ServiceBusConnection"];
            if (!string.IsNullOrWhiteSpace(connectionString))
            {
                return new ServiceBusClient(connectionString);
            }

            var qualifiedNamespace = context.Configuration["ServiceBusConnection:fullyQualifiedNamespace"];
            if (!string.IsNullOrWhiteSpace(qualifiedNamespace))
            {
                return new ServiceBusClient(qualifiedNamespace, new DefaultAzureCredential());
            }

            throw new InvalidOperationException(
                "Configurar 'ServiceBusConnection' (connection string) o 'ServiceBusConnection__fullyQualifiedNamespace' (identidad).");
        });
        services.AddSingleton(sp => sp.GetRequiredService<ServiceBusClient>()
            .CreateSender(context.Configuration["Sync:ChangesTopic"] ?? "changes"));

        // Mapas agendados: el dispatcher encola runs en 'scheduled-runs' (sesiones por
        // mapa) y el run publica lo pulleado al topic 'changes' con las mismas
        // convenciones que la ingesta.
        services.AddSingleton(sp => new ScheduledRunsSender(sp.GetRequiredService<ServiceBusClient>()
            .CreateSender(context.Configuration["Sync:ScheduledRunsQueue"] ?? "scheduled-runs")));
        services.AddSingleton<IChangeEventPublisher>(sp =>
            new ServiceBusChangeEventPublisher(sp.GetRequiredService<ServiceBusSender>()));
        services.AddSingleton<ScheduledRunService>();

        // Histórico de sincronización: Table Storage cuando hay History:TableUri
        // (producción, managed identity), archivos JSONL locales si no (desarrollo).
        // El pipeline lo escribe best-effort; la pestaña Histórico del portal lo lee.
        var historyTableUri = context.Configuration["History:TableUri"];
        if (!string.IsNullOrWhiteSpace(historyTableUri))
        {
            services.AddSingleton<ISyncHistoryStore>(_ => new TableSyncHistoryStore(new TableClient(
                new Uri(historyTableUri),
                context.Configuration["History:TableName"] ?? "synchistory",
                new DefaultAzureCredential())));
        }
        else
        {
            var historyDirectory = context.Configuration["History:Directory"];
            services.AddSingleton<ISyncHistoryStore>(_ => new JsonFileSyncHistoryStore(
                string.IsNullOrWhiteSpace(historyDirectory) ? "history" : historyDirectory));
        }

        // Xref y watermarks: Cosmos cuando hay Xref:CosmosAccountEndpoint (producción
        // con identidad; en local, Xref:CosmosKey con la key de la cuenta), archivos
        // JSON locales si no (desarrollo). La base/contenedores los aprovisiona el
        // Bicep, no el motor.
        var cosmosEndpoint = context.Configuration["Xref:CosmosAccountEndpoint"];
        if (!string.IsNullOrWhiteSpace(cosmosEndpoint))
        {
            var cosmosKey = context.Configuration["Xref:CosmosKey"];
            var cosmosClient = string.IsNullOrWhiteSpace(cosmosKey)
                ? new CosmosClient(cosmosEndpoint, new DefaultAzureCredential(), CosmosXrefStore.ClientOptions)
                : new CosmosClient(cosmosEndpoint, cosmosKey, CosmosXrefStore.ClientOptions);
            var cosmosDatabase = context.Configuration["Xref:CosmosDatabase"] ?? "integrator";
            services.AddSingleton<IXrefStore>(_ => new CosmosXrefStore(cosmosClient.GetContainer(
                cosmosDatabase,
                context.Configuration["Xref:CosmosContainer"] ?? "xref")));
            services.AddSingleton<IWatermarkStore>(_ => new CosmosWatermarkStore(cosmosClient.GetContainer(
                cosmosDatabase,
                context.Configuration["Watermarks:CosmosContainer"] ?? "watermarks")));
        }
        else
        {
            var xrefDirectory = context.Configuration["Xref:Directory"];
            services.AddSingleton<IXrefStore>(_ => new JsonFileXrefStore(
                string.IsNullOrWhiteSpace(xrefDirectory) ? "xref" : xrefDirectory));
            var watermarksDirectory = context.Configuration["Watermarks:Directory"];
            services.AddSingleton<IWatermarkStore>(_ => new JsonFileWatermarkStore(
                string.IsNullOrWhiteSpace(watermarksDirectory) ? "watermarks" : watermarksDirectory));
        }

        services.AddSingleton<SyncPipeline>();
    });

builder.Build().Run();
