using Axxon.Integrator.Azure;
using Axxon.Integrator.Connectors.Dataverse;
using Axxon.Integrator.Connectors.FinOps;
using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Stores;
using Axxon.Integrator.Core.Sync;
using global::Azure.Identity;
using global::Azure.Messaging.ServiceBus;
using global::Azure.Storage.Blobs;
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

        // Xref: archivos JSON locales (desarrollo). TODO(fase 1): CosmosXrefStore
        // (documentos espejo por lado, ETag) y CosmosWatermarkStore cuando entre el
        // catch-up por polling.
        var xrefDirectory = context.Configuration["Xref:Directory"];
        services.AddSingleton<IXrefStore>(_ => new JsonFileXrefStore(
            string.IsNullOrWhiteSpace(xrefDirectory) ? "xref" : xrefDirectory));

        services.AddSingleton<SyncPipeline>();
    });

builder.Build().Run();
