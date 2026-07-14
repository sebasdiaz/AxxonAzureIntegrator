using Axxon.Integrator.AdminPortal.Components;
using Axxon.Integrator.Azure;
using global::Azure.Data.Tables;
using global::Azure.Identity;
using global::Azure.Storage.Blobs;
using Axxon.Integrator.Connectors.Dataverse;
using Axxon.Integrator.Connectors.FinOps;
using Axxon.Integrator.Core.Abstractions;
using Axxon.Integrator.Core.Stores;
using Axxon.Integrator.Core.Sync;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

// Los conectores alimentan el diseñador de mapas con la metadata real de cada
// sistema (GetMetadataAsync), autenticados con las app registrations de Entra ID
// (decisión 14). Sin credenciales configuradas el portal arranca igual y el
// diseñador degrada a entrada manual de nombres de campo.
var dataverse = builder.Configuration.GetSection("Dataverse").Get<EntraAppOptions>() ?? new EntraAppOptions();
var finops = builder.Configuration.GetSection("FinOps").Get<EntraAppOptions>() ?? new EntraAppOptions();
builder.Services.AddSingleton<IConnector>(new DataverseConnector(EntraHttp.ClientFor(dataverse), dataverse));
builder.Services.AddSingleton<IConnector>(new FinOpsConnector(EntraHttp.ClientFor(finops), finops));
builder.Services.AddSingleton<IReadOnlyDictionary<string, IConnector>>(sp =>
    sp.GetServices<IConnector>().ToDictionary(c => c.SystemName));
builder.Services.AddSingleton<MappingEngine>();

// Mapas como documentos JSON planos: blobs en 'entity-maps' cuando hay
// Maps:BlobContainerUri (producción, managed identity), archivos locales si no
// (desarrollo). Mismo documento en ambos. TODO(fase 4): auth Entra ID con roles
// viewer/operator.
// Vacío cuenta como no-configurado: los templates traen "" y con ?? solo el null
// caería al default.
var mapsBlobUri = builder.Configuration["Maps:BlobContainerUri"];
if (!string.IsNullOrWhiteSpace(mapsBlobUri))
{
    builder.Services.AddSingleton<IEntityMapStore>(_ => new BlobEntityMapStore(
        new BlobContainerClient(new Uri(mapsBlobUri), new DefaultAzureCredential())));
}
else
{
    var mapsDirectory = builder.Configuration["Maps:Directory"];
    if (string.IsNullOrWhiteSpace(mapsDirectory))
    {
        mapsDirectory = Path.Combine(builder.Environment.ContentRootPath, "maps");
    }
    builder.Services.AddSingleton<IEntityMapStore>(_ => new JsonFileEntityMapStore(mapsDirectory));
}

// Histórico de sincronización (lo escribe el motor; acá solo se lee para la pestaña
// Histórico): Table Storage con History:TableUri, archivos JSONL locales si no. En
// desarrollo, apuntar History:Directory al mismo directorio que usa el SyncEngine.
var historyTableUri = builder.Configuration["History:TableUri"];
if (!string.IsNullOrWhiteSpace(historyTableUri))
{
    builder.Services.AddSingleton<ISyncHistoryStore>(_ => new TableSyncHistoryStore(new TableClient(
        new Uri(historyTableUri),
        builder.Configuration["History:TableName"] ?? "synchistory",
        new DefaultAzureCredential())));
}
else
{
    var historyDirectory = builder.Configuration["History:Directory"];
    if (string.IsNullOrWhiteSpace(historyDirectory))
    {
        historyDirectory = Path.Combine(builder.Environment.ContentRootPath, "history");
    }
    builder.Services.AddSingleton<ISyncHistoryStore>(_ => new JsonFileSyncHistoryStore(historyDirectory));
}

var app = builder.Build();

app.UseStaticFiles();
app.UseAntiforgery();
app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.Run();
