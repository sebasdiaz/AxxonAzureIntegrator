using Axxon.Integrator.AdminPortal.Components;
using Axxon.Integrator.Azure;
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

// Mapas como documentos JSON: archivos locales en desarrollo; en producción el mismo
// documento vive en Cosmos DB. TODO(fase 4): CosmosEntityMapStore + autenticación
// Entra ID con roles viewer/operator.
builder.Services.AddSingleton<IEntityMapStore>(_ => new JsonFileEntityMapStore(
    builder.Configuration["Maps:Directory"]
        ?? Path.Combine(builder.Environment.ContentRootPath, "maps")));

var app = builder.Build();

app.UseStaticFiles();
app.UseAntiforgery();
app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.Run();
