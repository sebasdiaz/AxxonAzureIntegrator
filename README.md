# AxxonAzureIntegrator

Integrador de datos bidireccional near-real-time en Azure, estilo Dual Write pero
desacoplado, extensible a cualquier sistema y **sin desarrollo sobre F&O** (data events
nativos → Service Bus para capturar, OData para escribir).

📐 Diseño completo: [docs/architecture.md](docs/architecture.md)

## Estructura

```
src/
  Axxon.Integrator.Core/                 Contratos, modelos y motor (sin Azure, testeable)
  Axxon.Integrator.Connectors.FinOps/    Conector Dynamics 365 F&O
  Axxon.Integrator.Connectors.Dataverse/ Conector Dataverse
  Axxon.Integrator.SyncEngine/           Azure Functions (.NET 8 isolated)
  Axxon.Integrator.AdminPortal/          Portal Blazor: diseñador de mapas, DLQ, métricas
infra/
  main.bicep                             Service Bus, Cosmos (xref), Blob (mapas), Function App, Key Vault, App Insights
docs/
  architecture.md                        Decisiones de diseño y flujos
```

## Build

```powershell
dotnet build AxxonAzureIntegrator.slnx
```

## Portal de administración

```powershell
dotnet run --project src/Axxon.Integrator.AdminPortal
```

Diseñador de mapas estilo Dual Write (campos, transformaciones, value maps,
integration key compuesta) persistiendo documentos JSON en `maps/` (en producción,
blobs en el container `entity-maps` con versioning — mismo documento).

## Estado

Esqueleto de arquitectura (fase 0). Ver las fases del roadmap en el documento de diseño.
