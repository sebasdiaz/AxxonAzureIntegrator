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
infra/
  main.bicep                             Service Bus, Cosmos, Function App, Key Vault, App Insights
docs/
  architecture.md                        Decisiones de diseño y flujos
```

## Build

```powershell
dotnet build AxxonAzureIntegrator.slnx
```

## Estado

Esqueleto de arquitectura (fase 0). Ver las fases del roadmap en el documento de diseño.
