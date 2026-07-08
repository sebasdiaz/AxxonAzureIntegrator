// Infraestructura base del integrador. Punto de partida — falta parametrizar
// networking, RBAC fino y alertas.
targetScope = 'resourceGroup'

@description('Prefijo para nombrar recursos (minúsculas, sin guiones).')
param prefix string = 'axxonint'

@description('Ambiente: dev, test, prod.')
@allowed(['dev', 'test', 'prod'])
param env string = 'dev'

param location string = resourceGroup().location

var name = '${prefix}-${env}'

// --- Service Bus: backbone de mensajería ---------------------------------
// Standard como mínimo: Basic no soporta topics.
resource serviceBus 'Microsoft.ServiceBus/namespaces@2022-10-01-preview' = {
  name: '${name}-sb'
  location: location
  sku: {
    name: 'Standard'
    tier: 'Standard'
  }
}

// Cola de ingesta: F&O (data events) y Dataverse (service endpoints) publican acá
// sus payloads nativos. Sin sesiones: esos endpoints no permiten estampar SessionId.
// El IngestProcessor parsea, normaliza y re-publica al topic 'changes' con SessionId
// y MessageId determinístico. Payload imparseable = error permanente → DLQ de esta cola.
resource ingestQueue 'Microsoft.ServiceBus/namespaces/queues@2022-10-01-preview' = {
  parent: serviceBus
  name: 'ingest'
  properties: {
    maxDeliveryCount: 10
    deadLetteringOnMessageExpiration: true
    lockDuration: 'PT1M'
  }
}

// Topic de eventos normalizados (ChangeEvent en JSON). El duplicate detection es
// efectivo porque el IngestProcessor estampa MessageId determinístico por evento.
resource changesTopic 'Microsoft.ServiceBus/namespaces/topics@2022-10-01-preview' = {
  parent: serviceBus
  name: 'changes'
  properties: {
    requiresDuplicateDetection: true
    duplicateDetectionHistoryTimeWindow: 'PT10M'
    supportOrdering: true
  }
}

resource engineSubscription 'Microsoft.ServiceBus/namespaces/topics/subscriptions@2022-10-01-preview' = {
  parent: changesTopic
  name: 'sync-engine'
  properties: {
    requiresSession: true // orden por registro: SessionId = ID del registro origen
    maxDeliveryCount: 10 // agotados los reintentos, el mensaje va a la DLQ
    deadLetteringOnMessageExpiration: true
    lockDuration: 'PT1M'
  }
}

// --- Cosmos DB: mapas, xref, watermarks -----------------------------------
resource cosmos 'Microsoft.DocumentDB/databaseAccounts@2023-11-15' = {
  name: '${name}-cosmos'
  location: location
  kind: 'GlobalDocumentDB'
  properties: {
    databaseAccountOfferType: 'Standard'
    capabilities: [
      { name: 'EnableServerless' }
    ]
    consistencyPolicy: {
      defaultConsistencyLevel: 'Session'
    }
    locations: [
      { locationName: location, failoverPriority: 0 }
    ]
  }
}

resource cosmosDb 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases@2023-11-15' = {
  parent: cosmos
  name: 'integrator'
  properties: {
    resource: { id: 'integrator' }
  }
}

resource mapsContainer 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2023-11-15' = {
  parent: cosmosDb
  name: 'entity-maps'
  properties: {
    resource: {
      id: 'entity-maps'
      partitionKey: { paths: ['/sourceSystem'], kind: 'Hash' }
    }
  }
}

// Dos documentos espejo por vínculo (uno por lado del par) con
// lookupKey = '<pairKey>|<system>|<recordId>': point reads O(1) desde cualquier
// lado y distribución pareja incluso durante el sync inicial (con pk por mapa, una
// migración masiva martillaría una sola partición lógica). Consistencia entre
// espejos por ETag; el escritor es único por sesión, así que la contención es rara.
resource xrefContainer 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2023-11-15' = {
  parent: cosmosDb
  name: 'xref'
  properties: {
    resource: {
      id: 'xref'
      partitionKey: { paths: ['/lookupKey'], kind: 'Hash' }
    }
  }
}

// --- Observabilidad --------------------------------------------------------
resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2022-10-01' = {
  name: '${name}-logs'
  location: location
  properties: {
    sku: { name: 'PerGB2018' }
    retentionInDays: 30
  }
}

resource appInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: '${name}-ai'
  location: location
  kind: 'web'
  properties: {
    Application_Type: 'web'
    WorkspaceResourceId: logAnalytics.id
  }
}

// --- Key Vault: secrets (connection string del bus para el endpoint de F&O) -
resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: '${name}-kv'
  location: location
  properties: {
    sku: { family: 'A', name: 'standard' }
    tenantId: tenant().tenantId
    enableRbacAuthorization: true
  }
}

// --- Function App (motor de sincronización) --------------------------------
resource storage 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: '${prefix}${env}st'
  location: location
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
}

resource plan 'Microsoft.Web/serverfarms@2023-01-01' = {
  name: '${name}-plan'
  location: location
  sku: {
    name: 'Y1' // consumption; pasar a EP1 si el cold start molesta
    tier: 'Dynamic'
  }
}

resource functionApp 'Microsoft.Web/sites@2023-01-01' = {
  name: '${name}-func'
  location: location
  kind: 'functionapp'
  identity: {
    type: 'SystemAssigned' // managed identity: RBAC contra Service Bus, Cosmos y Key Vault
  }
  properties: {
    serverFarmId: plan.id
    siteConfig: {
      netFrameworkVersion: 'v8.0'
      appSettings: [
        { name: 'AzureWebJobsStorage', value: 'DefaultEndpointsProtocol=https;AccountName=${storage.name};AccountKey=${storage.listKeys().keys[0].value}' }
        { name: 'FUNCTIONS_EXTENSION_VERSION', value: '~4' }
        { name: 'FUNCTIONS_WORKER_RUNTIME', value: 'dotnet-isolated' }
        { name: 'APPLICATIONINSIGHTS_CONNECTION_STRING', value: appInsights.properties.ConnectionString }
        { name: 'ServiceBusConnection__fullyQualifiedNamespace', value: '${serviceBus.name}.servicebus.windows.net' }
        { name: 'Sync:IngestQueue', value: ingestQueue.name }
        { name: 'Sync:ChangesTopic', value: changesTopic.name }
        { name: 'Sync:EngineSubscription', value: engineSubscription.name }
      ]
    }
  }
}

output serviceBusNamespace string = serviceBus.name
output functionAppName string = functionApp.name
output cosmosAccountName string = cosmos.name
output keyVaultName string = keyVault.name
