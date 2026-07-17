// Infraestructura base del integrador. Punto de partida — falta parametrizar
// networking, RBAC fino y alertas.
targetScope = 'resourceGroup'

@description('Prefijo para nombrar recursos (minúsculas, sin guiones).')
param prefix string = 'axxonint'

@description('Ambiente: dev, test, prod.')
@allowed(['dev', 'test', 'prod'])
param env string = 'dev'

param location string = resourceGroup().location

// --- App registrations (decisión 14): escritura en Dataverse y F&O -----------
// Los client secrets NO son parámetros: viven en Key Vault como
// 'dataverse-client-secret' y 'finops-client-secret'; la Function App los lee por
// referencia. Los IntegrationUserId alimentan la supresión de eco.
@description('URL del ambiente Dataverse (ej. https://org.crm.dynamics.com). Vacío hasta aprovisionar.')
param dataverseEnvironmentUrl string = ''
param dataverseClientId string = ''
param dataverseIntegrationUserId string = ''

@description('URL del ambiente F&O (ej. https://axxon-dev.operations.dynamics.com). Vacío hasta aprovisionar.')
param finopsEnvironmentUrl string = ''
param finopsClientId string = ''
param finopsIntegrationUserId string = ''

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

// Cola de runs agendados: el ScheduleDispatcher (timer) encola una ocurrencia por
// mapa vencido y el ScheduledRunProcessor la ejecuta. Sesiones (SessionId = mapa) =
// un mapa nunca corre dos runs en paralelo; duplicate detection (MessageId = mapa +
// ocurrencia) = ticks del timer con ventanas solapadas no duplican runs.
resource scheduledRunsQueue 'Microsoft.ServiceBus/namespaces/queues@2022-10-01-preview' = {
  parent: serviceBus
  name: 'scheduled-runs'
  properties: {
    requiresSession: true
    requiresDuplicateDetection: true
    duplicateDetectionHistoryTimeWindow: 'PT10M'
    maxDeliveryCount: 5
    deadLetteringOnMessageExpiration: true
    lockDuration: 'PT5M' // un run hace pull paginado del origen: más largo que un mensaje común
  }
}

// --- Cosmos DB: xref (vínculos + estado) y watermarks ----------------------
// Los mapas NO viven acá: son blobs JSON en el storage account (más abajo).
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

// Watermarks de los mapas agendados: un documento por mapa (id = 'system|map:<nombre>'),
// point read + upsert. El escritor es único (sesiones de 'scheduled-runs'), sin ETag.
resource watermarksContainer 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2023-11-15' = {
  parent: cosmosDb
  name: 'watermarks'
  properties: {
    resource: {
      id: 'watermarks'
      partitionKey: { paths: ['/id'], kind: 'Hash' }
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

// Los mapas de entidades viven como blobs JSON planos (decisión 13): un blob por
// mapa en 'entity-maps'. El versioning del blob service conserva la historia de
// cada guardado del diseñador; el acceso es por managed identity con el rol
// Storage Blob Data Contributor (pendiente junto con el resto del RBAC).
resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' = {
  parent: storage
  name: 'default'
  properties: {
    isVersioningEnabled: true
  }
}

resource entityMapsContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobService
  name: 'entity-maps'
}

// Histórico de sincronización por mapa (PartitionKey = mapa, RowKey = ticks
// invertidos): lo escribe el pipeline best-effort y lo lee la pestaña Histórico del
// portal. Acceso por managed identity con Storage Table Data Contributor (pendiente
// junto con el resto del RBAC). Retención: Tables no tiene TTL — limpieza pendiente.
resource tableService 'Microsoft.Storage/storageAccounts/tableServices@2023-01-01' = {
  parent: storage
  name: 'default'
}

resource syncHistoryTable 'Microsoft.Storage/storageAccounts/tableServices/tables@2023-01-01' = {
  parent: tableService
  name: 'synchistory'
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
        { name: 'Sync:ScheduledRunsQueue', value: scheduledRunsQueue.name }
        { name: 'Maps__BlobContainerUri', value: 'https://${storage.name}.blob.${environment().suffixes.storage}/${entityMapsContainer.name}' }
        { name: 'History__TableUri', value: 'https://${storage.name}.table.${environment().suffixes.storage}' }
        { name: 'History__TableName', value: syncHistoryTable.name }
        // Xref en Cosmos por identidad (rol de datos Cosmos DB Built-in Data
        // Contributor sobre la managed identity: pendiente junto al resto del RBAC).
        { name: 'Xref__CosmosAccountEndpoint', value: cosmos.properties.documentEndpoint }
        // App registrations (decisión 14). Secrets por referencia de Key Vault:
        // requiere el rol Key Vault Secrets User sobre la managed identity (pendiente
        // junto con el resto del RBAC).
        { name: 'Dataverse__EnvironmentUrl', value: dataverseEnvironmentUrl }
        { name: 'Dataverse__TenantId', value: tenant().tenantId }
        { name: 'Dataverse__ClientId', value: dataverseClientId }
        { name: 'Dataverse__ClientSecret', value: '@Microsoft.KeyVault(SecretUri=${keyVault.properties.vaultUri}secrets/dataverse-client-secret/)' }
        { name: 'Dataverse__IntegrationUserId', value: dataverseIntegrationUserId }
        { name: 'FinOps__EnvironmentUrl', value: finopsEnvironmentUrl }
        { name: 'FinOps__TenantId', value: tenant().tenantId }
        { name: 'FinOps__ClientId', value: finopsClientId }
        { name: 'FinOps__ClientSecret', value: '@Microsoft.KeyVault(SecretUri=${keyVault.properties.vaultUri}secrets/finops-client-secret/)' }
        { name: 'FinOps__IntegrationUserId', value: finopsIntegrationUserId }
      ]
    }
  }
}

output serviceBusNamespace string = serviceBus.name
output functionAppName string = functionApp.name
output cosmosAccountName string = cosmos.name
output keyVaultName string = keyVault.name
