namespace Axxon.Integrator.Azure;

/// <summary>
/// Credenciales de la app registration de Entra ID con la que el motor escribe en un
/// sistema (decisión 14: client credentials para Dataverse y F&O). Una sección de
/// configuración por sistema ("Dataverse", "FinOps"); el secret llega por referencia
/// de Key Vault en producción y por user-secrets/local.settings.json en desarrollo.
/// </summary>
public sealed record EntraAppOptions
{
    /// <summary>
    /// URL raíz del ambiente, que es también el resource del token
    /// (ej. "https://org.crm.dynamics.com", "https://axxon-dev.operations.dynamics.com").
    /// </summary>
    public string EnvironmentUrl { get; init; } = "";

    public string TenantId { get; init; } = "";
    public string ClientId { get; init; } = "";
    public string ClientSecret { get; init; } = "";

    /// <summary>
    /// ID del usuario que el sistema asocia a esta app registration (application user
    /// en Dataverse, usuario de servicio de Microsoft Entra applications en F&O).
    /// Alimenta la lista de usuarios de integración del EchoGuard: la credencial de
    /// escritura y la defensa primaria anti-loop son la misma identidad.
    /// </summary>
    public string IntegrationUserId { get; init; } = "";

    /// <summary>Scope de client credentials contra el ambiente.</summary>
    public string Scope => $"{EnvironmentUrl.TrimEnd('/')}/.default";

    /// <summary>
    /// False mientras falte configuración (desarrollo local sin credenciales): los
    /// hosts arrancan igual y el error claro recién aparece si se intenta llamar al
    /// sistema.
    /// </summary>
    public bool IsConfigured =>
        !string.IsNullOrWhiteSpace(EnvironmentUrl) &&
        !string.IsNullOrWhiteSpace(TenantId) &&
        !string.IsNullOrWhiteSpace(ClientId) &&
        !string.IsNullOrWhiteSpace(ClientSecret);
}
