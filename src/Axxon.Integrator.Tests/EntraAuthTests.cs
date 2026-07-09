using Axxon.Integrator.Azure;
using Axxon.Integrator.Connectors.Dataverse;
using Xunit;

namespace Axxon.Integrator.Tests;

/// <summary>
/// Los tres caminos de configuración incompleta deben dar errores que digan QUÉ
/// falta, nunca el críptico "BaseAddress must be set" de HttpClient.
/// </summary>
public sealed class EntraAuthTests
{
    [Fact]
    public void BaseAddress_depends_only_on_environment_url()
    {
        // Config parcial (falta el secret): el request debe poder salir hacia el
        // handler, que es quien reporta las claves faltantes.
        using var client = EntraHttp.ClientFor(new EntraAppOptions { EnvironmentUrl = "https://org.crm.dynamics.com" });

        Assert.Equal(new Uri("https://org.crm.dynamics.com/"), client.BaseAddress);
    }

    [Fact]
    public async Task Missing_secret_reports_exactly_which_keys_are_missing()
    {
        using var client = EntraHttp.ClientFor(new EntraAppOptions
        {
            EnvironmentUrl = "https://org.crm.dynamics.com",
            TenantId = "tenant",
            ClientId = "client",
            // ClientSecret ausente: el caso típico de olvidarse el user-secret
        });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => client.GetAsync("api/data/v9.2/WhoAmI", CancellationToken.None));

        Assert.Contains("ClientSecret", ex.Message);
        Assert.DoesNotContain("TenantId", ex.Message);
    }

    [Fact]
    public async Task Missing_environment_url_reports_the_setting_name()
    {
        var options = new EntraAppOptions(); // sin nada configurado
        var connector = new DataverseConnector(EntraHttp.ClientFor(options), options);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => connector.ListEntitiesAsync(CancellationToken.None));

        Assert.Contains("Dataverse:EnvironmentUrl", ex.Message);
    }
}
