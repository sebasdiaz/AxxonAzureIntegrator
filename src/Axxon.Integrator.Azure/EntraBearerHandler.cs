using System.Net.Http.Headers;
using Azure.Core;
using Azure.Identity;

namespace Axxon.Integrator.Azure;

/// <summary>
/// DelegatingHandler que autentica cada request con un bearer token de client
/// credentials de la app registration (decisión 14). El token se cachea y se renueva
/// con margen antes de expirar. La credencial se crea recién en el primer request:
/// un host sin credenciales configuradas arranca igual, y el error claro aparece al
/// intentar usar el conector.
/// </summary>
public sealed class EntraBearerHandler(EntraAppOptions options) : DelegatingHandler
{
    private static readonly TimeSpan RefreshMargin = TimeSpan.FromMinutes(5);

    private readonly SemaphoreSlim _refreshLock = new(1, 1);
    private TokenCredential? _credential;
    private AccessToken _token;

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
    {
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", await GetTokenAsync(ct));
        return await base.SendAsync(request, ct);
    }

    private async Task<string> GetTokenAsync(CancellationToken ct)
    {
        if (_token.ExpiresOn > DateTimeOffset.UtcNow + RefreshMargin)
        {
            return _token.Token;
        }

        await _refreshLock.WaitAsync(ct);
        try
        {
            if (_token.ExpiresOn <= DateTimeOffset.UtcNow + RefreshMargin)
            {
                if (!options.IsConfigured)
                {
                    throw new InvalidOperationException(
                        $"Faltan credenciales de la app registration para '{options.EnvironmentUrl}': " +
                        "configurá EnvironmentUrl, TenantId, ClientId y ClientSecret en la sección del sistema.");
                }

                _credential ??= new ClientSecretCredential(options.TenantId, options.ClientId, options.ClientSecret);
                _token = await _credential.GetTokenAsync(new TokenRequestContext([options.Scope]), ct);
            }
            return _token.Token;
        }
        finally
        {
            _refreshLock.Release();
        }
    }
}

/// <summary>Fábrica del HttpClient autenticado que recibe cada conector.</summary>
public static class EntraHttp
{
    public static HttpClient ClientFor(EntraAppOptions options) => new(
        new EntraBearerHandler(options)
        {
            // Cliente singleton (vive lo que el conector): rotar conexiones para no
            // fijar DNS de por vida.
            InnerHandler = new SocketsHttpHandler { PooledConnectionLifetime = TimeSpan.FromMinutes(10) },
        })
    {
        BaseAddress = options.IsConfigured ? new Uri(options.EnvironmentUrl.TrimEnd('/') + "/") : null,
    };
}
