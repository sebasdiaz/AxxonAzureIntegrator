using System.Net;

namespace Axxon.Integrator.Azure;

/// <summary>
/// Reintento ante throttling para los HttpClient de los conectores: Dataverse (service
/// protection limits) y F&O responden 429 — y a veces 503 transitorio — con Retry-After.
/// Sin esto, el primer 429 revienta en quien llamó (el diseñador del portal cargando
/// metadata, un run agendado paginando el origen).
///
/// La espera honra Retry-After con tope: si el servicio pide más que
/// <see cref="MaxDelay"/>, no vale la pena colgar al llamador — se devuelve el 429 y
/// decide el camino de error de siempre (banner en el portal, reintento del trigger en
/// el motor). Solo se reintentan 429/503: un 4xx de verdad es permanente.
/// </summary>
public sealed class ThrottlingRetryHandler : DelegatingHandler
{
    private const int MaxRetries = 3;
    private static readonly TimeSpan DefaultDelay = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan MaxDelay = TimeSpan.FromSeconds(30);

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
    {
        // El contenido se buffea antes del primer envío: HttpClient no permite
        // re-enviar el mismo HttpRequestMessage, así que cada reintento es un clon
        // (los cuerpos son JSON chicos; el costo es despreciable).
        var body = request.Content is null ? null : await request.Content.ReadAsByteArrayAsync(ct);

        for (var attempt = 0; ; attempt++)
        {
            var current = attempt == 0 ? request : Clone(request, body);
            var response = await base.SendAsync(current, ct);

            if (attempt >= MaxRetries ||
                response.StatusCode is not (HttpStatusCode.TooManyRequests or HttpStatusCode.ServiceUnavailable))
            {
                return response;
            }

            var delay = RetryAfter(response) ?? TimeSpan.FromSeconds(DefaultDelay.TotalSeconds * (attempt + 1));
            if (delay > MaxDelay)
            {
                return response; // pausa demasiado larga: mejor devolver el 429 que colgar al llamador
            }

            response.Dispose();
            await Task.Delay(delay, ct);
        }
    }

    private static TimeSpan? RetryAfter(HttpResponseMessage response)
    {
        var retryAfter = response.Headers.RetryAfter;
        if (retryAfter?.Delta is { } delta)
        {
            return delta > TimeSpan.Zero ? delta : TimeSpan.Zero;
        }
        if (retryAfter?.Date is { } date)
        {
            var delta2 = date - DateTimeOffset.UtcNow;
            return delta2 > TimeSpan.Zero ? delta2 : TimeSpan.Zero;
        }
        return null;
    }

    private static HttpRequestMessage Clone(HttpRequestMessage request, byte[]? body)
    {
        var clone = new HttpRequestMessage(request.Method, request.RequestUri) { Version = request.Version };
        foreach (var header in request.Headers)
        {
            clone.Headers.TryAddWithoutValidation(header.Key, header.Value);
        }
        if (body is not null)
        {
            var content = new ByteArrayContent(body);
            if (request.Content is not null)
            {
                foreach (var header in request.Content.Headers)
                {
                    content.Headers.TryAddWithoutValidation(header.Key, header.Value);
                }
            }
            clone.Content = content;
        }
        return clone;
    }
}
