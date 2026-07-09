using System.Globalization;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace Axxon.Integrator.Azure;

/// <summary>Requests OData v4 con los headers que piden Dataverse (Web API) y F&O (OData / Metadata service).</summary>
public static class ODataRequest
{
    public static HttpRequestMessage Get(string url) => WithHeaders(new HttpRequestMessage(HttpMethod.Get, url));

    public static HttpRequestMessage Post(string url, HttpContent content) =>
        WithHeaders(new HttpRequestMessage(HttpMethod.Post, url) { Content = content });

    /// <summary>
    /// PATCH. Con <paramref name="ifMatchAny"/> es update-only (404 si el registro no
    /// existe); sin él, Dataverse lo trata como upsert por clave.
    /// </summary>
    public static HttpRequestMessage Patch(string url, HttpContent content, bool ifMatchAny = false)
    {
        var request = WithHeaders(new HttpRequestMessage(HttpMethod.Patch, url) { Content = content });
        if (ifMatchAny)
        {
            request.Headers.IfMatch.Add(EntityTagHeaderValue.Any);
        }
        return request;
    }

    public static HttpRequestMessage Delete(string url)
    {
        var request = WithHeaders(new HttpRequestMessage(HttpMethod.Delete, url));
        request.Headers.IfMatch.Add(EntityTagHeaderValue.Any);
        return request;
    }

    public static HttpContent JsonContent(object value) =>
        new StringContent(JsonSerializer.Serialize(value), Encoding.UTF8, "application/json");

    /// <summary>Error con status y cuerpo de la respuesta: el mensaje pelado de EnsureSuccessStatusCode no alcanza para diagnosticar un 400 de OData.</summary>
    public static async Task EnsureSuccessAsync(HttpResponseMessage response, CancellationToken ct)
    {
        if (response.IsSuccessStatusCode)
        {
            return;
        }

        var body = await response.Content.ReadAsStringAsync(ct);
        throw new HttpRequestException(
            $"{(int)response.StatusCode} {response.ReasonPhrase} de {response.RequestMessage?.Method} {response.RequestMessage?.RequestUri}: " +
            (body.Length > 500 ? body[..500] : body),
            inner: null,
            response.StatusCode);
    }

    private static HttpRequestMessage WithHeaders(HttpRequestMessage request)
    {
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        request.Headers.Add("OData-MaxVersion", "4.0");
        request.Headers.Add("OData-Version", "4.0");
        return request;
    }
}

/// <summary>
/// Formateo de valores como literales OData para URLs de clave y $filter, con
/// unwrapping de JsonElement (los payloads deserializados traen los valores así).
/// </summary>
public static class ODataLiteral
{
    public static string Format(object? value) => value switch
    {
        null => "null",
        JsonElement element => FromJson(element),
        string text => Quote(text),
        bool flag => flag ? "true" : "false",
        Guid guid => guid.ToString(),
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
        _ => Quote(value.ToString() ?? ""),
    };

    private static string FromJson(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.String => Quote(element.GetString()!),
        JsonValueKind.Number => element.GetRawText(),
        JsonValueKind.True => "true",
        JsonValueKind.False => "false",
        JsonValueKind.Null => "null",
        _ => Quote(element.GetRawText()),
    };

    /// <summary>Comillas simples con escape OData (' → ''): frena inyección en $filter y claves.</summary>
    private static string Quote(string value) => $"'{value.Replace("'", "''")}'";
}
