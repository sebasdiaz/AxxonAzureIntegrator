using System.Net.Http.Headers;

namespace Axxon.Integrator.Azure;

/// <summary>GET OData v4 con los headers que piden Dataverse (Web API) y F&O (OData / Metadata service).</summary>
public static class ODataRequest
{
    public static HttpRequestMessage Get(string url)
    {
        var request = new HttpRequestMessage(HttpMethod.Get, url);
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        request.Headers.Add("OData-MaxVersion", "4.0");
        request.Headers.Add("OData-Version", "4.0");
        return request;
    }
}
