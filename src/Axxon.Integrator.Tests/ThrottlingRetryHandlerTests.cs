using System.Net;
using System.Net.Http.Headers;
using Axxon.Integrator.Azure;
using Xunit;

namespace Axxon.Integrator.Tests;

/// <summary>
/// Reintento ante throttling del pipeline HTTP de los conectores: 429/503 se
/// reintentan honrando Retry-After (con tope), el resto de los status pasan directo,
/// y los reintentos re-envían el cuerpo original (clonado, porque un
/// HttpRequestMessage no se puede re-enviar).
/// </summary>
public sealed class ThrottlingRetryHandlerTests
{
    [Fact]
    public async Task Retries_429_honoring_retry_after_and_returns_success()
    {
        var stub = new SequencedStub(
            Throttled(retryAfterSeconds: 0),
            Throttled(retryAfterSeconds: 0),
            () => new HttpResponseMessage(HttpStatusCode.OK));
        var client = ClientWith(stub);

        var response = await client.GetAsync("data/CustomersV3");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(3, stub.Requests.Count);
    }

    [Fact]
    public async Task Retry_resends_the_original_body()
    {
        var stub = new SequencedStub(
            Throttled(retryAfterSeconds: 0),
            () => new HttpResponseMessage(HttpStatusCode.OK));
        var client = ClientWith(stub);

        await client.PostAsync("data/CustomersV3", new StringContent("""{"a":1}"""));

        Assert.Equal(2, stub.Requests.Count);
        Assert.Equal("""{"a":1}""", stub.Bodies[1]);
    }

    [Fact]
    public async Task Gives_up_after_max_retries_and_returns_the_429()
    {
        var stub = new SequencedStub(Enumerable.Repeat((Func<HttpResponseMessage>)(() => Throttled(0)()), 10).ToArray());
        var client = ClientWith(stub);

        var response = await client.GetAsync("data/CustomersV3");

        Assert.Equal(HttpStatusCode.TooManyRequests, response.StatusCode);
        Assert.Equal(4, stub.Requests.Count); // 1 intento + 3 reintentos
    }

    [Fact]
    public async Task Huge_retry_after_short_circuits_instead_of_hanging()
    {
        var stub = new SequencedStub(Throttled(retryAfterSeconds: 600), () => new HttpResponseMessage(HttpStatusCode.OK));
        var client = ClientWith(stub);

        var response = await client.GetAsync("data/CustomersV3");

        Assert.Equal(HttpStatusCode.TooManyRequests, response.StatusCode);
        Assert.Single(stub.Requests);
    }

    [Fact]
    public async Task Permanent_errors_are_not_retried()
    {
        var stub = new SequencedStub(() => new HttpResponseMessage(HttpStatusCode.BadRequest));
        var client = ClientWith(stub);

        var response = await client.GetAsync("data/CustomersV3");

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Single(stub.Requests);
    }

    private static Func<HttpResponseMessage> Throttled(int retryAfterSeconds) => () =>
    {
        var response = new HttpResponseMessage(HttpStatusCode.TooManyRequests);
        response.Headers.RetryAfter = new RetryConditionHeaderValue(TimeSpan.FromSeconds(retryAfterSeconds));
        return response;
    };

    private static HttpClient ClientWith(SequencedStub stub) => new(
        new ThrottlingRetryHandler { InnerHandler = stub })
    {
        BaseAddress = new Uri("https://unit.test/"),
    };

    private sealed class SequencedStub(params Func<HttpResponseMessage>[] responses) : HttpMessageHandler
    {
        private readonly Queue<Func<HttpResponseMessage>> _responses = new(responses);

        public List<HttpRequestMessage> Requests { get; } = [];
        public List<string> Bodies { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Requests.Add(request);
            Bodies.Add(request.Content is null ? "" : await request.Content.ReadAsStringAsync(ct));
            var factory = _responses.Count > 1 ? _responses.Dequeue() : _responses.Peek();
            var response = factory();
            response.RequestMessage = request;
            return response;
        }
    }
}
