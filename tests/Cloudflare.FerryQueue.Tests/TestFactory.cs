using Cloudflare.FerryQueue.Client;
using Cloudflare.FerryQueue.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RichardSzalay.MockHttp;

namespace Cloudflare.FerryQueue.Tests;

internal static class TestFactory
{
    internal const string AccountId  = "test-account-abc123";
    internal const string ApiToken   = "test-token-xyz";
    internal const string BaseUrl    = "https://api.cloudflare.com/client/v4";
    internal const string QueueId    = "q-11111111-1111-1111-1111-111111111111";
    internal const string ConsumerId = "c-22222222-2222-2222-2222-222222222222";

    internal static (CloudflareQueuesClient client, MockHttpMessageHandler mockHttp) CreateClient(
        int maxRetries = 0)
    {
        var mockHttp = new MockHttpMessageHandler();
        var options  = Options.Create(new CloudflareQueuesOptions
        {
            AccountId  = AccountId,
            ApiToken   = ApiToken,
            BaseUrl    = BaseUrl,
            MaxRetries = maxRetries,
        });

        var http = mockHttp.ToHttpClient();
        http.BaseAddress = new Uri(BaseUrl + "/");

        var client = new CloudflareQueuesClient(
            http,
            options,
            NullLogger<CloudflareQueuesClient>.Instance);

        return (client, mockHttp);
    }

    internal static string QueueUrl(string path)
        => $"*/accounts/{AccountId}/queues/{QueueId}/{path}";

    internal static string OkEnvelope(string result = "{}")
        => $$"""{ "success": true, "errors": [], "messages": [], "result": {{result}} }""";

    internal static string ErrorEnvelope(int code = 10000, string message = "some error")
        => $$"""
             {
               "success": false,
               "errors": [{ "code": {{code}}, "message": "{{message}}" }],
               "messages": [],
               "result": null
             }
             """;
}
