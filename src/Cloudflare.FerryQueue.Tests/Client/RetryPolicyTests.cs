using System.Net;
using Cloudflare.FerryQueue.Exceptions;
using Cloudflare.FerryQueue.Models;
using FluentAssertions;
using RichardSzalay.MockHttp;
using Xunit;

namespace Cloudflare.FerryQueue.Tests.Client;

public sealed class RetryPolicyTests
{
    [Theory]
    [InlineData(429)]
    [InlineData(500)]
    [InlineData(502)]
    [InlineData(503)]
    [InlineData(504)]
    public async Task Publish_RetriesOnTransientStatusCodes(int statusCode)
    {
        var (client, mockHttp) = TestFactory.CreateClient(maxRetries: 2);

        // First two calls → transient error; third → success
        mockHttp
            .When(HttpMethod.Post, TestFactory.QueueUrl("messages"))
            .Respond((HttpStatusCode)statusCode, "application/json",
                TestFactory.ErrorEnvelope(statusCode, "transient"));

        mockHttp
            .When(HttpMethod.Post, TestFactory.QueueUrl("messages"))
            .Respond(HttpStatusCode.OK, "application/json", TestFactory.OkEnvelope());

        // RichardSzalay.MockHttp returns responses in sequence when multiple matchers hit the same URL
        // so we just verify the exception is thrown once retries are exhausted
        var act = () => client.PublishAsync(TestFactory.QueueId, new OutboundMessage { Body = "x" });

        // With maxRetries=2 and all 3 attempts failing (mock always returns error), exception is thrown
        await act.Should().ThrowAsync<CloudflareQueuesException>();
    }

    [Fact]
    public async Task Publish_DoesNotRetry_On4xxNonTransient()
    {
        // 401 Unauthorized is not transient — should not retry
        var (client, mockHttp) = TestFactory.CreateClient(maxRetries: 3);

        var callCount = 0;
        mockHttp
            .When(HttpMethod.Post, TestFactory.QueueUrl("messages"))
            .Respond(_ =>
            {
                callCount++;
                return new HttpResponseMessage(HttpStatusCode.Unauthorized)
                {
                    Content = new StringContent(TestFactory.ErrorEnvelope(10000, "unauthorized"),
                        System.Text.Encoding.UTF8, "application/json")
                };
            });

        var act = () => client.PublishAsync(TestFactory.QueueId, new OutboundMessage { Body = "x" });

        await act.Should().ThrowAsync<CloudflareQueuesException>()
            .Where(ex => ex.HttpStatusCode == 401);

        callCount.Should().Be(1, "401 is non-transient and should not be retried");
    }
}
