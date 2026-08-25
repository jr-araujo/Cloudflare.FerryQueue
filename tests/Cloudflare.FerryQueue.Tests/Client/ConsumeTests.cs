using System.Net;
using Cloudflare.FerryQueue.Exceptions;
using Cloudflare.FerryQueue.Models;
using FluentAssertions;
using RichardSzalay.MockHttp;
using Xunit;

namespace Cloudflare.FerryQueue.Tests.Client;

public sealed class ConsumeTests
{
    private const string PullResponse = """
        {
          "success": true,
          "errors": [],
          "messages": [],
          "result": {
            "messages": [
              {
                "lease_id": "lease-aaa",
                "id":       "msg-001",
                "body":     "{\"order_id\": 1, \"customer_id\": \"cust-a\"}",
                "timestamp_ms": 1700000000000,
                "attempts": 1
              },
              {
                "lease_id": "lease-bbb",
                "id":       "msg-002",
                "body":     "{\"order_id\": 77, \"customer_id\": \"cust-x\"}",
                "timestamp_ms": 1700000001000,
                "attempts": 2
              }
            ]
          }
        }
        """;

    // ── PullAsync (raw) ───────────────────────────────────────────────────────

    [Fact]
    public async Task PullAsync_ReturnsTwoMessages_WhenQueueHasMessages()
    {
        var (client, mockHttp) = TestFactory.CreateClient();
        mockHttp
            .When(HttpMethod.Post, TestFactory.QueueUrl("messages/pull"))
            .Respond(HttpStatusCode.OK, "application/json", PullResponse);

        var result = await client.PullAsync(TestFactory.QueueId, TestFactory.ConsumerId);

        result.Success.Should().BeTrue();
        result.Messages.Should().HaveCount(2);
        result.Messages[0].LeaseId.Should().Be("lease-aaa");
        result.Messages[0].Id.Should().Be("msg-001");
        result.Messages[0].Attempts.Should().Be(1);
        result.Messages[1].EnqueuedAt.Year.Should().Be(2023);
    }

    [Fact]
    public async Task PullAsync_ReturnsEmptyList_WhenQueueIsEmpty()
    {
        var (client, mockHttp) = TestFactory.CreateClient();
        mockHttp
            .When(HttpMethod.Post, TestFactory.QueueUrl("messages/pull"))
            .Respond(HttpStatusCode.OK, "application/json",
                """{ "success": true, "errors": [], "messages": [], "result": { "messages": [] } }""");

        var result = await client.PullAsync(TestFactory.QueueId, TestFactory.ConsumerId);

        result.Success.Should().BeTrue();
        result.Messages.Should().BeEmpty();
    }

    [Fact]
    public async Task PullAsync_ThrowsCloudflareQueuesException_OnApiError()
    {
        var (client, mockHttp) = TestFactory.CreateClient();
        mockHttp
            .When(HttpMethod.Post, TestFactory.QueueUrl("messages/pull"))
            .Respond(HttpStatusCode.Unauthorized, "application/json",
                TestFactory.ErrorEnvelope(10000, "Authentication error"));

        var act = () => client.PullAsync(TestFactory.QueueId, TestFactory.ConsumerId);

        await act.Should().ThrowAsync<CloudflareQueuesException>()
            .Where(ex => ex.HttpStatusCode == 401);
    }

    [Fact]
    public async Task PullAsync_ReturnsEmptySuccessResultWithIsPaused_WhenDeliveryIsPaused()
    {
        var (client, mockHttp) = TestFactory.CreateClient();
        mockHttp
            .When(HttpMethod.Post, TestFactory.QueueUrl("messages/pull"))
            .Respond(HttpStatusCode.OK, "application/json",
                TestFactory.ErrorEnvelope(0, "messages cannot be pulled because delivery is paused"));

        // Should NOT throw — a paused queue is surfaced as a normal empty result.
        var result = await client.PullAsync(TestFactory.QueueId, TestFactory.ConsumerId);

        result.Success.Should().BeTrue();
        result.Messages.Should().BeEmpty();
        result.IsPaused.Should().BeTrue();
    }

    // ── PullAsync<T> (typed) ──────────────────────────────────────────────────

    [Fact]
    public async Task PullAsync_Typed_DeserializesObjectBody()
    {
        var (client, mockHttp) = TestFactory.CreateClient();
        mockHttp
            .When(HttpMethod.Post, TestFactory.QueueUrl("messages/pull"))
            .Respond(HttpStatusCode.OK, "application/json", PullResponse);

        var result = await client.PullAsync<OrderPlaced>(TestFactory.QueueId, TestFactory.ConsumerId);

        result.Success.Should().BeTrue();
        result.Messages.Should().HaveCount(2);

        // Second message has an object body matching OrderPlaced
        var second = result.Messages[1];
        second.Body.Should().NotBeNull();
        second.Body!.OrderId.Should().Be(77);
        second.Body.CustomerId.Should().Be("cust-x");
    }

    // ── AcknowledgeAsync ──────────────────────────────────────────────────────

    [Fact]
    public async Task AcknowledgeAsync_Single_CallsAckEndpoint()
    {
        var (client, mockHttp) = TestFactory.CreateClient();
        var handler = mockHttp
            .When(HttpMethod.Post, TestFactory.QueueUrl("messages/ack"))
            .Respond(HttpStatusCode.OK, "application/json", TestFactory.OkEnvelope());

        await client.AcknowledgeAsync(TestFactory.QueueId, TestFactory.ConsumerId, "lease-aaa");

        mockHttp.GetMatchCount(handler).Should().Be(1);
    }

    [Fact]
    public async Task RetryAsync_Single_CallsAckEndpointWithRetry()
    {
        var (client, mockHttp) = TestFactory.CreateClient();
        var handler = mockHttp
            .When(HttpMethod.Post, TestFactory.QueueUrl("messages/ack"))
            .Respond(HttpStatusCode.OK, "application/json", TestFactory.OkEnvelope());

        await client.RetryAsync(TestFactory.QueueId, TestFactory.ConsumerId, "lease-bbb");

        mockHttp.GetMatchCount(handler).Should().Be(1);
    }

    [Fact]
    public async Task AcknowledgeAsync_Batch_SendsBothAcksAndRetries()
    {
        var (client, mockHttp) = TestFactory.CreateClient();
        var handler = mockHttp
            .When(HttpMethod.Post, TestFactory.QueueUrl("messages/ack"))
            .Respond(HttpStatusCode.OK, "application/json", TestFactory.OkEnvelope());

        await client.AcknowledgeAsync(
            TestFactory.QueueId,
            TestFactory.ConsumerId,
            new AckBatch
            {
                Acks    = ["lease-aaa", "lease-bbb"],
                Retries = ["lease-ccc"],
            });

        mockHttp.GetMatchCount(handler).Should().Be(1);
    }
}
