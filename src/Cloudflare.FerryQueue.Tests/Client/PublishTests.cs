using System.Net;
using Cloudflare.FerryQueue.Exceptions;
using Cloudflare.FerryQueue.Models;
using FluentAssertions;
using RichardSzalay.MockHttp;
using Xunit;

namespace Cloudflare.FerryQueue.Tests.Client;

public sealed class PublishTests
{
    // ── PublishAsync (raw) ────────────────────────────────────────────────────

    [Fact]
    public async Task PublishAsync_ReturnsSuccess_OnOkResponse()
    {
        var (client, mockHttp) = TestFactory.CreateClient();
        mockHttp
            .When(HttpMethod.Post, TestFactory.QueueUrl("messages"))
            .Respond(HttpStatusCode.OK, "application/json", TestFactory.OkEnvelope());

        var result = await client.PublishAsync(TestFactory.QueueId, new OutboundMessage { Body = "hello" });

        result.Success.Should().BeTrue();
        result.Errors.Should().BeEmpty();
    }

    [Fact]
    public async Task PublishAsync_ThrowsCloudflareQueuesException_OnApiError()
    {
        var (client, mockHttp) = TestFactory.CreateClient();
        mockHttp
            .When(HttpMethod.Post, TestFactory.QueueUrl("messages"))
            .Respond(HttpStatusCode.Forbidden, "application/json",
                TestFactory.ErrorEnvelope(10001, "Missing permission"));

        var act = () => client.PublishAsync(TestFactory.QueueId, new OutboundMessage { Body = "x" });

        await act.Should().ThrowAsync<CloudflareQueuesException>()
            .Where(ex => ex.HttpStatusCode == 403 && ex.ApiErrors.Count == 1);
    }

    [Fact]
    public async Task PublishAsync_ThrowsArgumentException_WhenQueueIdEmpty()
    {
        var (client, _) = TestFactory.CreateClient();

        var act = () => client.PublishAsync("", new OutboundMessage { Body = "x" });

        await act.Should().ThrowAsync<ArgumentException>();
    }

    // ── PublishAsync<T> (typed) ───────────────────────────────────────────────

    [Fact]
    public async Task PublishAsync_Typed_SerializesBodyAsJson()
    {
        var (client, mockHttp) = TestFactory.CreateClient();
        var handler = mockHttp
            .When(HttpMethod.Post, TestFactory.QueueUrl("messages"))
            .Respond(HttpStatusCode.OK, "application/json", TestFactory.OkEnvelope());

        var payload = new OrderPlaced { OrderId = 42, CustomerId = "cust-7" };
        var result  = await client.PublishAsync(TestFactory.QueueId,
            new OutboundMessage<OrderPlaced> { Body = payload });

        result.Success.Should().BeTrue();
        mockHttp.GetMatchCount(handler).Should().Be(1);
    }

    [Fact]
    public async Task PublishAsync_Typed_WithDelay_PassesDelaySeconds()
    {
        var (client, mockHttp) = TestFactory.CreateClient();
        mockHttp
            .When(HttpMethod.Post, TestFactory.QueueUrl("messages"))
            .Respond(HttpStatusCode.OK, "application/json", TestFactory.OkEnvelope());

        var result = await client.PublishAsync(TestFactory.QueueId,
            new OutboundMessage<string> { Body = "delayed", DelaySeconds = 60 });

        result.Success.Should().BeTrue();
    }

    // ── PublishBatchAsync ─────────────────────────────────────────────────────

    [Fact]
    public async Task PublishBatchAsync_ReturnsSuccess_ForValidBatch()
    {
        var (client, mockHttp) = TestFactory.CreateClient();
        mockHttp
            .When(HttpMethod.Post, TestFactory.QueueUrl("messages/batch"))
            .Respond(HttpStatusCode.OK, "application/json", TestFactory.OkEnvelope());

        var msgs = Enumerable.Range(1, 5)
            .Select(i => new OutboundMessage { Body = $"msg-{i}" });

        var result = await client.PublishBatchAsync(TestFactory.QueueId, msgs);

        result.Success.Should().BeTrue();
    }

    [Fact]
    public async Task PublishBatchAsync_ThrowsArgumentException_WhenOver100Messages()
    {
        var (client, _) = TestFactory.CreateClient();
        var msgs = Enumerable.Range(0, 101).Select(_ => new OutboundMessage { Body = "x" });

        var act = () => client.PublishBatchAsync(TestFactory.QueueId, msgs);

        await act.Should().ThrowAsync<ArgumentException>()
            .WithMessage("*100*");
    }

    [Fact]
    public async Task PublishBatchAsync_Typed_SerializesAllBodies()
    {
        var (client, mockHttp) = TestFactory.CreateClient();
        mockHttp
            .When(HttpMethod.Post, TestFactory.QueueUrl("messages/batch"))
            .Respond(HttpStatusCode.OK, "application/json", TestFactory.OkEnvelope());

        var msgs = new[]
        {
            new OutboundMessage<OrderPlaced> { Body = new OrderPlaced { OrderId = 1, CustomerId = "c1" } },
            new OutboundMessage<OrderPlaced> { Body = new OrderPlaced { OrderId = 2, CustomerId = "c2" }, DelaySeconds = 5 },
        };

        var result = await client.PublishBatchAsync(TestFactory.QueueId, msgs);
        result.Success.Should().BeTrue();
    }
}

// ── Shared test model ────────────────────────────────────────────────────────

public sealed class OrderPlaced
{
    public int    OrderId    { get; set; }
    public string CustomerId { get; set; } = string.Empty;
}
