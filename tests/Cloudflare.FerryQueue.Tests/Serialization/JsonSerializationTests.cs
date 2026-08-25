using System.Text.Json;
using Cloudflare.FerryQueue.Client;
using FluentAssertions;
using Xunit;

namespace Cloudflare.FerryQueue.Tests.Serialization;

public sealed class JsonSerializationTests
{
    [Fact]
    public void JsonOpts_UsesSnakeCaseLower()
    {
        var obj = new { OrderId = 1, CustomerName = "Alice" };
        var json = JsonSerializer.Serialize(obj, CloudflareQueuesClient.JsonOpts);

        json.Should().Contain("order_id");
        json.Should().Contain("customer_name");
        json.Should().NotContain("OrderId");
    }

    [Fact]
    public void JsonOpts_OmitsNullProperties()
    {
        var obj = new TestModel { Name = "test", Value = null };
        var json = JsonSerializer.Serialize(obj, CloudflareQueuesClient.JsonOpts);

        json.Should().NotContain("value");
    }

    [Fact]
    public void JsonOpts_RoundTripsComplexObject()
    {
        var original = new OrderPlacedModel { OrderId = 99, Items = ["item-a", "item-b"] };
        var json     = JsonSerializer.Serialize(original, CloudflareQueuesClient.JsonOpts);
        var restored = JsonSerializer.Deserialize<OrderPlacedModel>(json, CloudflareQueuesClient.JsonOpts);

        restored.Should().NotBeNull();
        restored!.OrderId.Should().Be(99);
        restored.Items.Should().BeEquivalentTo("item-a", "item-b");
    }
}

file sealed class TestModel
{
    public string Name  { get; set; } = string.Empty;
    public string? Value { get; set; }
}

file sealed class OrderPlacedModel
{
    public int           OrderId { get; set; }
    public List<string>  Items   { get; set; } = [];
}
