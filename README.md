# Cloudflare.FerryQueue

A production-ready **.NET 10** client library for [Cloudflare Queues](https://developers.cloudflare.com/queues/).  
Supports **publish**, **batch publish**, **pull consume**, **ack/retry**, typed messages,
DI integration, and background processing out of the box.

[![NuGet](https://img.shields.io/nuget/v/Cloudflare.FerryQueue.svg)](https://www.nuget.org/packages/Cloudflare.FerryQueue)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![CI](https://github.com/jr-araujo/Cloudflare.FerryQueue/actions/workflows/ci.yml/badge.svg)](https://github.com/jr-araujo/Cloudflare.FerryQueue/actions)

---

## Installation

```bash
dotnet add package Cloudflare.FerryQueue
```

---

## Quick start

### 1. Register services

**From `appsettings.json`** (recommended):

```json
{
  "CloudflareQueues": {
    "AccountId": "YOUR_ACCOUNT_ID",
    "ApiToken":  "YOUR_API_TOKEN"
  }
}
```

```csharp
builder.Services.AddCloudflareQueues(builder.Configuration);
```

**Or inline:**

```csharp
builder.Services.AddCloudflareQueues(opts =>
{
    opts.AccountId = "YOUR_ACCOUNT_ID";
    opts.ApiToken  = "YOUR_API_TOKEN";
});
```

---

### 2. Publish messages

```csharp
public class OrderService(ICloudflareQueuesClient client)
{
    // Single typed message
    public async Task PublishOrderAsync(Order order)
    {
        var result = await client.PublishAsync("my-queue-id", new OutboundMessage<Order>
        {
            Body         = order,
            DelaySeconds = 5,   // optional — deliver after 5 s
        });
    }

    // Batch (up to 100 messages per call)
    public async Task PublishBatchAsync(IEnumerable<Order> orders)
    {
        var result = await client.PublishBatchAsync("my-queue-id",
            orders.Select(o => new OutboundMessage<Order> { Body = o }));
    }
}
```

---

### 3. Consume with the background processor (recommended)

Implement `ICloudflareQueueConsumer<T>`:

```csharp
public class OrderConsumer(ILogger<OrderConsumer> logger, IOrderRepository repo)
    : ICloudflareQueueConsumer<Order>
{
    public async Task<bool> ConsumeAsync(InboundMessage<Order> message, CancellationToken ct)
    {
        if (message.Body is null) return false;   // false = retry

        await repo.SaveAsync(message.Body, ct);
        logger.LogInformation("Order {Id} processed (attempt {N})",
            message.Body.Id, message.Attempts);

        return true;   // true = ack
    }
}
```

Register the processor:

```csharp
builder.Services
    .AddCloudflareQueues(builder.Configuration)
    .AddCloudflareQueuesProcessor<Order, OrderConsumer>(opts =>
    {
        opts.QueueId               = "my-queue-id";
        opts.ConsumerId            = "my-consumer-id";
        opts.BatchSize             = 20;
        opts.MaxConcurrentMessages = 4;   // parallel handlers per batch
        opts.EmptyQueueDelay       = TimeSpan.FromSeconds(2);
    });
```

> **Tip:** `OrderConsumer` is resolved as **scoped** per message, so it can safely inject
> scoped dependencies like `DbContext` or `IRepository`.

---

### 4. Manual pull / ack / retry

```csharp
// Pull
var result = await client.PullAsync<Order>("queue-id", "consumer-id",
    new PullOptions { BatchSize = 10, VisibilityTimeoutMs = 30_000 });

// Process
var acks    = new List<string>();
var retries = new List<string>();

foreach (var msg in result.Messages)
{
    bool ok = await ProcessAsync(msg.Body!);
    (ok ? acks : retries).Add(msg.LeaseId);
}

// Ack / retry in one call
await client.AcknowledgeAsync("queue-id", "consumer-id",
    new AckBatch { Acks = acks, Retries = retries });

// Or one at a time
await client.AcknowledgeAsync("queue-id", "consumer-id", leaseId);
await client.RetryAsync("queue-id", "consumer-id", leaseId);
```

---

## Configuration reference

| Property         | Default   | Description                                          |
|------------------|-----------|------------------------------------------------------|
| `AccountId`      | required  | Cloudflare Account ID                                |
| `ApiToken`       | required  | API Token with *Workers Queues: Edit* permission     |
| `BaseUrl`        | CF API    | Override for testing/proxies                         |
| `Timeout`        | 30 s      | HTTP request timeout                                 |
| `MaxRetries`     | 3         | Retry attempts on transient errors (429, 5xx)        |
| `RetryDelay`     | 250 ms    | Base delay for exponential back-off                  |
| `MaxRetryDelay`  | 10 s      | Maximum delay cap                                    |

### Processor options

| Property                       | Default | Description                                                          |
|---------------------------------|---------|------------------------------------------------------------------------|
| `QueueId`                      | required| Queue to consume from                                                |
| `ConsumerId`                   | required| Pull Consumer ID                                                    |
| `BatchSize`                    | 10      | Messages per pull cycle (1–100)                                     |
| `VisibilityTimeoutMs`          | 30 000  | How long messages stay invisible after pull                         |
| `EmptyQueueDelay`              | 2 s     | Initial wait between polls when the queue is empty/paused            |
| `MaxEmptyQueueDelay`           | 60 s    | Cap for the empty-queue exponential back-off                        |
| `EmptyQueueBackoffMultiplier`  | 2.0     | Growth rate applied to the delay after each empty/paused pull        |
| `ErrorDelay`                   | 5 s     | Wait after unexpected errors                                        |
| `MaxConcurrentMessages`        | 1       | Parallel message handlers within a batch                            |

> **Cost-aware polling:** the processor backs off exponentially while the queue is empty
> (e.g. `2s → 4s → 8s → 16s → 32s → 60s` capped), instead of polling at a fixed interval —
> this significantly reduces billed Cloudflare API calls during idle periods, and resets
> back to `EmptyQueueDelay` the moment messages are found again.
>
> **Paused queues:** if a queue's delivery is paused (e.g. via the Cloudflare dashboard),
> `PullAsync` does **not** throw — it returns a normal empty result with `IsPaused = true`,
> and the processor keeps listening on the same back-off cadence until delivery resumes.

---

## Samples

Two standalone sample apps under `samples/` demonstrate a realistic producer/consumer split:

- **`Cloudflare.FerryQueue.Sample.Producer`** — publishes `OrderEvent` messages at a configurable
  rate and count (`Producer:MessageCount`, `Producer:PublishIntervalMs` in `appsettings.json`).
  Supports local console commands (`pause`, `resume`, `status`, `quit`) purely as a manual
  testing convenience for simulating start/stop publishing — unrelated to Cloudflare's own
  queue-pause feature.
- **`Cloudflare.FerryQueue.Sample.Consumer`** — registers `AddCloudflareQueuesProcessor<TMessage, TConsumer>`
  and consumes messages in the background, including the cost-aware back-off settings above.

```bash
# Terminal 1
dotnet run --project samples/Cloudflare.FerryQueue.Sample.Producer

# Terminal 2
dotnet run --project samples/Cloudflare.FerryQueue.Sample.Consumer
```

Both sample apps read `CloudflareQueues:AccountId` / `ApiToken` from `appsettings.json`
(placeholders committed to the repo) — set your real credentials via
[.NET User Secrets](https://learn.microsoft.com/aspnet/core/security/app-secrets) instead of
editing that file directly:

```bash
dotnet user-secrets set "CloudflareQueues:AccountId" "your-account-id" --project samples/Cloudflare.FerryQueue.Sample.Producer
dotnet user-secrets set "CloudflareQueues:ApiToken"  "your-api-token"  --project samples/Cloudflare.FerryQueue.Sample.Producer
```

---

## Error handling

All API errors throw `CloudflareQueuesException`:

```csharp
try
{
    await client.PublishAsync(queueId, message);
}
catch (CloudflareQueuesException ex)
{
    Console.WriteLine($"HTTP {ex.HttpStatusCode}");
    foreach (var e in ex.ApiErrors)
        Console.WriteLine($"  [{e.Code}] {e.Message}");
}
```

Transient errors (429, 500–504, network failures) are automatically retried with
exponential back-off up to `MaxRetries`.

---

## Building & publishing

```bash
# Build
dotnet build

# Test
dotnet test

# Pack
dotnet pack src/Cloudflare.FerryQueue -c Release -o ./nupkg

# Publish
dotnet nuget push ./nupkg/*.nupkg \
  --api-key YOUR_NUGET_TOKEN \
  --source https://api.nuget.org/v3/index.json
```

---

## Architecture

```
src/Cloudflare.FerryQueue/
├── Abstractions/
│   ├── ICloudflareQueuesClient.cs    ← Main client interface
│   └── ICloudflareQueueConsumer.cs  ← Consumer interface
├── Client/
│   ├── CloudflareQueuesClient.cs    ← HTTP implementation + retry
│   └── CloudflareQueuesProcessor.cs ← BackgroundService processor
├── Configuration/
│   └── CloudflareQueuesOptions.cs
├── DependencyInjection/
│   └── ServiceCollectionExtensions.cs
├── Exceptions/
│   └── CloudflareQueuesException.cs
├── Models/
│   └── Models.cs                    ← All domain types
├── Retry/
│   └── RetryPolicy.cs               ← Exponential back-off
└── Serialization/
    └── ApiDtos.cs                   ← Internal JSON DTOs
```

---

## License

MIT — see [LICENSE](LICENSE).
