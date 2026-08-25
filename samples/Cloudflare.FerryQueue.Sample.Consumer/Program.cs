using Cloudflare.FerryQueue.Abstractions;
using Cloudflare.FerryQueue.DependencyInjection;
using Cloudflare.FerryQueue.Models;

// ──────────────────────────────────────────────────────────────────────────────
//  Cloudflare.FerryQueue — Consumer sample application
//
//  A standalone app whose ONLY job is consuming messages from a Cloudflare
//  Queue via the background processor (ICloudflareQueueConsumer<T>).
//
//  This is the same processor configuration used in the original combined
//  sample, including the empty-queue back-off settings that minimize billed
//  API calls while idle — the processor also automatically treats a paused
//  queue (Cloudflare-side "delivery paused") the same way, without throwing.
//
//  Run this alongside Cloudflare.FerryQueue.Sample.Producer to see end-to-end
//  publish → consume flow, or run it standalone against messages published
//  by any other means.
// ──────────────────────────────────────────────────────────────────────────────

const string QueueId    = "6dc9e927e8d64191b159b053c5c631d4";
const string ConsumerId = "DotNet_Sample_Local_VS";

var builder = Host.CreateApplicationBuilder(args);

// ── 1. Register the Cloudflare Queues client ──────────────────────────────────
//
// Option A: bind from appsettings.json  (recommended for production)
builder.Services.AddCloudflareQueues(builder.Configuration);

// Option B: configure inline
// builder.Services.AddCloudflareQueues(opts =>
// {
//     opts.AccountId    = "your-account-id";
//     opts.ApiToken     = "your-api-token";
//     opts.MaxRetries   = 3;
// });

// ── 2. Register a background processor for OrderEvent ─────────────────────────
//
// The processor pulls messages in a loop and dispatches to OrderEventConsumer.
// The consumer is resolved as SCOPED, so it can depend on EF Core DbContext etc.
builder.Services.AddCloudflareQueuesProcessor<OrderEvent, OrderEventConsumer>(opts =>
{
    opts.QueueId               = QueueId;
    opts.ConsumerId            = ConsumerId;
    opts.BatchSize             = 10;
    opts.VisibilityTimeoutMs   = 30_000;
    opts.MaxConcurrentMessages = 4;          // process up to 4 messages in parallel

    // ── Empty-queue back-off (reduces billed API calls while there's no work) ──
    // Starts at EmptyQueueDelay and doubles (EmptyQueueBackoffMultiplier) after each
    // consecutive empty/paused pull, up to MaxEmptyQueueDelay. Resets back to
    // EmptyQueueDelay the moment messages are found again.
    // e.g. 2s → 4s → 8s → 16s → 32s → 60s (capped)
    opts.EmptyQueueDelay             = TimeSpan.FromSeconds(2);
    opts.MaxEmptyQueueDelay          = TimeSpan.FromSeconds(60);
    opts.EmptyQueueBackoffMultiplier = 2.0;
});

await builder.Build().RunAsync();

// ──────────────────────────────────────────────────────────────────────────────
//  Domain model (shared shape with the Producer sample)
// ──────────────────────────────────────────────────────────────────────────────

public sealed class OrderEvent
{
    public int    OrderId    { get; set; }
    public string CustomerId { get; set; } = string.Empty;
    public string Status     { get; set; } = string.Empty;
}

// ──────────────────────────────────────────────────────────────────────────────
//  Consumer — called by the background processor for each pulled message
// ──────────────────────────────────────────────────────────────────────────────

public sealed class OrderEventConsumer(ILogger<OrderEventConsumer> logger)
    : ICloudflareQueueConsumer<OrderEvent>
{
    public Task<bool> ConsumeAsync(InboundMessage<OrderEvent> message, CancellationToken cancellationToken)
    {
        if (message.Body is null)
        {
            logger.LogWarning("Received null body for message {Id} — retrying", message.Id);
            return Task.FromResult(false); // retry
        }

        logger.LogInformation(
            "Processing order #{OrderId} status={Status} customer={CustomerId} attempt={Attempt}",
            message.Body.OrderId, message.Body.Status, message.Body.CustomerId, message.Attempts);

        // Implement your business logic here.
        // Return true  → message is acknowledged (done).
        // Return false → message is retried (reappears after visibility timeout).
        // Throw        → also triggers a retry (processor handles it safely).

        return Task.FromResult(true);
    }
}
