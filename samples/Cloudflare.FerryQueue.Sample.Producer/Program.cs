using Cloudflare.FerryQueue.Abstractions;
using Cloudflare.FerryQueue.DependencyInjection;
using Cloudflare.FerryQueue.Models;
using Microsoft.Extensions.Options;

// ──────────────────────────────────────────────────────────────────────────────
//  Cloudflare.FerryQueue — Producer sample application
//
//  A standalone app whose ONLY job is publishing messages to a Cloudflare Queue.
//  It demonstrates:
//    1. Registering the client via DI (same configuration pattern as the
//       original combined sample).
//    2. A configurable publish loop — how many messages to send and how often.
//    3. A producer-only "pause/resume" control, so you can manually pause and
//       resume publishing at runtime to test how a consumer behaves when no
//       new messages are arriving. This pause is purely a local testing knob
//       for THIS app — it has nothing to do with Cloudflare's own queue-level
//       "delivery paused" feature (see Cloudflare.FerryQueue.Sample.Consumer for
//       how the library already handles that transparently).
//
//  Run it, then type commands in the console:
//    pause    → stop publishing new messages
//    resume   → resume publishing
//    status   → print current state (paused? how many sent so far?)
//    quit     → stop the app
// ──────────────────────────────────────────────────────────────────────────────

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

// ── 2. Bind producer-specific settings from the "Producer" section ────────────
builder.Services.Configure<ProducerOptions>(builder.Configuration.GetSection("Producer"));

// ── 3. Shared control state + console command listener + publish loop ─────────
builder.Services.AddSingleton<ProducerControlState>();
builder.Services.AddHostedService<ConsoleControlService>();
builder.Services.AddHostedService<ProducerDemo>();

await builder.Build().RunAsync();

// ──────────────────────────────────────────────────────────────────────────────
//  Domain model (shared shape with the Consumer sample)
// ──────────────────────────────────────────────────────────────────────────────

public sealed class OrderEvent
{
    public int    OrderId    { get; set; }
    public string CustomerId { get; set; } = string.Empty;
    public string Status     { get; set; } = string.Empty;
}

// ──────────────────────────────────────────────────────────────────────────────
//  Producer-only options — how many messages to publish and how often.
// ──────────────────────────────────────────────────────────────────────────────

/// <summary>
/// Configuration for <see cref="ProducerDemo"/>. Bind from the <c>"Producer"</c>
/// section of <c>appsettings.json</c>.
/// </summary>
public sealed class ProducerOptions
{
    /// <summary>
    /// Total number of messages to publish before the producer stops itself.
    /// Set to <c>-1</c> to publish indefinitely until the app is stopped.
    /// Default: <c>50</c>.
    /// </summary>
    public int MessageCount { get; set; } = 50;

    /// <summary>How long to wait between each published message. Default: <c>1000ms</c>.</summary>
    public int PublishIntervalMs { get; set; } = 1_000;
}

// ──────────────────────────────────────────────────────────────────────────────
//  Producer-only pause/resume control — purely a local testing convenience for
//  this sample app. Shared (singleton) between the console listener and the
//  publish loop.
// ──────────────────────────────────────────────────────────────────────────────

public sealed class ProducerControlState
{
    private volatile bool _paused;

    /// <summary><c>true</c> while the operator has paused publishing via the console.</summary>
    public bool IsPaused => _paused;

    /// <summary>Number of messages successfully published so far.</summary>
    public int PublishedCount { get; private set; }

    public void Pause()  => _paused = true;
    public void Resume() => _paused = false;

    public void RecordPublished() => PublishedCount++;
}

// ──────────────────────────────────────────────────────────────────────────────
//  Console command listener — lets you type "pause" / "resume" / "status" /
//  "quit" while the producer is running, to simulate stopping and resuming
//  message production for testing purposes.
// ──────────────────────────────────────────────────────────────────────────────

public sealed class ConsoleControlService(
    ProducerControlState control,
    IHostApplicationLifetime lifetime,
    ILogger<ConsoleControlService> logger)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation(
            "[Producer] Console commands available: pause | resume | status | quit");

        while (!stoppingToken.IsCancellationRequested)
        {
            // Console.ReadLine() has no cancellation support, so run it on a background
            // thread and race it against the stopping token to avoid blocking shutdown.
            var readTask = Task.Run(Console.ReadLine, stoppingToken);
            var completed = await Task.WhenAny(readTask, Task.Delay(Timeout.Infinite, stoppingToken))
                .ConfigureAwait(false);

            if (completed != readTask)
                break; // cancellation requested while waiting for input

            var line = (readTask.Result ?? string.Empty).Trim().ToLowerInvariant();

            switch (line)
            {
                case "pause":
                    control.Pause();
                    logger.LogWarning("[Producer] ⏸ Publishing PAUSED (local test control) — type 'resume' to continue");
                    break;

                case "resume":
                    control.Resume();
                    logger.LogWarning("[Producer] ▶ Publishing RESUMED");
                    break;

                case "status":
                    logger.LogInformation(
                        "[Producer] status: paused={Paused} published={Count}",
                        control.IsPaused, control.PublishedCount);
                    break;

                case "quit" or "exit":
                    logger.LogInformation("[Producer] Shutting down (requested via console) …");
                    lifetime.StopApplication();
                    return;

                case "":
                    break; // ignore blank lines

                default:
                    logger.LogInformation("[Producer] Unknown command '{Line}'. Try: pause | resume | status | quit", line);
                    break;
            }
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
//  Publisher — publishes OrderEvents at a configurable rate, up to a
//  configurable total count, honoring the local pause/resume control.
// ──────────────────────────────────────────────────────────────────────────────

public sealed class ProducerDemo(
    ICloudflareQueuesClient client,
    ProducerControlState control,
    IOptions<ProducerOptions> options,
    IHostApplicationLifetime lifetime,
    ILogger<ProducerDemo> logger)
    : BackgroundService
{
    private const string QueueId = "6dc9e927e8d64191b159b053c5c631d4";
    private readonly ProducerOptions _opts = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation(
            "[Producer] Started — target queue={QueueId} messageCount={Count} interval={Interval}ms",
            QueueId, _opts.MessageCount < 0 ? "unlimited" : _opts.MessageCount.ToString(), _opts.PublishIntervalMs);

        var counter = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            if (control.IsPaused)
            {
                // Producer-only pause: just idle without hitting the API at all.
                await Task.Delay(250, stoppingToken).ConfigureAwait(false);
                continue;
            }

            if (_opts.MessageCount >= 0 && counter >= _opts.MessageCount)
            {
                logger.LogInformation(
                    "[Producer] Reached configured MessageCount={Count} — stopping producer app",
                    _opts.MessageCount);
                lifetime.StopApplication();
                return;
            }

            counter++;

            var order = new OrderEvent
            {
                OrderId    = counter,
                CustomerId = $"cust-{counter % 5:000}",
                Status     = "Placed",
            };

            var result = await client.PublishAsync(QueueId, new OutboundMessage<OrderEvent>
            {
                Body = order,
            }, stoppingToken).ConfigureAwait(false);

            control.RecordPublished();

            logger.LogInformation(
                "[Producer] Published message #{N}/{Total} order={OrderId} customer={CustomerId} success={Ok}",
                counter,
                _opts.MessageCount < 0 ? "∞" : _opts.MessageCount.ToString(),
                order.OrderId, order.CustomerId, result.Success);

            await Task.Delay(_opts.PublishIntervalMs, stoppingToken).ConfigureAwait(false);
        }
    }
}
