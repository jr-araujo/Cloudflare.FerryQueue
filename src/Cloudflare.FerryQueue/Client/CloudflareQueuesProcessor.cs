using Cloudflare.FerryQueue.Abstractions;
using Cloudflare.FerryQueue.Exceptions;
using Cloudflare.FerryQueue.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Cloudflare.FerryQueue.Client;

/// <summary>
/// Options for <see cref="CloudflareQueuesProcessor{TMessage}"/>.
/// </summary>
public sealed class ProcessorOptions
{
    /// <summary>ID of the Cloudflare Queue to consume from. <b>Required.</b></summary>
    public string QueueId { get; set; } = string.Empty;

    /// <summary>ID of the Pull Consumer. <b>Required.</b></summary>
    public string ConsumerId { get; set; } = string.Empty;

    /// <summary>Messages pulled per cycle. Default: <c>10</c>. Max: <c>100</c>.</summary>
    public int BatchSize { get; set; } = 10;

    /// <summary>
    /// How long pulled messages remain invisible before reappearing.
    /// Should be longer than your processing time. Default: <c>30 000 ms</c>.
    /// </summary>
    public int VisibilityTimeoutMs { get; set; } = 30_000;

    /// <summary>
    /// How long to wait between polling cycles when the queue is empty.
    /// This is the <b>initial</b> delay — it grows via exponential back-off
    /// (see <see cref="EmptyQueueBackoffMultiplier"/> and <see cref="MaxEmptyQueueDelay"/>)
    /// the longer the queue stays empty, to minimize the number of billed API calls
    /// made while there is no work to do. Default: <c>2s</c>.
    /// </summary>
    public TimeSpan EmptyQueueDelay { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Upper bound for the empty-queue back-off delay. Default: <c>60s</c>.
    /// </summary>
    public TimeSpan MaxEmptyQueueDelay { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Multiplier applied to the current empty-queue delay after each consecutive
    /// empty pull (e.g. <c>2.0</c> doubles the delay each time), up to <see cref="MaxEmptyQueueDelay"/>.
    /// Default: <c>2.0</c>.
    /// </summary>
    public double EmptyQueueBackoffMultiplier { get; set; } = 2.0;

    /// <summary>
    /// How long to wait after an unexpected error before retrying the loop.
    /// Default: <c>5s</c>.
    /// </summary>
    public TimeSpan ErrorDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Maximum number of concurrent message handlers within a single batch.
    /// Default: <c>1</c> (sequential). Increase for I/O-bound consumers.
    /// </summary>
    public int MaxConcurrentMessages { get; set; } = 1;

    internal void EnsureValid()
    {
        if (string.IsNullOrWhiteSpace(QueueId))
            throw new InvalidOperationException($"[CloudflareQueues] {nameof(QueueId)} must not be empty.");
        if (string.IsNullOrWhiteSpace(ConsumerId))
            throw new InvalidOperationException($"[CloudflareQueues] {nameof(ConsumerId)} must not be empty.");
        if (BatchSize is < 1 or > 100)
            throw new InvalidOperationException($"[CloudflareQueues] {nameof(BatchSize)} must be between 1 and 100.");
        if (MaxConcurrentMessages < 1)
            throw new InvalidOperationException($"[CloudflareQueues] {nameof(MaxConcurrentMessages)} must be ≥ 1.");
        if (MaxEmptyQueueDelay < EmptyQueueDelay)
            throw new InvalidOperationException(
                $"[CloudflareQueues] {nameof(MaxEmptyQueueDelay)} must be ≥ {nameof(EmptyQueueDelay)}.");
        if (EmptyQueueBackoffMultiplier < 1.0)
            throw new InvalidOperationException(
                $"[CloudflareQueues] {nameof(EmptyQueueBackoffMultiplier)} must be ≥ 1.0.");
    }
}

/// <summary>
/// A <see cref="BackgroundService"/> that continuously polls a Cloudflare Queue
/// and dispatches messages to a registered <see cref="ICloudflareQueueConsumer{TMessage}"/>.
/// </summary>
/// <typeparam name="TMessage">The message body type.</typeparam>
public sealed class CloudflareQueuesProcessor<TMessage> : BackgroundService
{
    private readonly ICloudflareQueuesClient _client;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ProcessorOptions _processorOpts;
    private readonly ILogger<CloudflareQueuesProcessor<TMessage>> _logger;

    // Tracks the current empty-queue back-off delay, growing exponentially while the
    // queue stays empty and resetting to ProcessorOptions.EmptyQueueDelay as soon as
    // messages are found again. This is what keeps idle polling cheap.
    private TimeSpan _currentEmptyDelay;

    /// <summary>
    /// Initializes a new instance of the <see cref="CloudflareQueuesProcessor{TMessage}"/> class.
    /// </summary>
    /// <param name="client">The Cloudflare Queues client used to pull and acknowledge messages.</param>
    /// <param name="scopeFactory">Factory used to create a DI scope per processing cycle.</param>
    /// <param name="processorOpts">Options controlling processor behavior.</param>
    /// <param name="logger">The logger instance.</param>
    public CloudflareQueuesProcessor(
        ICloudflareQueuesClient client,
        IServiceScopeFactory scopeFactory,
        ProcessorOptions processorOpts,
        ILogger<CloudflareQueuesProcessor<TMessage>> logger)
    {
        _client       = client;
        _scopeFactory = scopeFactory;
        _processorOpts = processorOpts;
        _logger       = logger;

        _processorOpts.EnsureValid();
        _currentEmptyDelay = _processorOpts.EmptyQueueDelay;
    }

    /// <summary>
    /// Runs the background polling loop that pulls, dispatches, and acknowledges messages
    /// until cancellation is requested.
    /// </summary>
    /// <param name="stoppingToken">Token signaled when the host is shutting down.</param>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "[CloudflareQueues] Processor started — queue={QueueId} consumer={ConsumerId} " +
            "batchSize={BatchSize} concurrency={Concurrency}",
            _processorOpts.QueueId, _processorOpts.ConsumerId,
            _processorOpts.BatchSize, _processorOpts.MaxConcurrentMessages);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ProcessOneCycleAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break; // clean shutdown
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "[CloudflareQueues] Unexpected error in processor loop — pausing {Delay} before retry",
                    _processorOpts.ErrorDelay);

                await Task.Delay(_processorOpts.ErrorDelay, stoppingToken).ConfigureAwait(false);
            }
        }

        _logger.LogInformation(
            "[CloudflareQueues] Processor stopped — queue={QueueId}", _processorOpts.QueueId);
    }

    private async Task ProcessOneCycleAsync(CancellationToken ct)
    {
        PullResult<TMessage> pulled;

        try
        {
            pulled = await _client.PullAsync<TMessage>(
                _processorOpts.QueueId,
                _processorOpts.ConsumerId,
                new PullOptions
                {
                    BatchSize          = _processorOpts.BatchSize,
                    VisibilityTimeoutMs = _processorOpts.VisibilityTimeoutMs,
                },
                ct).ConfigureAwait(false);
        }
        catch (CloudflareQueuesException ex)
        {
            _logger.LogError(ex,
                "[CloudflareQueues] Pull failed for queue={QueueId} — pausing {Delay}",
                _processorOpts.QueueId, _processorOpts.ErrorDelay);

            await Task.Delay(_processorOpts.ErrorDelay, ct).ConfigureAwait(false);
            return;
        }

        if (!pulled.Success || pulled.Messages.Count == 0)
        {
            if (pulled.IsPaused)
            {
                _logger.LogDebug(
                    "[CloudflareQueues] Queue={QueueId} delivery paused — waiting {Delay} before checking again",
                    _processorOpts.QueueId, _currentEmptyDelay);
            }

            // Nothing to do — back off exponentially so we hit the (billed) pull endpoint
            // less and less often the longer the queue stays idle/paused, instead of
            // hammering it at a fixed interval. Applies equally whether the queue is
            // genuinely empty or delivery is paused — in both cases we just keep
            // listening quietly until there's something to do again.
            await Task.Delay(_currentEmptyDelay, ct).ConfigureAwait(false);

            var next = TimeSpan.FromMilliseconds(
                _currentEmptyDelay.TotalMilliseconds * _processorOpts.EmptyQueueBackoffMultiplier);
            _currentEmptyDelay = next > _processorOpts.MaxEmptyQueueDelay
                ? _processorOpts.MaxEmptyQueueDelay
                : next;

            return;
        }

        // Messages found — reset back-off so the next empty pull starts fresh again.
        _currentEmptyDelay = _processorOpts.EmptyQueueDelay;

        _logger.LogDebug(
            "[CloudflareQueues] Pulled {Count} messages (backlog≈{Backlog}) from queue={QueueId}",
            pulled.Messages.Count, pulled.BacklogCount, _processorOpts.QueueId);

        var acks    = new System.Collections.Concurrent.ConcurrentBag<string>();
        var retries = new System.Collections.Concurrent.ConcurrentBag<string>();

        // Process with configurable concurrency
        var semaphore = new SemaphoreSlim(_processorOpts.MaxConcurrentMessages, _processorOpts.MaxConcurrentMessages);
        var tasks = pulled.Messages.Select(async msg =>
        {
            await semaphore.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                bool success = await DispatchMessageAsync(msg, ct).ConfigureAwait(false);
                (success ? acks : retries).Add(msg.LeaseId);
            }
            finally
            {
                semaphore.Release();
            }
        });

        await Task.WhenAll(tasks).ConfigureAwait(false);

        // Batch ack/retry
        if (acks.Count > 0 || retries.Count > 0)
        {
            try
            {
                await _client.AcknowledgeAsync(
                    _processorOpts.QueueId,
                    _processorOpts.ConsumerId,
                    new AckBatch { Acks = [.. acks], Retries = [.. retries] },
                    ct).ConfigureAwait(false);

                _logger.LogDebug(
                    "[CloudflareQueues] Acked {AckCount}, retried {RetryCount} for queue={QueueId}",
                    acks.Count, retries.Count, _processorOpts.QueueId);
            }
            catch (CloudflareQueuesException ex)
            {
                _logger.LogError(ex,
                    "[CloudflareQueues] Ack/retry call failed for queue={QueueId} — messages will redeliver after visibility timeout",
                    _processorOpts.QueueId);
            }
        }
    }

    private async Task<bool> DispatchMessageAsync(InboundMessage<TMessage> message, CancellationToken ct)
    {
        // Resolve a scoped consumer so it can depend on scoped services (e.g. DbContext)
        await using var scope = _scopeFactory.CreateAsyncScope();
        var consumer = scope.ServiceProvider.GetRequiredService<ICloudflareQueueConsumer<TMessage>>();

        try
        {
            return await consumer.ConsumeAsync(message, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "[CloudflareQueues] Consumer threw on message id={Id} — scheduling retry",
                message.Id);
            return false;
        }
    }
}
