using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Cloudflare.FerryQueue.Abstractions;
using Cloudflare.FerryQueue.Configuration;
using Cloudflare.FerryQueue.Exceptions;
using Cloudflare.FerryQueue.Models;
using Cloudflare.FerryQueue.Retry;
using Cloudflare.FerryQueue.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Cloudflare.FerryQueue.Client;

/// <summary>
/// Default implementation of <see cref="ICloudflareQueuesClient"/> using
/// <see cref="HttpClient"/> and the Cloudflare REST API.
/// </summary>
public sealed class CloudflareQueuesClient : ICloudflareQueuesClient
{
    // ── Static JSON options shared across all instances ────────────────────────
    internal static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        NumberHandling = JsonNumberHandling.AllowReadingFromString,
        WriteIndented = false,
    };

    private readonly HttpClient _http;
    private readonly CloudflareQueuesOptions _opts;
    private readonly ILogger<CloudflareQueuesClient> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="CloudflareQueuesClient"/> class.
    /// </summary>
    /// <param name="http">The <see cref="HttpClient"/> used to call the Cloudflare API.</param>
    /// <param name="options">The configured <see cref="CloudflareQueuesOptions"/>.</param>
    /// <param name="logger">The logger instance.</param>
    public CloudflareQueuesClient(
        HttpClient http,
        IOptions<CloudflareQueuesOptions> options,
        ILogger<CloudflareQueuesClient> logger)
    {
        _http   = http;
        _opts   = options.Value;
        _logger = logger;

        _opts.EnsureValid();
    }

    // ── Publish ───────────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public Task<PublishResult> PublishAsync(
        string queueId,
        OutboundMessage message,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queueId);
        ArgumentNullException.ThrowIfNull(message);

        var dto = new PublishSingleRequest { Body = message.Body, DelaySeconds = message.DelaySeconds };

        return RetryPolicy.ExecuteAsync(
            ct => PostAndMapAsync(
                url: QueueUrl(queueId, "messages"),
                payload: dto,
                map: r => new PublishResult { Success = r.Success, Errors = AsReadOnly(r.Errors), ApiMessages = AsReadOnly(r.Messages) },
                ct),
            _opts, _logger, nameof(PublishAsync), cancellationToken);
    }

    /// <inheritdoc/>
    public Task<PublishResult> PublishAsync<T>(
        string queueId,
        OutboundMessage<T> message,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queueId);
        ArgumentNullException.ThrowIfNull(message);

        // Serialize body to JsonElement so Cloudflare receives proper JSON structure
        var bodyElement = JsonSerializer.SerializeToElement(message.Body, JsonOpts);
        return PublishAsync(
            queueId,
            new OutboundMessage { Body = bodyElement, DelaySeconds = message.DelaySeconds },
            cancellationToken);
    }

    /// <inheritdoc/>
    public Task<BatchPublishResult> PublishBatchAsync(
        string queueId,
        IEnumerable<OutboundMessage> messages,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queueId);
        ArgumentNullException.ThrowIfNull(messages);

        var list = messages as IList<OutboundMessage> ?? messages.ToList();

        if (list.Count > 100)
            throw new ArgumentException(
                $"Cloudflare Queues supports at most 100 messages per batch, got {list.Count}.",
                nameof(messages));

        var dto = new PublishBatchRequest
        {
            Messages = list.Select(m => new PublishSingleRequest { Body = m.Body, DelaySeconds = m.DelaySeconds })
        };

        return RetryPolicy.ExecuteAsync(
            ct => PostAndMapAsync(
                url: QueueUrl(queueId, "messages/batch"),
                payload: dto,
                map: r => new BatchPublishResult { Success = r.Success, Errors = AsReadOnly(r.Errors), ApiMessages = AsReadOnly(r.Messages) },
                ct),
            _opts, _logger, nameof(PublishBatchAsync), cancellationToken);
    }

    /// <inheritdoc/>
    public Task<BatchPublishResult> PublishBatchAsync<T>(
        string queueId,
        IEnumerable<OutboundMessage<T>> messages,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queueId);
        ArgumentNullException.ThrowIfNull(messages);

        var raw = messages.Select(m => new OutboundMessage
        {
            Body         = JsonSerializer.SerializeToElement(m.Body, JsonOpts),
            DelaySeconds = m.DelaySeconds
        });

        return PublishBatchAsync(queueId, raw, cancellationToken);
    }

    // ── Consume ───────────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public async Task<PullResult> PullAsync(
        string queueId,
        string consumerId,
        PullOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queueId);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerId);

        options ??= new PullOptions();
        var dto = new PullRequest { BatchSize = options.BatchSize, VisibilityTimeoutMs = options.VisibilityTimeoutMs };

        try
        {
            return await RetryPolicy.ExecuteAsync(
                ct => PostAndMapAsync<PullRequest, PullResultPayload, PullResult>(
                    url: QueueUrl(queueId, "messages/pull"),
                    payload: dto,
                    map: r => new PullResult
                    {
                        Success  = r.Success,
                        Messages = (IReadOnlyList<InboundMessage>?)r.Result?.Messages ?? [],
                        Errors   = AsReadOnly(r.Errors),
                        BacklogCount = r.Result?.MessageBacklogCount,
                    },
                    ct),
                _opts, _logger, nameof(PullAsync), cancellationToken).ConfigureAwait(false);
        }
        catch (CloudflareQueuesException ex) when (ex.IsDeliveryPaused)
        {
            // Delivery is temporarily paused (e.g. someone paused the queue in the
            // Cloudflare dashboard/API). This is an expected, transient state — not a
            // failure — so we surface it as a normal empty result instead of throwing.
            // Callers (including CloudflareQueuesProcessor) should simply keep polling;
            // delivery resumes automatically once the queue is unpaused.
            _logger.LogInformation(
                "[CloudflareQueues] Queue delivery is paused for queue={QueueId} — will keep listening for resume",
                queueId);

            return new PullResult
            {
                Success  = true,
                Messages = [],
                Errors   = ex.ApiErrors,
                IsPaused = true,
            };
        }
    }

    /// <inheritdoc/>
    public async Task<PullResult<T>> PullAsync<T>(
        string queueId,
        string consumerId,
        PullOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var raw = await PullAsync(queueId, consumerId, options, cancellationToken).ConfigureAwait(false);

        var typed = raw.Messages
            .Select(m => new InboundMessage<T>
            {
                LeaseId     = m.LeaseId,
                Id          = m.Id,
                TimestampMs = m.TimestampMs,
                Attempts    = m.Attempts,
                Body = m.Body is JsonElement el
                    ? DeserializeBody<T>(el)
                    : default,
            })
            .ToList();

        return new PullResult<T>
        {
            Success  = raw.Success,
            Messages = typed,
            Errors   = raw.Errors,
            BacklogCount = raw.BacklogCount,
            IsPaused = raw.IsPaused,
        };
    }

    // ── Ack / Retry ───────────────────────────────────────────────────────────

    /// <summary>
    /// Deserializes a pulled message body into <typeparamref name="T"/>.
    /// Cloudflare returns JSON message bodies as a <b>JSON-encoded string</b>
    /// (e.g. <c>"body":"{\"orderId\":1}"</c>) rather than a nested JSON object,
    /// so the string must be re-parsed before deserializing into <typeparamref name="T"/>.
    /// </summary>
    private static T? DeserializeBody<T>(JsonElement el)
    {
        if (el.ValueKind == JsonValueKind.String)
        {
            var raw = el.GetString();
            return string.IsNullOrEmpty(raw)
                ? default
                : JsonSerializer.Deserialize<T>(raw, JsonOpts);
        }

        return el.Deserialize<T>(JsonOpts);
    }

    /// <inheritdoc/>
    public Task AcknowledgeAsync(
        string queueId,
        string consumerId,
        AckBatch batch,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queueId);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerId);
        ArgumentNullException.ThrowIfNull(batch);

        var dto = new AckRequest
        {
            Acks    = batch.Acks.Select(id => new AckEntry { LeaseId = id }),
            Retries = batch.Retries.Select(id => new RetryEntry { LeaseId = id }),
        };

        return RetryPolicy.ExecuteAsync(
            ct => PostAndMapAsync(
                url: QueueUrl(queueId, "messages/ack"),
                payload: dto,
                map: r => r.Success,   // discard result, caller gets Task
                ct),
            _opts, _logger, nameof(AcknowledgeAsync), cancellationToken);
    }

    /// <inheritdoc/>
    public Task AcknowledgeAsync(
        string queueId,
        string consumerId,
        string leaseId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(leaseId);
        return AcknowledgeAsync(queueId, consumerId, new AckBatch { Acks = [leaseId] }, cancellationToken);
    }

    /// <inheritdoc/>
    public Task RetryAsync(
        string queueId,
        string consumerId,
        string leaseId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(leaseId);
        return AcknowledgeAsync(queueId, consumerId, new AckBatch { Retries = [leaseId] }, cancellationToken);
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private string QueueUrl(string queueId, string path) =>
        $"accounts/{_opts.AccountId}/queues/{queueId}/{path}";

    private async Task<TResult> PostAndMapAsync<TRequest, TPayload, TResult>(
        string url,
        TRequest payload,
        Func<CloudflareEnvelope<TPayload>, TResult> map,
        CancellationToken ct)
    {
        _logger.LogDebug("[CloudflareQueues] POST {Url}", url);

        using var response = await _http
            .PostAsJsonAsync(url, payload, JsonOpts, ct)
            .ConfigureAwait(false);

        var envelope = await ReadEnvelopeAsync<TPayload>(response, ct).ConfigureAwait(false);

        if (!envelope.Success)
        {
            var errors = AsReadOnly(envelope.Errors);
            var msg = errors.Count > 0
                ? $"Cloudflare Queues API error on POST {url}: {errors[0]}"
                : $"Cloudflare Queues API returned success=false on POST {url}";

            throw new CloudflareQueuesException(msg, (int)response.StatusCode, errors);
        }

        return map(envelope);
    }

    // Overload for operations where we don't care about the result body (ack returns void semantics)
    private Task<TResult> PostAndMapAsync<TRequest, TResult>(
        string url,
        TRequest payload,
        Func<CloudflareEnvelope<object>, TResult> map,
        CancellationToken ct)
        => PostAndMapAsync<TRequest, object, TResult>(url, payload, map, ct);

    private async Task<CloudflareEnvelope<TPayload>> ReadEnvelopeAsync<TPayload>(
        HttpResponseMessage response,
        CancellationToken ct)
    {
        var statusCode = (int)response.StatusCode;

        if (RetryPolicy.IsTransient(statusCode))
        {
            var body = await response.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
            throw new CloudflareTransientException(
                $"Transient HTTP {statusCode} from Cloudflare API: {body}", statusCode);
        }

        CloudflareEnvelope<TPayload>? envelope;

        try
        {
            envelope = await response.Content
                .ReadFromJsonAsync<CloudflareEnvelope<TPayload>>(JsonOpts, ct)
                .ConfigureAwait(false);
        }
        catch (JsonException ex)
        {
            var raw = await response.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
            throw new CloudflareQueuesException(
                $"Failed to deserialize Cloudflare API response (HTTP {statusCode}): {raw}", ex);
        }

        return envelope
            ?? throw new CloudflareQueuesException($"Empty response body from Cloudflare API (HTTP {statusCode}).");
    }

    private static IReadOnlyList<T> AsReadOnly<T>(List<T>? list)
        => list is { Count: > 0 } ? list.AsReadOnly() : [];
}
