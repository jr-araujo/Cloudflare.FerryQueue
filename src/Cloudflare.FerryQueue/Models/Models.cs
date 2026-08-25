using System.Text.Json.Serialization;

namespace Cloudflare.FerryQueue.Models;

// ── Publish ───────────────────────────────────────────────────────────────────

/// <summary>A raw message to publish to a Cloudflare Queue.</summary>
public sealed class OutboundMessage
{
    /// <summary>
    /// The message body. Primitives, strings, and JSON-serializable objects are all accepted.
    /// </summary>
    public required object Body { get; init; }

    /// <summary>
    /// Optional number of seconds to delay delivery.
    /// Must be between 0 and 43200 (12 hours).
    /// </summary>
    public int? DelaySeconds { get; init; }
}

/// <summary>A strongly-typed message to publish to a Cloudflare Queue.</summary>
/// <typeparam name="T">Type of the message body — must be JSON-serializable.</typeparam>
public sealed class OutboundMessage<T>
{
    /// <summary>The message body.</summary>
    public required T Body { get; init; }

    /// <summary>
    /// Optional delivery delay in seconds (0–43200).
    /// </summary>
    public int? DelaySeconds { get; init; }
}

// ── Consume ───────────────────────────────────────────────────────────────────

/// <summary>A raw message pulled from a Cloudflare Queue Pull Consumer.</summary>
public sealed class InboundMessage
{
    /// <summary>Unique lease identifier — required for ack/retry.</summary>
    [JsonPropertyName("lease_id")]
    public string LeaseId { get; init; } = string.Empty;

    /// <summary>Cloudflare's message ID.</summary>
    [JsonPropertyName("id")]
    public string Id { get; init; } = string.Empty;

    /// <summary>Raw message body (may be a <see cref="System.Text.Json.JsonElement"/>).</summary>
    [JsonPropertyName("body")]
    public object? Body { get; init; }

    /// <summary>Unix epoch milliseconds when the message was enqueued.</summary>
    [JsonPropertyName("timestamp_ms")]
    public long TimestampMs { get; init; }

    /// <summary>Number of delivery attempts so far.</summary>
    [JsonPropertyName("attempts")]
    public int Attempts { get; init; }

    /// <summary>UTC timestamp derived from <see cref="TimestampMs"/>.</summary>
    public DateTimeOffset EnqueuedAt => DateTimeOffset.FromUnixTimeMilliseconds(TimestampMs);
}

/// <summary>A strongly-typed message pulled from a Cloudflare Queue Pull Consumer.</summary>
/// <typeparam name="T">Type to deserialize the body into.</typeparam>
public sealed class InboundMessage<T>
{
    /// <summary>Unique lease identifier — required for ack/retry.</summary>
    public string LeaseId { get; init; } = string.Empty;

    /// <summary>Cloudflare's message ID.</summary>
    public string Id { get; init; } = string.Empty;

    /// <summary>Deserialized message body.</summary>
    public T? Body { get; init; }

    /// <summary>Unix epoch milliseconds when the message was enqueued.</summary>
    public long TimestampMs { get; init; }

    /// <summary>Number of delivery attempts so far.</summary>
    public int Attempts { get; init; }

    /// <summary>UTC timestamp derived from <see cref="TimestampMs"/>.</summary>
    public DateTimeOffset EnqueuedAt => DateTimeOffset.FromUnixTimeMilliseconds(TimestampMs);
}

// ── Pull options ──────────────────────────────────────────────────────────────

/// <summary>Options for a Pull Consumer <c>messages/pull</c> call.</summary>
public sealed class PullOptions
{
    /// <summary>Number of messages to pull. Range: 1–100. Default: <c>10</c>.</summary>
    public int BatchSize { get; init; } = 10;

    /// <summary>
    /// How long pulled messages stay invisible before becoming available again.
    /// Range: 1–43200000 ms. Default: <c>30 000 ms</c> (30 s).
    /// </summary>
    public int VisibilityTimeoutMs { get; init; } = 30_000;
}

// ── Ack options ───────────────────────────────────────────────────────────────

/// <summary>Lease IDs to acknowledge and/or retry in a single call.</summary>
public sealed class AckBatch
{
    /// <summary>Lease IDs to mark as successfully processed.</summary>
    public IReadOnlyList<string> Acks { get; init; } = [];

    /// <summary>Lease IDs to make visible again for re-processing.</summary>
    public IReadOnlyList<string> Retries { get; init; } = [];
}

// ── Results ───────────────────────────────────────────────────────────────────

/// <summary>Result of a publish operation.</summary>
public sealed class PublishResult
{
    /// <summary>Whether the API returned <c>success: true</c>.</summary>
    public bool Success { get; init; }

    /// <summary>Errors reported by the Cloudflare API.</summary>
    public IReadOnlyList<CloudflareApiError> Errors { get; init; } = [];

    /// <summary>Informational messages returned by the API.</summary>
    public IReadOnlyList<CloudflareApiMessage> ApiMessages { get; init; } = [];
}

/// <summary>Result of a batch publish operation.</summary>
public sealed class BatchPublishResult
{
    /// <inheritdoc cref="PublishResult.Success"/>
    public bool Success { get; init; }

    /// <inheritdoc cref="PublishResult.Errors"/>
    public IReadOnlyList<CloudflareApiError> Errors { get; init; } = [];

    /// <inheritdoc cref="PublishResult.ApiMessages"/>
    public IReadOnlyList<CloudflareApiMessage> ApiMessages { get; init; } = [];
}

/// <summary>Result of a Pull Consumer <c>messages/pull</c> call.</summary>
public sealed class PullResult
{
    /// <inheritdoc cref="PublishResult.Success"/>
    public bool Success { get; init; }

    /// <summary>Messages pulled from the queue.</summary>
    public IReadOnlyList<InboundMessage> Messages { get; init; } = [];

    /// <inheritdoc cref="PublishResult.Errors"/>
    public IReadOnlyList<CloudflareApiError> Errors { get; init; } = [];

    /// <summary>
    /// Best-effort count of unacknowledged messages remaining in the queue
    /// (including the ones just returned in <see cref="Messages"/>).
    /// <c>null</c> when the API did not report it (e.g. on error responses).
    /// </summary>
    public int? BacklogCount { get; init; }

    /// <summary>
    /// <c>true</c> when the queue's message delivery is currently <b>paused</b>
    /// (e.g. via the Cloudflare dashboard or API). When paused, <see cref="Messages"/>
    /// is always empty and <see cref="Success"/> is still <c>true</c> — no exception
    /// is thrown for this expected, transient condition. Callers (or
    /// <see cref="Cloudflare.FerryQueue.Client.CloudflareQueuesProcessor{TMessage}"/>)
    /// should keep polling; delivery will resume automatically once unpaused.
    /// </summary>
    public bool IsPaused { get; init; }
}

/// <summary>Typed variant of <see cref="PullResult"/>.</summary>
/// <typeparam name="T">Message body type.</typeparam>
public sealed class PullResult<T>
{
    /// <inheritdoc cref="PublishResult.Success"/>
    public bool Success { get; init; }

    /// <summary>Deserialized messages pulled from the queue.</summary>
    public IReadOnlyList<InboundMessage<T>> Messages { get; init; } = [];

    /// <inheritdoc cref="PublishResult.Errors"/>
    public IReadOnlyList<CloudflareApiError> Errors { get; init; } = [];

    /// <inheritdoc cref="PullResult.BacklogCount"/>
    public int? BacklogCount { get; init; }

    /// <inheritdoc cref="PullResult.IsPaused"/>
    public bool IsPaused { get; init; }
}

// ── API error types ───────────────────────────────────────────────────────────

/// <summary>An error object returned by the Cloudflare API.</summary>
public sealed class CloudflareApiError
{
    /// <summary>The Cloudflare-specific error code.</summary>
    [JsonPropertyName("code")]
    public int Code { get; init; }

    /// <summary>A human-readable description of the error.</summary>
    [JsonPropertyName("message")]
    public string Message { get; init; } = string.Empty;

    /// <summary>Returns a string representation of the error.</summary>
    /// <returns>A string in the form <c>[code] message</c>.</returns>
    public override string ToString() => $"[{Code}] {Message}";
}

/// <summary>An informational message returned by the Cloudflare API.</summary>
public sealed class CloudflareApiMessage
{
    /// <summary>The Cloudflare-specific message code.</summary>
    [JsonPropertyName("code")]
    public int Code { get; init; }

    /// <summary>A human-readable description of the message.</summary>
    [JsonPropertyName("message")]
    public string Message { get; init; } = string.Empty;
}
