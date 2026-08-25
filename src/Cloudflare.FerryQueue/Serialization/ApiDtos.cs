using System.Text.Json.Serialization;
using Cloudflare.FerryQueue.Models;

namespace Cloudflare.FerryQueue.Serialization;

// ── Requests ──────────────────────────────────────────────────────────────────

internal sealed class PublishSingleRequest
{
    [JsonPropertyName("body")]
    public required object Body { get; init; }

    [JsonPropertyName("delay_seconds")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? DelaySeconds { get; init; }
}

internal sealed class PublishBatchRequest
{
    [JsonPropertyName("messages")]
    public required IEnumerable<PublishSingleRequest> Messages { get; init; }
}

internal sealed class PullRequest
{
    [JsonPropertyName("batch_size")]
    public int BatchSize { get; init; }

    [JsonPropertyName("visibility_timeout_ms")]
    public int VisibilityTimeoutMs { get; init; }
}

internal sealed class AckRequest
{
    [JsonPropertyName("acks")]
    public required IEnumerable<AckEntry> Acks { get; init; }

    [JsonPropertyName("retries")]
    public required IEnumerable<RetryEntry> Retries { get; init; }
}

internal sealed class AckEntry
{
    [JsonPropertyName("lease_id")]
    public required string LeaseId { get; init; }
}

internal sealed class RetryEntry
{
    [JsonPropertyName("lease_id")]
    public required string LeaseId { get; init; }
}

// ── Responses ─────────────────────────────────────────────────────────────────

internal sealed class CloudflareEnvelope<T>
{
    [JsonPropertyName("success")]
    public bool Success { get; init; }

    [JsonPropertyName("errors")]
    public List<CloudflareApiError>? Errors { get; init; }

    [JsonPropertyName("messages")]
    public List<CloudflareApiMessage>? Messages { get; init; }

    [JsonPropertyName("result")]
    public T? Result { get; init; }
}

internal sealed class PullResultPayload
{
    [JsonPropertyName("messages")]
    public List<InboundMessage>? Messages { get; init; }

    /// <summary>
    /// Best-effort count of unacknowledged messages still remaining in the queue
    /// (including the ones just returned in this batch). Used to decide whether
    /// to poll again immediately or back off when the queue appears empty.
    /// </summary>
    [JsonPropertyName("message_backlog_count")]
    public int? MessageBacklogCount { get; init; }
}
