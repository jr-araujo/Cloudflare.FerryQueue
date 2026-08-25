using Cloudflare.FerryQueue.Models;

namespace Cloudflare.FerryQueue.Abstractions;

/// <summary>
/// Client for the Cloudflare Queues REST API.
/// Supports publishing, batch-publishing, pull-consume, ack and retry operations.
/// </summary>
public interface ICloudflareQueuesClient
{
    // ── Publish ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Publishes a single raw message to the given queue.
    /// </summary>
    /// <param name="queueId">Cloudflare Queue ID.</param>
    /// <param name="message">Message to publish.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="Exceptions.CloudflareQueuesException">
    /// Thrown when the API returns a non-success response or retries are exhausted.
    /// </exception>
    Task<PublishResult> PublishAsync(
        string queueId,
        OutboundMessage message,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes a single strongly-typed message.
    /// The body is serialized to JSON automatically.
    /// </summary>
    /// <typeparam name="T">JSON-serializable body type.</typeparam>
    Task<PublishResult> PublishAsync<T>(
        string queueId,
        OutboundMessage<T> message,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes a batch of raw messages in a single API call (max 100).
    /// </summary>
    Task<BatchPublishResult> PublishBatchAsync(
        string queueId,
        IEnumerable<OutboundMessage> messages,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes a batch of strongly-typed messages in a single API call (max 100).
    /// </summary>
    Task<BatchPublishResult> PublishBatchAsync<T>(
        string queueId,
        IEnumerable<OutboundMessage<T>> messages,
        CancellationToken cancellationToken = default);

    // ── Consume ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Pulls raw messages from a Pull Consumer.
    /// </summary>
    /// <param name="queueId">Queue ID.</param>
    /// <param name="consumerId">Pull Consumer ID.</param>
    /// <param name="options">Pull options (batch size, visibility timeout).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<PullResult> PullAsync(
        string queueId,
        string consumerId,
        PullOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Pulls and deserializes messages from a Pull Consumer.
    /// </summary>
    /// <typeparam name="T">Type to deserialize the body into.</typeparam>
    Task<PullResult<T>> PullAsync<T>(
        string queueId,
        string consumerId,
        PullOptions? options = null,
        CancellationToken cancellationToken = default);

    // ── Ack / Retry ───────────────────────────────────────────────────────────

    /// <summary>
    /// Acknowledges and/or retries multiple messages in a single call.
    /// </summary>
    /// <param name="queueId">Queue ID.</param>
    /// <param name="consumerId">Pull Consumer ID.</param>
    /// <param name="batch">Lease IDs to ack and/or retry.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task AcknowledgeAsync(
        string queueId,
        string consumerId,
        AckBatch batch,
        CancellationToken cancellationToken = default);

    /// <summary>Acknowledges a single message (marks as successfully processed).</summary>
    Task AcknowledgeAsync(
        string queueId,
        string consumerId,
        string leaseId,
        CancellationToken cancellationToken = default);

    /// <summary>Marks a single message for retry (makes it visible again).</summary>
    Task RetryAsync(
        string queueId,
        string consumerId,
        string leaseId,
        CancellationToken cancellationToken = default);
}
