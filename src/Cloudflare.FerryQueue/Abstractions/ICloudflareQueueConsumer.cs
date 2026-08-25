using Cloudflare.FerryQueue.Models;

namespace Cloudflare.FerryQueue.Abstractions;

/// <summary>
/// Implement this interface to handle messages from a Cloudflare Queue.
/// Register it with <c>AddCloudflareQueuesProcessor&lt;TMessage, TConsumer&gt;()</c>
/// to have the background processor dispatch messages to it automatically.
/// </summary>
/// <typeparam name="TMessage">The deserialized body type.</typeparam>
public interface ICloudflareQueueConsumer<TMessage>
{
    /// <summary>
    /// Processes a single message pulled from the queue.
    /// </summary>
    /// <param name="message">The message to process.</param>
    /// <param name="cancellationToken">Cancellation token — honour it during long operations.</param>
    /// <returns>
    /// <see langword="true"/> to <b>acknowledge</b> the message (mark as done).<br/>
    /// <see langword="false"/> to <b>retry</b> the message (make it visible again).
    /// </returns>
    Task<bool> ConsumeAsync(InboundMessage<TMessage> message, CancellationToken cancellationToken);
}
