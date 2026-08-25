using Cloudflare.FerryQueue.Models;

namespace Cloudflare.FerryQueue.Exceptions;

/// <summary>
/// Thrown when the Cloudflare Queues API returns a non-success response
/// or when a network/timeout error occurs after all retries are exhausted.
/// </summary>
public sealed class CloudflareQueuesException : Exception
{
    /// <summary>HTTP status code, if the error originated from an HTTP response.</summary>
    public int? HttpStatusCode { get; }

    /// <summary>Structured errors returned by the Cloudflare API, if any.</summary>
    public IReadOnlyList<CloudflareApiError> ApiErrors { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="CloudflareQueuesException"/> class
    /// with a specified error message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public CloudflareQueuesException(string message)
        : base(message)
    {
        ApiErrors = [];
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="CloudflareQueuesException"/> class
    /// with a specified error message and a reference to the inner exception that caused it.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="inner">The exception that is the cause of the current exception.</param>
    public CloudflareQueuesException(string message, Exception inner)
        : base(message, inner)
    {
        ApiErrors = [];
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="CloudflareQueuesException"/> class
    /// with the HTTP status code and structured API errors returned by Cloudflare.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="httpStatusCode">The HTTP status code returned by the API.</param>
    /// <param name="apiErrors">The structured errors returned by the Cloudflare API.</param>
    public CloudflareQueuesException(
        string message,
        int httpStatusCode,
        IReadOnlyList<CloudflareApiError> apiErrors)
        : base(message)
    {
        HttpStatusCode = httpStatusCode;
        ApiErrors = apiErrors;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="CloudflareQueuesException"/> class
    /// with the HTTP status code, structured API errors, and a reference to the inner exception.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="httpStatusCode">The HTTP status code returned by the API.</param>
    /// <param name="apiErrors">The structured errors returned by the Cloudflare API.</param>
    /// <param name="inner">The exception that is the cause of the current exception.</param>
    public CloudflareQueuesException(
        string message,
        int httpStatusCode,
        IReadOnlyList<CloudflareApiError> apiErrors,
        Exception inner)
        : base(message, inner)
    {
        HttpStatusCode = httpStatusCode;
        ApiErrors = apiErrors;
    }

    /// <summary>
    /// <c>true</c> when this error indicates that message delivery has been
    /// <b>paused</b> on the queue (e.g. via the Cloudflare dashboard or API),
    /// rather than a genuine failure. Callers can use this to distinguish a
    /// transient, expected "paused" state from real errors.
    /// </summary>
    public bool IsDeliveryPaused =>
        ApiErrors.Any(e => e.Message.Contains("delivery is paused", StringComparison.OrdinalIgnoreCase));

    /// <summary>Returns a string representation of the exception, including any Cloudflare API errors.</summary>
    /// <returns>A string that represents the current exception.</returns>
    public override string ToString()
    {
        var errors = ApiErrors.Count > 0
            ? "\nAPI errors:\n  " + string.Join("\n  ", ApiErrors)
            : string.Empty;

        return $"{base.ToString()}{errors}";
    }
}
