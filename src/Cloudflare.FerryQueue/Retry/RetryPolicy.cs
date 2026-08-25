using Cloudflare.FerryQueue.Configuration;
using Microsoft.Extensions.Logging;

namespace Cloudflare.FerryQueue.Retry;

/// <summary>
/// Executes an async operation with exponential back-off retry on transient failures.
/// </summary>
internal static class RetryPolicy
{
    /// <summary>HTTP status codes considered transient and eligible for retry.</summary>
    private static readonly IReadOnlySet<int> TransientStatusCodes = new HashSet<int>
    {
        429, // Too Many Requests
        500, // Internal Server Error
        502, // Bad Gateway
        503, // Service Unavailable
        504, // Gateway Timeout
    };

    public static async Task<T> ExecuteAsync<T>(
        Func<CancellationToken, Task<T>> operation,
        CloudflareQueuesOptions options,
        ILogger logger,
        string operationName,
        CancellationToken cancellationToken)
    {
        int attempt = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                return await operation(cancellationToken).ConfigureAwait(false);
            }
            catch (CloudflareTransientException ex) when (attempt < options.MaxRetries)
            {
                attempt++;
                var delay = CalculateDelay(attempt, options);

                logger.LogWarning(
                    ex,
                    "[CloudflareQueues] {Operation} transient failure (attempt {Attempt}/{Max}), " +
                    "status={Status}, retrying in {Delay}ms…",
                    operationName, attempt, options.MaxRetries,
                    ex.StatusCode, (int)delay.TotalMilliseconds);

                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
            catch (HttpRequestException ex) when (attempt < options.MaxRetries)
            {
                attempt++;
                var delay = CalculateDelay(attempt, options);

                logger.LogWarning(
                    ex,
                    "[CloudflareQueues] {Operation} network error (attempt {Attempt}/{Max}), " +
                    "retrying in {Delay}ms…",
                    operationName, attempt, options.MaxRetries, (int)delay.TotalMilliseconds);

                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    public static bool IsTransient(int statusCode) => TransientStatusCodes.Contains(statusCode);

    private static TimeSpan CalculateDelay(int attempt, CloudflareQueuesOptions options)
    {
        // Exponential back-off: base * 2^(attempt-1), capped at MaxRetryDelay
        var factor = Math.Pow(2, attempt - 1);
        var delay = TimeSpan.FromTicks((long)(options.RetryDelay.Ticks * factor));
        return delay > options.MaxRetryDelay ? options.MaxRetryDelay : delay;
    }
}

/// <summary>Internal marker exception for transient HTTP errors eligible for retry.</summary>
internal sealed class CloudflareTransientException(string message, int statusCode)
    : Exception(message)
{
    public int StatusCode { get; } = statusCode;
}
