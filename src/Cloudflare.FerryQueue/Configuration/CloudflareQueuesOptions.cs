using System.ComponentModel.DataAnnotations;

namespace Cloudflare.FerryQueue.Configuration;

/// <summary>
/// Configuration options for the Cloudflare Queues client.
/// Bind from <c>appsettings.json</c> under the <c>"CloudflareQueues"</c> section.
/// </summary>
public sealed class CloudflareQueuesOptions
{
    /// <summary>The configuration section name used for binding.</summary>
    public const string SectionName = "CloudflareQueues";

    /// <summary>Your Cloudflare Account ID.</summary>
    [Required]
    public string AccountId { get; set; } = string.Empty;

    /// <summary>
    /// A Cloudflare API Token with <c>Workers Queues: Edit</c> permission.
    /// </summary>
    [Required]
    public string ApiToken { get; set; } = string.Empty;

    /// <summary>
    /// Cloudflare REST API base URL.
    /// Defaults to <c>https://api.cloudflare.com/client/v4</c>.
    /// </summary>
    public string BaseUrl { get; set; } = "https://api.cloudflare.com/client/v4";

    /// <summary>HTTP request timeout. Defaults to <c>30s</c>.</summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Maximum number of retry attempts on transient HTTP errors (5xx, network).
    /// Set to <c>0</c> to disable retries. Defaults to <c>3</c>.
    /// </summary>
    [Range(0, 10)]
    public int MaxRetries { get; set; } = 3;

    /// <summary>
    /// Base delay between retry attempts.
    /// Actual delay uses exponential back-off: <c>RetryDelay * 2^attempt</c>.
    /// Defaults to <c>250ms</c>.
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Maximum delay cap for exponential back-off. Defaults to <c>10s</c>.
    /// </summary>
    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromSeconds(10);

    internal void EnsureValid()
    {
        if (string.IsNullOrWhiteSpace(AccountId))
            throw new InvalidOperationException(
                $"[CloudflareQueues] '{nameof(AccountId)}' must not be empty.");

        if (string.IsNullOrWhiteSpace(ApiToken))
            throw new InvalidOperationException(
                $"[CloudflareQueues] '{nameof(ApiToken)}' must not be empty.");

        if (string.IsNullOrWhiteSpace(BaseUrl))
            throw new InvalidOperationException(
                $"[CloudflareQueues] '{nameof(BaseUrl)}' must not be empty.");
    }
}
