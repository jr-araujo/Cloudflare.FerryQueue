using Cloudflare.FerryQueue.Abstractions;
using Cloudflare.FerryQueue.Client;
using Cloudflare.FerryQueue.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Cloudflare.FerryQueue.DependencyInjection;

/// <summary>
/// Extension methods for registering Cloudflare Queues services with Microsoft DI.
/// </summary>
public static class ServiceCollectionExtensions
{
    // ── AddCloudflareQueues ───────────────────────────────────────────────────

    /// <summary>
    /// Registers <see cref="ICloudflareQueuesClient"/> reading configuration from
    /// the <c>"CloudflareQueues"</c> section of <paramref name="configuration"/>.
    /// </summary>
    /// <example>
    /// <code>
    /// // appsettings.json:
    /// // {
    /// //   "CloudflareQueues": {
    /// //     "AccountId": "abc123",
    /// //     "ApiToken": "your-api-token"
    /// //   }
    /// // }
    /// builder.Services.AddCloudflareQueues(builder.Configuration);
    /// </code>
    /// </example>
    public static IServiceCollection AddCloudflareQueues(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        services.Configure<CloudflareQueuesOptions>(
            configuration.GetSection(CloudflareQueuesOptions.SectionName));

        return services.RegisterHttpClient();
    }

    /// <summary>
    /// Registers <see cref="ICloudflareQueuesClient"/> with options configured inline.
    /// </summary>
    /// <example>
    /// <code>
    /// builder.Services.AddCloudflareQueues(opts =>
    /// {
    ///     opts.AccountId = "abc123";
    ///     opts.ApiToken  = "your-api-token";
    /// });
    /// </code>
    /// </example>
    public static IServiceCollection AddCloudflareQueues(
        this IServiceCollection services,
        Action<CloudflareQueuesOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        services.Configure(configure);

        return services.RegisterHttpClient();
    }

    private static IServiceCollection RegisterHttpClient(this IServiceCollection services)
    {
        services
            .AddHttpClient<ICloudflareQueuesClient, CloudflareQueuesClient>((sp, http) =>
            {
                var opts = sp.GetRequiredService<IOptions<CloudflareQueuesOptions>>().Value;
                opts.EnsureValid();

                http.BaseAddress = new Uri(opts.BaseUrl.TrimEnd('/') + "/");
                http.Timeout     = opts.Timeout;
                http.DefaultRequestHeaders.Add("Authorization", $"Bearer {opts.ApiToken}");
                http.DefaultRequestHeaders.Add("User-Agent", "Cloudflare.FerryQueue.NET/1.0");
            });

        return services;
    }

    // ── AddCloudflareQueuesProcessor ──────────────────────────────────────────

    /// <summary>
    /// Registers a <see cref="CloudflareQueuesProcessor{TMessage}"/> background service
    /// that continuously polls the queue and dispatches messages to <typeparamref name="TConsumer"/>.
    /// </summary>
    /// <typeparam name="TMessage">The message body type.</typeparam>
    /// <typeparam name="TConsumer">
    /// The consumer implementation. Resolved as <b>scoped</b> per message dispatch,
    /// so it can depend on scoped services (e.g. <c>DbContext</c>, <c>IRepository</c>).
    /// </typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="configureProcessor">Processor options (queue ID, consumer ID, batch size…).</param>
    /// <example>
    /// <code>
    /// builder.Services
    ///     .AddCloudflareQueues(builder.Configuration)
    ///     .AddCloudflareQueuesProcessor&lt;OrderEvent, OrderEventConsumer&gt;(opts =>
    ///     {
    ///         opts.QueueId    = "orders-queue-id";
    ///         opts.ConsumerId = "orders-consumer-id";
    ///         opts.BatchSize  = 20;
    ///         opts.MaxConcurrentMessages = 4;
    ///     });
    /// </code>
    /// </example>
    public static IServiceCollection AddCloudflareQueuesProcessor<TMessage, TConsumer>(
        this IServiceCollection services,
        Action<ProcessorOptions> configureProcessor)
        where TConsumer : class, ICloudflareQueueConsumer<TMessage>
    {
        ArgumentNullException.ThrowIfNull(configureProcessor);

        var opts = new ProcessorOptions();
        configureProcessor(opts);
        opts.EnsureValid();

        // Register the ProcessorOptions as a singleton keyed by the concrete processor type
        // so multiple processors for different queues don't conflict.
        services.AddKeyedSingleton($"CloudflareQueuesProcessorOptions_{typeof(TMessage).FullName}", opts);

        // Consumer is scoped — resolved fresh per message dispatch
        services.TryAddScoped<ICloudflareQueueConsumer<TMessage>, TConsumer>();

        // Background processor as a singleton hosted service
        services.AddHostedService(sp =>
            new CloudflareQueuesProcessor<TMessage>(
                sp.GetRequiredService<ICloudflareQueuesClient>(),
                sp.GetRequiredService<IServiceScopeFactory>(),
                opts,
                sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<CloudflareQueuesProcessor<TMessage>>>()));

        return services;
    }
}
