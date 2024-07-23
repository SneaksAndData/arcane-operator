using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Akka;
using Akka.Streams.Dsl;
using Akka.Util;
using Arcane.Operator.Models.Api;
using Arcane.Operator.Services.Base.EventFilters;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;

namespace Arcane.Operator.Services.EventFilters;

/// <summary>
/// Removed events from the stream if the events are related to the same version of the resource.
/// </summary>
/// <typeparam name="TResourceType">Resource type</typeparam>
[ExcludeFromCodeCoverage(Justification = "Requires integration testing")]
public class ResourceVersionDuplicateFilter<TResourceType> : IEventFilter<TResourceType> where TResourceType : IKubernetesObject<V1ObjectMeta>
{
    private readonly IMemoryCache memoryCache;
    private readonly ILogger<ResourceVersionDuplicateFilter<TResourceType>> logger;

    public ResourceVersionDuplicateFilter(IMemoryCache memoryCache, ILogger<ResourceVersionDuplicateFilter<TResourceType>> logger)
    {
        this.memoryCache = memoryCache;
        this.logger = logger;
    }

    /// <summary>
    /// Deduplicates events related to the same version of the resource.
    /// </summary>
    /// <returns>Event if the event related to new version of the resource or None if event is duplicate.</returns>
    public Flow<ResourceEvent<TResourceType>, Option<ResourceEvent<TResourceType>>, NotUsed> Filter()
    {
        return Flow.FromFunction<ResourceEvent<TResourceType>, Option<ResourceEvent<TResourceType>>>(ev =>
        {
            if (!memoryCache.TryGetValue<ResourceEvent<TResourceType>>(ToCacheKey(ev), out var cached))
            {
                memoryCache.Set(ToCacheKey(ev), ev);
                return ev;
            }

            if (cached != null && cached.kubernetesObject.ResourceVersion() != ev.kubernetesObject.ResourceVersion())
            {
                memoryCache.Set(ToCacheKey(ev), ev);
                return ev;
            }

            logger.LogInformation("Skip duplicate {type} event for  {objectKey}", ev.EventType, ToCacheKey(ev));
            return Option<ResourceEvent<TResourceType>>.None;
        });
    }


    private static string ToCacheKey(ResourceEvent<TResourceType> resourceEvent) => string.Join("/", new List<string>
    {
        resourceEvent?.kubernetesObject?.Kind,
        resourceEvent?.kubernetesObject?.Namespace() ?? "",
        resourceEvent?.kubernetesObject?.Name() ?? ""
    });
}
