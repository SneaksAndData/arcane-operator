using System.Collections.Generic;
using System.Diagnostics.Eventing.Reader;
using Akka;
using Akka.Streams.Dsl;
using Akka.Util;
using Arcane.Operator.Models.Api;
using Arcane.Operator.Services.Base;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;

namespace Arcane.Operator.Services.Filters;

public class DuplicateFilter<TResourceType>: IEventFilter<TResourceType> where TResourceType : IKubernetesObject<V1ObjectMeta>
{
    private readonly IMemoryCache memoryCache;
    private readonly ILogger<DuplicateFilter<TResourceType>> logger;

    public DuplicateFilter(IMemoryCache memoryCache, ILogger<DuplicateFilter<TResourceType>> logger)
    {
        this.memoryCache = memoryCache;
        this.logger = logger;
    }
    
    public Flow<ResourceEvent<TResourceType>, Option<ResourceEvent<TResourceType>>, NotUsed> Filter()
    {
        return Flow.FromFunction<ResourceEvent<TResourceType>, Option<ResourceEvent<TResourceType>>>(ev =>
        {
            if (!memoryCache.TryGetValue<ResourceEvent<TResourceType>>(this.ToCacheKey(ev), out var cached))
            {
               memoryCache.Set(this.ToCacheKey(ev), ev);
               return ev;
            }
            
            if (cached != null && cached.kubernetesObject.ResourceVersion() != ev.kubernetesObject.ResourceVersion())
            {
                memoryCache.Set(this.ToCacheKey(ev), ev);
                return ev;
            }

            logger.LogInformation("Skip duplicate {type} event for  {objectKey}", ev.EventType, ToCacheKey(ev));
            return Option<ResourceEvent<TResourceType>>.None;
        });
    }


    private string ToCacheKey(ResourceEvent<TResourceType> resourceEvent) => string.Join("/", new List<string>
    {
        resourceEvent?.kubernetesObject?.Kind,
        resourceEvent?.kubernetesObject?.Namespace() ?? "",
        resourceEvent?.kubernetesObject?.Name() ?? ""
    });
}