using System.Text.Json;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Operator.Models.StreamClass;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Models.StreamDefinitions;
using Arcane.Operator.Models.StreamDefinitions.Base;

namespace Arcane.Operator.Extensions;

/// <summary>
/// Extension methods for the JsonElement class
/// </summary>
public static class JsonElementExtensions
{
    /// <summary>
    /// Deserialize the JsonElement to IStreamDefinition object and wrap it in an Option{IStreamDefinition} object
    /// </summary>
    /// <param name="jsonElement">Element to deserialize</param>
    /// <returns></returns>
    public static Option<IStreamDefinition> AsOptionalStreamDefinition(this JsonElement jsonElement) =>
        jsonElement.Deserialize<StreamDefinition>().AsOption<IStreamDefinition>();
    
    /// <summary>
    /// Deserialize the JsonElement to IStreamDefinition object and wrap it in an Option{IStreamDefinition} object
    /// </summary>
    /// <param name="jsonElement">Element to deserialize</param>
    /// <returns></returns>
    public static Option<IStreamClass> AsOptionalStreamClass(this JsonElement jsonElement) =>
        jsonElement.Deserialize<V1Beta1StreamClass>().AsOption<IStreamClass>();
}
